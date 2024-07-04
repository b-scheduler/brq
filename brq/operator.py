from datetime import datetime
from typing import Any

import redis.asyncio as redis

from brq.log import logger
from brq.models import Job
from brq.rds import RedisOperator


class BrqOperator(RedisOperator):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
    ):
        super().__init__(redis_prefix, redis_seperator)
        self.redis = redis

    async def enque_deferred_job(self, function_name: str, maxlen: int = 1000):
        """
        From zset to stream
        """
        lua_script = """
        local zset_key = KEYS[1]
        local stream_key = KEYS[2]
        local current_timestamp = ARGV[1]
        local maxlen = ARGV[2]

        local elements = redis.call('ZRANGEBYSCORE', zset_key, '-inf', current_timestamp)

        for i, element in ipairs(elements) do
            redis.call('ZREM', zset_key, element)
            redis.call('XADD', stream_key, 'MAXLEN', maxlen, '*', 'payload', element)
        end

        return elements
        """
        defer_key = self.get_deferred_key(function_name)
        stream_name = self.get_stream_name(function_name)

        elements = await self.redis.eval(
            lua_script,
            2,
            defer_key,
            stream_name,
            await self.get_current_timestamp_ms(self.redis),
            maxlen,
        )
        if elements:
            logger.debug(f"Enqueued deferred jobs: {elements}")

    async def _remove_deferred_job(
        self,
        function_name: str,
        job: Job,
    ):
        """
        Remove specific deferred job

        Args:
            function_name (str): function name
            job (Job): job to be removed
        """
        defer_key = self.get_deferred_key(function_name)
        delete_nums = await self.redis.zrem(defer_key, job.to_redis())
        logger.info(f"Removed {delete_nums} deferred jobs")

    async def get_defer_timestamp_ms(
        self,
        defer_until: datetime = None,
        defer_hours: int = 0,
        defer_minutes: int = 0,
        defer_seconds: int = 0,
    ):
        if not any(
            [
                defer_until,
                defer_hours,
                defer_minutes,
                defer_seconds,
            ]
        ):
            return None

        if defer_until:
            logger.debug(f"Using defer_until, ignore defer_hours, defer_minutes, defer_seconds")
            defer_until = defer_until.timestamp()

        else:
            defer_until = (
                await self.get_current_timestamp(self.redis)
                + defer_hours * 60 * 60
                + defer_minutes * 60
                + defer_seconds
            )

        return defer_until * 1000

    async def get_deferred_jobs(self, function_name: str) -> dict[datetime:Job]:
        """
        Get all deferred jobs

        Args:
            function_name (str): function name

        Returns:
            dict[datetime:Job]: deferred jobs,
                key is the datetime when the job expected to be executed
                value is the job
        """
        defer_key = self.get_deferred_key(function_name)
        return {
            datetime.fromtimestamp(float(element[1]) / 1000): Job.from_redis(element[0])
            for element in await self.redis.zrangebyscore(
                defer_key, "-inf", "+inf", withscores=True
            )
        }

    async def get_dead_messages(self, function_name: str) -> dict[datetime:Job]:
        """
        Get all dead messages

        Args:
            function_name (str): function name

        Returns:
            dict[datetime:Job]: dead messages,
                key is the datetime when the job to be moved to dead message
                value is the job
        """
        dead_key = self.get_dead_message_key(function_name)
        return {
            datetime.fromtimestamp(float(element[1]) / 1000): Job.from_redis(element[0])
            for element in await self.redis.zrangebyscore(dead_key, "-inf", "+inf", withscores=True)
        }

    async def count_deferred_jobs(self, function_name: str) -> int:
        defer_key = self.get_deferred_key(function_name)
        if not await self.redis.exists(defer_key):
            return 0
        return await self.redis.zcard(defer_key)

    async def count_stream(self, function_name: str) -> int:
        """
        Count stream length

        If Consumer's `delete_message_after_process` if False, will include already processed messages
        """
        stream_name = self.get_stream_name(function_name)
        if not await self.redis.exists(stream_name):
            return 0
        return await self.redis.xlen(stream_name)

    async def count_unacked_jobs(
        self, function_name: str, group_name: str = "default-workers"
    ) -> int:
        """
        Count unacked jobs in group

        Args:
            function_name (str): function name
            group_name (str, optional): group name. Defaults to "default-workers". Should be the same as Consumer's `group_name`
        """
        stream_name = self.get_stream_name(function_name)
        if not await self.redis.exists(stream_name):
            return 0
        try:
            consumer_group_info = await self.redis.xinfo_consumers(stream_name, group_name)
            return consumer_group_info[0]["pending"]
        except redis.ResponseError as e:
            if "NOGROUP No such consumer group" in e.args[0]:
                return 0
            raise

    async def count_dead_messages(self, function_name: str) -> int:
        dead_key = self.get_dead_message_key(function_name)
        if not await self.redis.exists(dead_key):
            return 0
        return await self.redis.zcard(dead_key)

    async def emit_deferred_job(self, function_name: str, defer_until: int, job: Job):
        defer_key = self.get_deferred_key(function_name)
        await self.redis.zadd(defer_key, {job.to_redis(): defer_until})
        return job

    async def _deferred_job_exists(
        self,
        function_name: str,
        job: Job,
    ) -> bool:
        defer_key = self.get_deferred_key(function_name)
        return await self.redis.zscore(defer_key, job.to_redis())
