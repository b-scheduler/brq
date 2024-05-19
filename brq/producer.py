from datetime import datetime
from typing import Any

import redis.asyncio as redis

from brq.defer_operator import DeferOperator
from brq.log import logger
from brq.models import Job


class Producer(DeferOperator):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
        max_message_size: int = 1000,
    ):
        super().__init__(redis, redis_prefix, redis_seperator)
        self.max_message_size = max_message_size

    async def run_job(
        self,
        function_name: str,
        args: list[Any] = None,
        kwargs: dict[str, Any] = None,
        defer_until: datetime = None,
        defer_hours: int = 0,
        defer_minutes: int = 0,
        defer_seconds: int = 0,
    ) -> Job:

        args = args or []
        kwargs = kwargs or {}

        defer_until = await self.get_defer_timestamp(
            defer_until, defer_hours, defer_minutes, defer_seconds
        )

        if defer_until:
            logger.info(
                f"Deferring job: {function_name} until {defer_until} (defer_hours: {defer_hours}, defer_minutes: {defer_minutes}, defer_seconds: {defer_seconds})"
            )
            job = await self._emit_deferred_job(
                function_name,
                defer_until,
                args,
                kwargs,
            )
        else:
            logger.info(f"Scheduling job: {function_name}")
            job = await self._emit_job(function_name, args, kwargs)

        logger.info(f"Job created: {job}")
        return job

    async def _emit_deferred_job(
        self,
        function_name: str,
        defer_until: int,
        args: list[Any] = None,
        kwargs: dict[str, Any] = None,
    ) -> Job:
        defer_key = self.get_deferred_key(function_name)
        created_at = await self.get_current_timestamp(self.redis)

        job = Job(
            args=args or [],
            kwargs=kwargs or {},
            create_at=created_at,
        )
        await self.redis.zadd(defer_key, {job.to_redis(): defer_until})
        return job

    async def _emit_job(
        self, function_name: str, args: list[Any] = None, kwargs: dict[str, Any] = None
    ) -> Job:
        stream_name = self.get_stream_name(function_name)
        created_at = await self.get_current_timestamp(self.redis)

        job = Job(
            args=args or [],
            kwargs=kwargs or {},
            create_at=created_at,
        )
        await self.redis.xadd(stream_name, job.to_message(), maxlen=self.max_message_size)
        return job

    async def prune(self, function_name: str):
        stream_name = self.get_stream_name(function_name)
        defer_key = self.get_deferred_key(function_name)
        dead_key = self.get_dead_message_key(function_name)
        await self.redis.delete(stream_name, defer_key, dead_key)

    async def remove_deferred_job(
        self,
        function_name: str,
        job: Job,
    ):
        defer_key = self.get_deferred_key(function_name)
        delete_nums = await self.redis.zrem(defer_key, job.to_redis())
        logger.info(f"Removed {delete_nums} deferred jobs")

    async def get_defer_timestamp(
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
        return defer_until

    async def count_deferred_jobs(self, function_name: str):
        defer_key = self.get_deferred_key(function_name)
        return await self.redis.zcard(defer_key)

    async def count_stream(self, function_name: str):
        stream_name = self.get_stream_name(function_name)
        return await self.redis.xlen(stream_name)

    async def count_unacked_jobs(self, function_name: str, group_name: str = "default-workers"):
        stream_name = self.get_stream_name(function_name)
        return await self.redis.xpending(stream_name, group_name)

    async def count_dead_messages(self, function_name: str):
        dead_key = self.get_dead_message_key(function_name)
        return await self.redis.zcard(dead_key)
