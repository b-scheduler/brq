import asyncio
import os
import uuid
from typing import Any, Awaitable, Callable

import redis.asyncio as redis

from brq.defer_operator import DeferOperator
from brq.log import logger
from brq.models import Job
from brq.tools import event_wait

CONSUMER_IDENTIFIER_ENV = "BRQ_CONSUMER_IDENTIFIER"


class RunnableMixin:
    def __init__(self):
        self._stop_event = asyncio.Event()
        self._task: None | asyncio.Task = None

    async def start(self):
        if self._task:
            raise RuntimeError("Consumer already started.")
        self._stop_event.clear()
        self._task = asyncio.create_task(self._start())

    async def _start(self):
        await self.initlize()
        while not await event_wait(self._stop_event, 0.1):
            if self._stop_event.is_set():
                break
            try:
                await self.run()
            except Exception as e:
                logger.exception(e)

        await self.cleanup()

    async def stop(self):
        self._stop_event.set()
        if self._task:
            logger.info("Waiting for consumer to stop...")
            await self._task

    async def initlize(self):
        """
        Initialize, implement in subclass
        """

    async def run(self):
        """
        Run, implement in subclass
        """

    async def cleanup(self):
        """
        Cleanup, implement in subclass
        """


class Consumer(DeferOperator, RunnableMixin):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        awaitable_function: Callable[[Any], Awaitable[Any]],
        register_function_name: str = None,
        group_name="default-workers",
        consumer_identifier: str = os.getenv(CONSUMER_IDENTIFIER_ENV, uuid.uuid4().hex),
        count_per_fetch: int = 1,
        block_time: int = 1,
        pool_time: int = 5,
        expire_time: int = 24 * 60 * 60,
        process_timeout: int = 60,
        retry_lock_time: int = 300,
        retry_cooldown: int = 60,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
        enable_enque_deferred_job: bool = True,
        max_message_size: int = 1000,
        delete_messgae_after_process: bool = False,
        run_parallel: bool = False,
    ):
        super().__init__(redis, redis_prefix, redis_seperator)

        self.awaitable_function = awaitable_function
        self.register_function_name = register_function_name or awaitable_function.__name__
        self.group_name = group_name
        self.consumer_identifier = consumer_identifier
        self.count_per_fetch = count_per_fetch

        self.block_time = block_time
        self.pool_time = pool_time
        self.expire_time = expire_time
        self.process_timeout = process_timeout
        self.retry_lock_time = retry_lock_time
        self.retry_cooldown = retry_cooldown

        self.enable_enque_deferred_job = enable_enque_deferred_job
        self.max_message_size = max_message_size
        self.delete_messgae_after_process = delete_messgae_after_process
        self.run_parallel = run_parallel

    @property
    def retry_lock_key(self) -> str:
        return self.get_redis_key(self.stream_name, self.group_name, "lock")

    @property
    def retry_cooldown_key(self) -> str:
        return self.get_redis_key(self.stream_name, self.group_name, "cooldown")

    @property
    def stream_name(self) -> str:
        return self.get_stream_name(self.register_function_name)

    @property
    def deferred_key(self) -> str:
        return self.get_deferred_key(self.register_function_name)

    async def _consume(self):
        try:
            if await self._acquire_retry_lock():
                await self._process_unacked_job()
        finally:
            await self._release_retry_lock()

        if self.run_parallel:
            await self._pool_job_prallel()
        else:
            await self._pool_job()

    async def _process_unacked_job(self):
        pass

    async def _pool_job(self):
        _, messages = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_identifier,
            streams={self.stream_name: ">"},
            count=self.count_per_fetch,
            block=self.block_time,
        )[0]
        for message_id, serialized_job in messages:
            job = Job.from_redis(serialized_job)
            try:
                await self.awaitable_function(*job.args, **job.kwargs)
            except Exception as e:
                logger.exception(e)

            await self.redis.xack(self.stream_name, self.group_name, job.job_id)

            if self.delete_messgae_after_process:
                await self.redis.xdel(self.stream_name, message_id)

    async def _pool_job_prallel(self):
        _, messages = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_identifier,
            streams={self.stream_name: ">"},
            count=self.count_per_fetch,
            block=self.block_time,
        )[0]

        async def _job_wrap(message_id, *args, **kwargs):
            await self.awaitable_function(*args, **kwargs)
            return message_id

        jobs = {
            messages_id: Job.from_redis(serialized_job) for messages_id, serialized_job in messages
        }
        results = await asyncio.gather(
            *[_job_wrap(message_id, *job.args, **job.kwargs) for message_id, job in jobs.items()],
            return_exceptions=True,
        )

        for message_id, result in zip(jobs, results):
            if isinstance(result, Exception):
                logger.exception(result)
                continue

            await self.redis.xack(self.stream_name, self.group_name, message_id)
            if self.delete_messgae_after_process:
                await self.redis.xdel(self.stream_name, message_id)

    async def _acquire_retry_lock(self) -> bool:
        return await self.redis.get(self.reprocess_key) == self.job_id or await self.redis.set(
            self.reprocess_key,
            self.job_id,
            ex=self.process_unacked_events_timeout,
            nx=True,
        )

    async def _release_retry_lock(self) -> bool:
        lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        """

        deleted = bool(
            await self.redis.eval(lua_script, 1, self.reprocess_key, self.consumer_identifier)
        )
        return deleted

    async def initlize(self):
        try:
            logger.info(
                f"Creating consumer group: {self.group_name} for stream: {self.stream_name}"
            )
            await self.redis.xgroup_create(self.stream_name, self.group_name, 0, mkstream=True)
        except redis.ResponseError as e:
            if e.args[0] != "BUSYGROUP Consumer Group name already exists":
                raise
            logger.info(f"Consumer group already exists: {self.group_name}, skipping...")

    async def run(self):
        if self.enable_enque_deferred_job:
            await self.enque_deferred_job(self.register_function_name, self.max_message_size)
        await self._consume()

    async def cleanup(self):
        await self.redis.xgroup_delconsumer(self.stream_name, self.group_name, self.consumer_name)
        if await self._release_retry_lock():
            logger.info(f"Stop reprocessing unprocessed events: {self.job_id}")
