import asyncio
import copy
import os
import uuid
from typing import Any, Awaitable, Callable

import redis.asyncio as redis

from brq.log import logger
from brq.models import Job
from brq.operator import BrqOperator
from brq.tools import event_wait

CONSUMER_IDENTIFIER_ENV = "BRQ_CONSUMER_IDENTIFIER"


class CannotProcessError(RuntimeError):
    pass


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
        try:
            await self.initialize()
        except Exception as e:
            # Unrecoverable error
            logger.exception(e)
            exit(1)
        while not await event_wait(self._stop_event, 1 / 1000000):
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
            logger.info("Waiting for consumer finish...")
            await self._task

    async def initialize(self):
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


class Consumer(BrqOperator, RunnableMixin):
    """
    A consumer for redis stream.

    When many replicas of one kind of consumer are created, they should be in **the same** group.
    When many kind of consumers are created, they should be in **different** groups.

    See [example](https://github.com/Wh1isper/brq/tree/main/examples) for running example.

    Args:
        redis(redis.Redis | redis.RedisCluster): Redis client.
        awaitable_function(Callable[[Any], Awaitable[Any]]): The function to be called.
        register_function_name(str): The function name to be registered, if None, will use the function name of `awaitable_function`.
        group_name(str, default="default-workers"): The group name. All replicas should be in the same group.
        consumer_identifier(str, default=os.getenv(CONSUMER_IDENTIFIER_ENV, uuid.uuid4().hex)): The consumer identifier. For restarting in some special case.
        count_per_fetch(int, default=1): The count of message per fetch.
        block_time(int, default=1): The block time of reading stream.
        expire_time(int, default=60*60): The expire time of a message. Expired messages will be moved to dead queue.
        process_timeout(int, default=60): The timeout of a job. If a job is not finished in this time, it will be reprocessed.
        retry_lock_time(int, default=300): The lock time for retrying. Only one consumer can retry jobs at a time.
        retry_cooldown_time(int, default=60): The cooldown time between retrying.
        redis_prefix(str, default="brq"): The prefix of redis key.
        redis_seperator(str, default=":"): The seperator of redis key.
        enable_enque_deferred_job(bool, default=True): Whether to enable enque deferred job. If not, this consumer won't enque deferred jobs.
        enable_reprocess_timeout_job(bool, default=True): Whether to enable reprocess timeout job. If not, this consumer won't reprocess timeout jobs, jobs with expired timeout will be moved to dead queue(if enabled).
        enable_dead_queue(bool, default=True): Whether to enable dead queue. If not, this consumer won't move expired jobs to dead queue but just delete them.
        max_message_len(int, default=1000): The maximum length of a message. Follow redis stream `maxlen`.
        delete_message_after_process(bool, default=False): Whether to delete message after process. If many consumer groups are used, this should be set to False.
        run_parallel(bool, default=False): Whether to run in parallel.
        awaitable_function_callback(Callable[[Job, Exception | Any, "Consumer"], Awaitable[Any]] | None, default=None): The callback function for awaitable_function.
    """

    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        awaitable_function: Callable[[Any], Awaitable[Any]],
        register_function_name: str | None = None,
        group_name="default-workers",
        consumer_identifier: str = os.getenv(CONSUMER_IDENTIFIER_ENV, uuid.uuid4().hex),
        count_per_fetch: int = 1,
        block_time: int = 1,
        expire_time: int = 60 * 60,
        process_timeout: int = 60,
        retry_lock_time: int = 300,
        retry_cooldown_time: int = 60,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
        enable_enque_deferred_job: bool = True,
        enable_reprocess_timeout_job: bool = True,
        enable_dead_queue: bool = True,
        max_message_len: int = 1000,
        delete_message_after_process: bool = False,
        run_parallel: bool = False,
        awaitable_function_callback: (
            Callable[[Job, Exception | Any, "Consumer"], Awaitable[Any]] | None
        ) = None,
    ):

        super().__init__(redis, redis_prefix, redis_seperator)
        self._stop_event = asyncio.Event()
        self._task: None | asyncio.Task = None

        self.awaitable_function = awaitable_function
        self.awaitable_function_callback = awaitable_function_callback
        self.register_function_name = register_function_name or awaitable_function.__name__
        self.group_name = group_name
        self.consumer_identifier = consumer_identifier
        self.count_per_fetch = count_per_fetch

        self.block_time = block_time
        self.expire_time = expire_time
        self.process_timeout = process_timeout

        if self.expire_time < self.process_timeout:
            logger.warning(
                f"expire_time({self.expire_time}) < process_timeout({self.process_timeout}), will causes no job retried but moved dead queue(if enabled)."
            )

        self.retry_lock_time = retry_lock_time
        self.retry_cooldown_time = retry_cooldown_time

        self.enable_enque_deferred_job = enable_enque_deferred_job
        self.enable_reprocess_timeout_job = enable_reprocess_timeout_job
        self.enable_dead_queue = enable_dead_queue
        self.max_message_len = max_message_len
        self.delete_message_after_process = delete_message_after_process
        self.run_parallel = run_parallel

    @property
    def retry_lock_key(self) -> str:
        return f"{self.stream_name}-{self.group_name}-lock"

    @property
    def retry_cooldown_key(self) -> str:
        return f"{self.stream_name}-{self.group_name}-cooldown"

    @property
    def stream_name(self) -> str:
        return self.get_stream_name(self.register_function_name)

    @property
    def deferred_key(self) -> str:
        return self.get_deferred_key(self.register_function_name)

    @property
    def dead_key(self) -> str:
        return self.get_dead_message_key(self.register_function_name)

    async def callback(self, job: Job, exception_or_result: Exception | Any):
        if not self.awaitable_function_callback:
            return
        try:
            await self.awaitable_function_callback(job, exception_or_result, self)
        except Exception as e:
            logger.warning(f"Callback error: {e}")
            logger.exception(e)

    async def _is_retry_cooldown(self) -> bool:
        return await self.redis.exists(self.retry_cooldown_key)

    async def _set_retry_timeout(self):
        await self.redis.set(self.retry_cooldown_key, "1", ex=self.retry_cooldown_time)

    async def _consume(self):
        if self._stop_event.is_set():
            return
        try:
            if not await self._is_retry_cooldown() and await self._acquire_retry_lock():
                await self._process_unacked_job()
                await self._move_expired_jobs()
                await self._set_retry_timeout()
        finally:
            await self._release_retry_lock()

        if self._stop_event.is_set():
            return
        if self.run_parallel:
            await self._pool_job_prallel()
        else:
            await self._pool_job()

    async def process_dead_jobs(self, raise_exception: bool = False) -> list[Job]:
        unprocessed_jobs = []
        for job in (Job.from_redis(j) for j in await self.redis.zrange(self.dead_key, 0, -1)):
            try:
                await self.awaitable_function(*job.args, **job.kwargs)
            except Exception as e:
                logger.exception(e)
                unprocessed_jobs.append(job)
                if raise_exception:
                    raise e
            else:
                await self.redis.zrem(self.dead_key, job.to_redis())

        return unprocessed_jobs

    async def _move_expired_jobs(self):
        while True:
            if self._stop_event.is_set():
                break

            message_id = "0-0"
            expired_messages = (
                await self.redis.xautoclaim(
                    self.stream_name,
                    self.group_name,
                    self.consumer_identifier,
                    min_idle_time=int(self.expire_time * 1000),
                    start_id=message_id,
                    count=100,
                )
            )[1]
            if not expired_messages:
                break

            for message_id, serialized_job in expired_messages:
                if message_id == serialized_job == None:
                    # Fix (None, None) for redis 6.x
                    continue
                job = Job.from_message(serialized_job)
                if self.enable_dead_queue:
                    await self.redis.zadd(self.dead_key, {job.to_redis(): job.create_at})
                    logger.info(f"Put expired job {job} to dead queue")
                await self.redis.xdel(self.stream_name, message_id)
                logger.debug(f"{job} expired")
                await self.callback(job, CannotProcessError("Expired"))

    async def _process_unacked_job(self):
        if not self.enable_reprocess_timeout_job:
            return

        while True:
            if self._stop_event.is_set():
                break

            message_id = "0-0"
            claimed_messages = (
                await self.redis.xautoclaim(
                    self.stream_name,
                    self.group_name,
                    self.consumer_identifier,
                    min_idle_time=int(self.process_timeout * 1000),
                    start_id=message_id,
                    count=10,
                )
            )[1]
            if not claimed_messages:
                break

            for message_id, serialized_job in claimed_messages:
                if message_id == serialized_job == None:
                    # Fix (None, None) for redis 6.x
                    continue
                job = Job.from_message(serialized_job)
                try:
                    r = await self.awaitable_function(*job.args, **job.kwargs)
                except Exception as e:
                    logger.exception(e)
                    await self.callback(job, e)
                else:
                    logger.info(f"Retry {job} successfully")
                    await self.redis.xack(self.stream_name, self.group_name, message_id)

                    if self.delete_message_after_process:
                        await self.redis.xdel(self.stream_name, message_id)
                    await self.callback(job, r)

    async def _pool_job(self):
        poll_result = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_identifier,
            streams={self.stream_name: ">"},
            count=self.count_per_fetch,
            block=int(self.block_time * 1000),
        )
        if not poll_result:
            return

        _, messages = poll_result[0]
        for message_id, serialized_job in messages:
            job = Job.from_message(serialized_job)
            try:
                r = await self.awaitable_function(*job.args, **job.kwargs)
            except Exception as e:
                logger.exception(e)
                await self.callback(job, e)
            else:
                await self.redis.xack(self.stream_name, self.group_name, message_id)

                if self.delete_message_after_process:
                    await self.redis.xdel(self.stream_name, message_id)
                await self.callback(job, r)

    async def _pool_job_prallel(self):
        pool_result = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_identifier,
            streams={self.stream_name: ">"},
            count=self.count_per_fetch,
            block=int(self.block_time * 1000),
        )
        if not pool_result:
            return
        _, messages = pool_result[0]

        async def _job_wrap(message_id, *args, **kwargs):
            await self.awaitable_function(*args, **kwargs)
            return message_id

        jobs = {
            messages_id: Job.from_message(serialized_job)
            for messages_id, serialized_job in messages
        }
        results = await asyncio.gather(
            *[_job_wrap(message_id, *job.args, **job.kwargs) for message_id, job in jobs.items()],
            return_exceptions=True,
        )

        for message_id, result in zip(jobs, results):
            if isinstance(result, Exception):
                logger.exception(result)
                await self.callback(jobs[message_id], result)
                continue

            await self.redis.xack(self.stream_name, self.group_name, message_id)
            if self.delete_message_after_process:
                await self.redis.xdel(self.stream_name, message_id)
            await self.callback(jobs[message_id], result)

    async def _acquire_retry_lock(self) -> bool:
        return await self.redis.get(
            self.retry_lock_key
        ) == self.consumer_identifier or await self.redis.set(
            self.retry_lock_key,
            self.consumer_identifier,
            ex=self.retry_lock_time,
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
            await self.redis.eval(lua_script, 1, self.retry_lock_key, self.consumer_identifier)
        )
        return deleted

    async def initialize(self):
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
            await self.enque_deferred_job(self.register_function_name, self.max_message_len)
        await self._consume()

    async def cleanup(self):
        await self.redis.xgroup_delconsumer(
            self.stream_name, self.group_name, self.consumer_identifier
        )
        if await self._release_retry_lock():
            logger.info(f"Stop reprocessing unprocessed events: {self.consumer_identifier}")
