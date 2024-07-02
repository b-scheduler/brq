from datetime import datetime
from typing import Any
from uuid import uuid4

import redis.asyncio as redis

from brq.log import logger
from brq.models import Job
from brq.operator import BrqOperator


class Producer(BrqOperator):
    """
    Producer to publish jobs to redis stream

    Use `max_message_len` to control max message length.

    Job control methods:
    * Call `run_job` to emit job.
    * Call `prune` to remove all jobs.

    Queue methods:
    * Call `count_stream` to count all jobs.
    * Call `count_deferred_jobs` to count deferred jobs.
    * Call `count_unacked_jobs` to count unacked jobs.
    * Call `count_dead_messages` to count all dead messages.

    Some higher level methods, see ``BrqOperator``

    For inspect the whole brq, see ``browser.py``

    Args:
        redis (redis.Redis | redis.RedisCluster): async redis client
        redis_prefix (str, optional): redis prefix. Defaults to "brq".
        redis_seperator (str, optional): redis seperator. Defaults to ":".
        max_message_len (int, optional): max message length. Defaults to 1000. Follow redis stream `maxlen`.
    """

    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
        max_message_len: int = 1000,
    ):
        super().__init__(redis, redis_prefix, redis_seperator)
        self.max_message_len = max_message_len

    async def run_job(
        self,
        function_name: str,
        args: list[Any] = None,
        kwargs: dict[str, Any] = None,
        defer_until: datetime = None,
        defer_hours: int = 0,
        defer_minutes: int = 0,
        defer_seconds: int = 0,
        unique: bool = True,
    ) -> Job:
        """
        Emit job to redis stream. The args and kwargs will be serialized to json.

        If `defer_until` is not None, job will be deferred until `defer_until`.

        Else if any of `defer_hours`, `defer_minutes`, `defer_seconds` is not 0,
        job will be deferred for `defer_hours` hours, `defer_minutes` minutes, `defer_seconds` seconds.

        Args:
            function_name (str): function name
            args (list, optional): args. Defaults to None.
            kwargs (dict, optional): kwargs. Defaults to None.
            defer_until (datetime, optional): defer until. Defaults to None.
            defer_hours (int, optional): defer hours. Defaults to 0.
            defer_minutes (int, optional): defer minutes. Defaults to 0.
            defer_seconds (int, optional): defer seconds. Defaults to 0.
            unique (bool, optional): The deferred job will be unique. Defaults to True.

        Example:
            >>> await producer.run_job('function_name', args=[], kwargs={})

        Returns:
            Job: created job
        """

        args = args or []
        kwargs = kwargs or {}

        defer_until = await self.get_defer_timestamp_ms(
            defer_until, defer_hours, defer_minutes, defer_seconds
        )

        if defer_until:
            logger.debug(
                f"Deferring job: {function_name} until {datetime.fromtimestamp(defer_until / 1000)}"
            )
            job = await self.run_deferred_job(
                function_name,
                defer_until,
                args,
                kwargs,
                unique=unique,
            )
        else:
            logger.debug(f"Scheduling job: {function_name}")
            job = await self._run_job(function_name, args, kwargs)

        return job

    async def _run_job(
        self, function_name: str, args: list[Any] = None, kwargs: dict[str, Any] = None
    ) -> Job:
        stream_name = self.get_stream_name(function_name)
        created_at = await self.get_current_timestamp_ms(self.redis)

        job = Job(
            args=args or [],
            kwargs=kwargs or {},
            create_at=created_at,
        )
        await self.redis.xadd(stream_name, job.to_message(), maxlen=self.max_message_len)
        return job

    async def run_deferred_job(
        self,
        function_name: str,
        defer_until: int,
        args: list[Any] = None,
        kwargs: dict[str, Any] = None,
        unique: bool = True,
    ) -> Job:
        if unique:
            job = Job(
                args=args or [],
                kwargs=kwargs or {},
                create_at=await self.get_current_timestamp_ms(self.redis),
                uid=uuid4().hex,
            )
        else:
            job = Job(
                args=args or [],
                kwargs=kwargs or {},
            )
        return await self.emit_deferred_job(function_name, defer_until, job)

    async def prune(self, function_name: str):
        """
        Prune all jobs for function_name

        Args:
            function_name (str): function name
        """
        stream_name = self.get_stream_name(function_name)
        defer_key = self.get_deferred_key(function_name)
        dead_key = self.get_dead_message_key(function_name)
        await self.redis.delete(stream_name, defer_key, dead_key)

    async def remove_deferred_job(
        self,
        function_name: str,
        args: list[Any] = None,
        kwargs: dict[str, Any] = None,
    ):
        """
        Only not unique job can be removed
        """

        job = Job(
            args=args or [],
            kwargs=kwargs or {},
            create_at=0,
        )
        await self._remove_deferred_job(function_name, job)

    async def deferred_job_exists(
        self, function_name: str, args: list[Any] = None, kwargs: dict[str, Any] = None
    ) -> bool:
        """
        Only not unique job can be detected
        """
        job = Job(
            args=args or [],
            kwargs=kwargs or {},
            create_at=0,
        )
        return await self._deferred_job_exists(function_name, job)
