from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator

import redis.asyncio as redis
from pydantic import BaseModel

from brq.defer_operator import DeferOperator
from brq.log import logger
from brq.models import Job


class ScannerMixin(DeferOperator):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
    ):
        self.redis = redis
        self.redis_prefix = redis_prefix
        self.redis_seperator = redis_seperator

    def scan_iter(self) -> AsyncIterator[str]:
        if self.redis_prefix == "*":
            scan_key = "*"
        else:
            scan_key = self.get_redis_key("*")
        logger.debug(f"Scanning {scan_key}")
        return self.redis.scan_iter(match=scan_key, _type="stream")

    async def parse_redis_key(self, redis_key: str) -> tuple[str, str]:
        function_and_domain = redis_key[len(self.redis_prefix + self.redis_seperator) :]
        function, domain_key = function_and_domain.split(self.redis_seperator)
        return function, domain_key

    async def get_stream_info(self, stream_key: str) -> _StreamInfo:
        pass

    async def get_group_info(self, stream_key: str) -> _GroupInfo:
        pass

    async def get_consumer_infos(self, stream_key: str) -> dict[str, _ConsumerInfo]:
        pass


class _StreamInfo(BaseModel):
    pass


class _GroupInfo(BaseModel):
    pass


class _ConsumerInfo(BaseModel):
    pass


class DeferQueueInfo(BaseModel):
    defer_until: datetime
    job: Job


class DeadQueueInfo(BaseModel):
    dead_at: datetime
    job: Job


class JobQueueInfo(BaseModel):
    stream_info: _StreamInfo
    group_info: _GroupInfo
    consumer_infos: dict[str, _ConsumerInfo]
    defer_queue_info: list[DeferQueueInfo]
    dead_queue_info: list[DeadQueueInfo]


class BrqJobsInfo(BaseModel):
    job_queue_info: dict[str, JobQueueInfo]


class Browser(ScannerMixin):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: str = "brq",
        redis_seperator: str = ":",
        group_name: str = "default-workers",
    ):
        super().__init__(redis, redis_prefix, redis_seperator)
        self.group_name = group_name

    async def status(self) -> BrqJobsInfo:
        inspect_functions = []
        async for stream_key in self.scan_iter():
            function_name, _ = await self.parse_redis_key(stream_key)
            inspect_functions.append(function_name)
        return BrqJobsInfo(
            job_queue_info={
                function_name: self.one_queue_info(function_name)
                for function_name in inspect_functions
            }
        )

    async def one_queue_info(self, function_name: str) -> JobQueueInfo:
        stream_key = self.get_stream_name(function_name)
        dead_job_info = [
            DeadQueueInfo(
                dead_at=k,
                job=v,
            )
            for k, v in (await self.get_dead_messages(function_name)).items()
        ]

        defer_job_info = [
            DeferQueueInfo(
                defer_until=k,
                job=v,
            )
            for k, v in (await self.get_deferred_jobs(function_name)).items()
        ]

        return JobQueueInfo(
            stream_info=await self.get_stream_info(stream_key),
            group_info=await self.get_group_info(stream_key),
            consumer_infos=await self.get_consumer_infos(stream_key),
            defer_queue_info=defer_job_info,
            dead_queue_info=dead_job_info,
        )
