from __future__ import annotations

from datetime import datetime
from typing import AsyncIterator, Optional, Union

import redis.asyncio as redis
from pydantic import BaseModel

from brq.log import logger
from brq.models import Job
from brq.operator import BrqOperator


def str_if_bytes(value: Union[str, bytes]) -> str:
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value


def pairs_to_dict(response, decode_keys=False, decode_string_values=False):
    """Create a dict given a list of key/value pairs"""
    if response is None:
        return {}
    if decode_keys or decode_string_values:
        # the iter form is faster, but I don't know how to make that work
        # with a str_if_bytes() map
        keys = response[::2]
        if decode_keys:
            keys = map(str_if_bytes, keys)
        values = response[1::2]
        if decode_string_values:
            values = map(str_if_bytes, values)
        return dict(zip(keys, values))
    else:
        it = iter(response)
        return dict(zip(it, it))


def prettify(function_name: str, job_queue_info: JobQueueInfo) -> str:
    stream_brief = (
        f"""
    length: {job_queue_info.stream_full_info.length}
    groups: {job_queue_info.stream_full_info.groups}
    radix_tree_keys: {job_queue_info.stream_full_info.radix_tree_keys}
    radix_tree_nodes: {job_queue_info.stream_full_info.radix_tree_nodes}
    last_generated_id: {job_queue_info.stream_full_info.last_generated_id}
    max_deleted_entry_id: {job_queue_info.stream_full_info.max_deleted_entry_id}
    entries_added: {job_queue_info.stream_full_info.entries_added}
    entries: {job_queue_info.stream_full_info.entries}
    group: {job_queue_info.stream_full_info.group}
""".strip()
        if job_queue_info.stream_full_info
        else "None"
    )

    template = f"""------------------------------------------------------
Function: {function_name}

  Brief for stream:
    {stream_brief}

  Brief for defer queue:
    length: {len(job_queue_info.defer_queue_info)}
    details: {job_queue_info.defer_queue_info}

  Brief for dead queue:
    length: {len(job_queue_info.dead_queue_info)}
    details: {job_queue_info.dead_queue_info}
"""

    return template.strip() + "\n"


class ScannerMixin(BrqOperator):
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
        return self.redis.scan_iter(match=scan_key)

    async def parse_redis_key(self, redis_key: str) -> tuple[str, str]:
        function_and_domain = redis_key[len(self.redis_prefix + self.redis_seperator) :]
        function, domain_key = function_and_domain.split(self.redis_seperator)

        if "{" in function:
            function = function.replace("{", "").replace("}", "")

        return function, domain_key

    async def get_stream_full_info(self, stream_key: str) -> _StreamFullInfo | None:
        if not await self.redis.exists(stream_key):
            return None

        try:
            full_response = await self.redis.xinfo_stream(stream_key, full=True)
        except IndexError:
            # https://github.com/redis/redis-py/pull/3282
            logger.warning(
                "Due to redis-py bug, only brief info when no consumer group found. Please upgrade redis>=5.0.7 to fix this."
            )
            full_response = await self.redis.xinfo_stream(stream_key)

        return _StreamFullInfo.from_xinfo_stream_full(full_response)


class _StreamFullInfo(BaseModel):
    length: int
    groups: int
    radix_tree_keys: int
    radix_tree_nodes: int
    last_generated_id: str
    max_deleted_entry_id: Optional[str] = None
    entries_added: Optional[int] = None
    entries: dict[str, Job]
    group: dict[str, _GroupInfo]

    @classmethod
    def from_xinfo_stream_full(cls, full_response: dict) -> _StreamFullInfo:
        entries = (
            {k: Job.model_validate_json(v["payload"]) for k, v in full_response["entries"].items()}
            if "entries" in full_response
            else {}
        )
        return cls(
            length=full_response["length"],
            groups=(
                len(full_response["groups"])
                if isinstance(full_response["groups"], list)
                else full_response["groups"]
            ),
            radix_tree_keys=full_response["radix-tree-keys"],
            radix_tree_nodes=full_response["radix-tree-nodes"],
            last_generated_id=full_response["last-generated-id"],
            max_deleted_entry_id=full_response.get("max-deleted-entry-id"),
            entries_added=full_response.get("entries-added"),
            entries=entries,
            group=(
                {g["name"]: _GroupInfo.from_xinfo_stream_full(g) for g in full_response["groups"]}
                if isinstance(full_response["groups"], list)
                else {}
            ),
        )


class _GroupInfo(BaseModel):
    name: str
    last_delivered_id: str
    entries_read: Optional[int] = None
    lag: Optional[int] = None
    pel_count: int
    pending: list[_PendingJobInfo]
    consumers: list[_ConsumerInfo]

    @classmethod
    def from_xinfo_stream_full(cls, full_response: dict) -> _GroupInfo:
        return cls(
            name=full_response["name"],
            last_delivered_id=full_response["last-delivered-id"],
            entries_read=full_response.get("entries-read"),
            lag=full_response.get("lag"),
            pel_count=full_response["pel-count"],
            pending=[_PendingJobInfo.from_xinfo_stream_full(p) for p in full_response["pending"]],
            consumers=[_ConsumerInfo.from_xinfo_stream_full(c) for c in full_response["consumers"]],
        )


class _PendingJobInfo(BaseModel):
    id: str
    consumer: str
    delivered_at: datetime
    delivered_times: int

    @classmethod
    def from_xinfo_stream_full(cls, full_response: dict) -> _PendingJobInfo:
        return cls(
            id=full_response[0],
            consumer=full_response[1],
            delivered_at=datetime.fromtimestamp(full_response[2] / 1000),
            delivered_times=full_response[3],
        )


class _ConsumerInfo(BaseModel):
    """
    Note that before Redis 7.2.0, seen-time used to denote the last successful interaction.
    In 7.2.0, active-time was added and seen-time was changed to denote the last attempted interaction.
    """

    name: str
    seen_time: datetime
    active_time: Optional[datetime] = None
    pel_count: int
    pending: list[_PendingJobInfo]

    @classmethod
    def from_xinfo_stream_full(cls, full_response: dict | list) -> _ConsumerInfo:
        if isinstance(full_response, list):
            # Incase redis-py not parse consumer
            full_response = pairs_to_dict(full_response, decode_keys=True)

        active_time = full_response.get("active-time")
        if active_time:
            active_time = datetime.fromtimestamp(active_time / 1000)
        else:
            active_time = None
        return cls(
            name=full_response["name"],
            seen_time=datetime.fromtimestamp(full_response["seen-time"] / 1000),
            active_time=active_time,
            pel_count=full_response["pel-count"],
            pending=[
                _PendingJobInfo(
                    id=p[0],
                    consumer=full_response["name"],
                    delivered_at=p[1],
                    delivered_times=p[2],
                )
                for p in full_response["pending"]
            ],
        )


class DeferQueueInfo(BaseModel):
    defer_until: datetime
    job: Job


class DeadQueueInfo(BaseModel):
    dead_at: datetime
    job: Job


class JobQueueInfo(BaseModel):
    stream_full_info: Optional[_StreamFullInfo]
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
    ):
        super().__init__(redis, redis_prefix, redis_seperator)

    async def status(self) -> BrqJobsInfo:
        inspect_functions = []
        async for stream_key in self.scan_iter():
            function_name, _ = await self.parse_redis_key(stream_key)
            (
                inspect_functions.append(function_name)
                if function_name not in inspect_functions
                else None
            )
        return BrqJobsInfo(
            job_queue_info={
                function_name: await self.one_queue_info(function_name)
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
            stream_full_info=await self.get_stream_full_info(stream_key),
            defer_queue_info=defer_job_info,
            dead_queue_info=dead_job_info,
        )
