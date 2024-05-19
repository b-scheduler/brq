import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator

import redis.asyncio as redis

from brq.log import logger


async def get_current_timestamp(
    redis_client: redis.Redis | redis.RedisCluster,
) -> int:
    return int((await redis_client.time())[0])


async def get_current_timestamp_ms(
    redis_client: redis.Redis | redis.RedisCluster,
) -> int:
    return int((await redis_client.time())[0] * 1000 + (await redis_client.time())[1] / 1000)


def timestamp_to_datetime(timestamp: int, tz=None) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=tz)


def timestamp_ms_to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000)


async def event_wait(evt, timeout):
    # suppress TimeoutError because we'll return False in case of timeout
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(evt.wait(), timeout)
    return evt.is_set()


def get_redis_url(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    cluster: bool = False,
    tls: bool = False,
):
    if not cluster:
        url = f"{host}:{port}/{db}"
    else:
        logger.debug("Cluster mode, no need to specify db")
        url = f"{host}:{port}"

    if tls:
        return f"rediss://{url}"
    else:
        return f"redis://{url}"


@asynccontextmanager
async def get_redis_client(
    redis_url,
    is_cluster: bool = False,
) -> AsyncGenerator[Any, redis.Redis | redis.RedisCluster]:
    if is_cluster:
        redis_client = redis.RedisCluster.from_url(redis_url, decode_responses=True)
    else:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    try:
        yield redis_client
    finally:
        await redis_client.aclose()
