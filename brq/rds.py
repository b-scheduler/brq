import redis.asyncio as redis

from brq.tools import (
    get_current_timestamp,
    get_current_timestamp_ms,
    timestamp_to_datetime,
)


class RedisOperator:
    def __init__(
        self,
        redis_prefix: list[str] = ["brq"],
        redis_seperator: str = ":",
    ):
        self.redis_prefix = redis_prefix
        self.redis_seperator = redis_seperator

    def get_redis_key(self, *key: list[str]) -> str:
        return self.redis_seperator.join([self.redis_prefix] + list(key))

    def get_stream_name(self, function_name: str) -> str:
        return self.get_redis_key(function_name, "stream")

    def get_dead_message_key(self, function_name: str) -> str:
        return self.get_redis_key(function_name, "dead")

    def get_deferred_key(self, function_name: str) -> str:
        return self.get_redis_key(function_name, "deferred")

    async def get_current_timestamp(self, redis_client: redis.Redis | redis.RedisCluster) -> int:
        return await get_current_timestamp(redis_client)

    async def get_current_timestamp_ms(self, redis_client: redis.Redis | redis.RedisCluster) -> int:
        return await get_current_timestamp_ms(redis_client)

    async def get_current_datetime(
        self, redis_client: redis.Redis | redis.RedisCluster, tz=None
    ) -> str:
        return timestamp_to_datetime(await get_current_timestamp(redis_client), tz=tz)
