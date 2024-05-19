import redis.asyncio as redis

from brq.log import logger
from brq.rds import RedisOperator


class DeferOperator(RedisOperator):
    def __init__(
        self,
        redis: redis.Redis | redis.RedisCluster,
        redis_prefix: list[str] = ["brq"],
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
            logger.info(f"Enqueued deferred jobs: {elements}")
