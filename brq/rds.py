class RedisOperator:
    def __init__(
        self,
        redis_prefix: list[str] = ["brq"],
        redis_seperator: str = ":",
    ):
        self.redis_prefix = redis_prefix
        self.redis_seperator = redis_seperator

    def get_redis_key(self, *key: list[str]) -> str:
        return self.redis_seperator.join(self.redis_prefix + list(key))

    def get_stream_name(self, function_name: str) -> str:
        return self.get_redis_key(function_name)

    def get_deferred_key(self, function_name: str) -> str:
        return self.get_redis_key(function_name, "deferred")
