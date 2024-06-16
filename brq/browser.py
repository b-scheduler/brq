from typing import AsyncIterator

import redis.asyncio as redis
from pydantic import BaseModel

from brq.log import logger
from brq.rds import RedisOperator


class ScannerMixin(RedisOperator):
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

    async def status(self):
        async for stream_key in self.scan_iter():
            print(stream_key)
