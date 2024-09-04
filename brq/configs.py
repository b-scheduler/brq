import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import redis.asyncio as redis
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from brq.tools import get_redis_client, get_redis_url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="brq_",
        env_file=".env",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


class RedisSettingsMixin:
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_cluster: bool = False
    redis_tls: bool = False
    redis_username: str = ""
    redis_password: str = ""

    @property
    def redis_url(self) -> str:
        return get_redis_url(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            cluster=self.redis_cluster,
            tls=self.redis_tls,
            username=self.redis_username,
            password=self.redis_password,
        )

    @asynccontextmanager
    async def open_redis_client(
        self,
    ) -> AsyncGenerator[Any, redis.Redis | redis.RedisCluster]:
        async with get_redis_client(self.redis_url) as redis_client:
            yield redis_client


class BrqConfig(Settings, RedisSettingsMixin):
    redis_key_prefix: str = "brq"
    redis_key_seperator: str = ":"

    producer_max_message_length: int = 1000

    consumer_group_name: str = "default-workers"
    consumer_identifier: str = uuid.uuid4().hex
    consumer_count_per_fetch: int = 1
    consumer_block_time: int = 1
    consumer_expire_time: int = 60 * 60
    consumer_process_timeout: int = 60
    consumer_retry_lock_time: int = 300
    consumer_retry_cooldown_time: int = 60
    consumer_enable_enque_deferred_job: bool = True
    consumer_enable_reprocess_timeout_job: bool = True
    consumer_enable_dead_queue: bool = True
    consumer_max_message_len: int = 1000
    consumer_delete_message_after_process: bool = False
    consumer_run_parallel: bool = False

    daemon_concurrency: int = 1
