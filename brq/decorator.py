import asyncio
from functools import partial
from typing import Awaitable, Callable

from anyio import CapacityLimiter

from brq.configs import BrqConfig
from brq.consumer import Consumer
from brq.daemon import Daemon
from brq.log import logger
from brq.tools import ensure_awaitable


class BrqTaskWrapper:
    def __init__(
        self,
        func: Callable | Awaitable,
        config: BrqConfig,
        register_function_name: str | None = None,
    ):
        self._func = func
        self.config = config
        self.register_function_name = register_function_name or func.__name__

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def serve(self):
        asyncio.run(self._serve())

    async def _serve(self):
        async with self.config.open_redis_client() as async_redis_client:
            awaitable_function = ensure_awaitable(
                self._func,
                limiter=CapacityLimiter(total_tokens=self.config.daemon_concurrency),
            )
            consumer_builder = partial(
                Consumer,
                redis_prefix=self.config.redis_key_prefix,
                redis_seperator=self.config.redis_key_seperator,
                redis=async_redis_client,
                awaitable_function=awaitable_function,
                register_function_name=self.register_function_name,
                group_name=self.config.consumer_group_name,
                consumer_identifier=self.config.consumer_identifier,
                count_per_fetch=self.config.consumer_count_per_fetch,
                block_time=self.config.consumer_block_time,
                expire_time=self.config.consumer_expire_time,
                process_timeout=self.config.consumer_process_timeout,
                retry_lock_time=self.config.consumer_retry_lock_time,
                retry_cooldown_time=self.config.consumer_retry_cooldown_time,
                enable_enque_deferred_job=self.config.consumer_enable_enque_deferred_job,
                enable_reprocess_timeout_job=self.config.consumer_enable_reprocess_timeout_job,
                enable_dead_queue=self.config.consumer_enable_dead_queue,
                max_message_len=self.config.consumer_max_message_len,
                delete_message_after_process=self.config.consumer_delete_message_after_process,
                run_parallel=self.config.consumer_run_parallel,
            )
            daemon = Daemon(*[consumer_builder() for _ in range(self.config.daemon_concurrency)])
            await daemon.run_forever()


def task(
    _func=None,
    *,
    config: BrqConfig | None = None,
    register_function_name: str | None = None,
    # Redis connection
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_db: int | None = None,
    redis_cluster: bool | None = None,
    redis_tls: bool | None = None,
    redis_username: str | None = None,
    redis_password: str | None = None,
    # Scope
    redis_key_prefix: str | None = None,
    redis_key_seperator: str | None = None,
    # Consumer
    group_name: str | None = None,
    consumer_identifier: str | None = None,
    count_per_fetch: int | None = None,
    block_time: int | None = None,
    expire_time: int | None = None,
    process_timeout: int | None = None,
    retry_lock_time: int | None = None,
    retry_cooldown_time: int | None = None,
    enable_enque_deferred_job: bool | None = None,
    enable_reprocess_timeout_job: bool | None = None,
    enable_dead_queue: bool | None = None,
    max_message_len: int | None = None,
    delete_message_after_process: bool | None = None,
    run_parallel: bool | None = None,
    # Daemon
    daemon_concurrency: int | None = None,
):

    def _wrapper(
        func,
        config: BrqConfig | None = None,
        register_function_name: str | None = None,
    ):

        return BrqTaskWrapper(func, config, register_function_name)

    kwargs = {
        k: v
        for k, v in {
            "redis_host": redis_host,
            "redis_port": redis_port,
            "redis_db": redis_db,
            "redis_cluster": redis_cluster,
            "redis_tls": redis_tls,
            "redis_username": redis_username,
            "redis_password": redis_password,
            "redis_key_prefix": redis_key_prefix,
            "redis_key_seperator": redis_key_seperator,
            "consumer_group_name": group_name,
            "consumer_identifier": consumer_identifier,
            "consumer_count_per_fetch": count_per_fetch,
            "consumer_block_time": block_time,
            "consumer_expire_time": expire_time,
            "consumer_process_timeout": process_timeout,
            "consumer_retry_lock_time": retry_lock_time,
            "consumer_retry_cooldown_time": retry_cooldown_time,
            "consumer_enable_enque_deferred_job": enable_enque_deferred_job,
            "consumer_enable_reprocess_timeout_job": enable_reprocess_timeout_job,
            "consumer_enable_dead_queue": enable_dead_queue,
            "consumer_max_message_len": max_message_len,
            "consumer_delete_message_after_process": delete_message_after_process,
            "consumer_run_parallel": run_parallel,
            "daemon_concurrency": daemon_concurrency,
        }.items()
        if v is not None
    }

    if not config:
        logger.info("Initializing config from environment variables and .env file")
        config = BrqConfig()
    else:
        logger.info("Using custom config")
        config = config

    config = BrqConfig.model_validate(
        {
            **config.model_dump(),
            **kwargs,
        }
    )

    if _func is None:
        return partial(
            _wrapper,
            config=config,
            register_function_name=register_function_name,
        )
    else:
        return _wrapper(_func, config=config, register_function_name=register_function_name)
