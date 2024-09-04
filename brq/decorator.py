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
):
    if not config:
        logger.info("Initializing config from environment variables and .env file")
        config = BrqConfig()
    else:
        logger.info("Using custom config")
        config = config

    def _wrapper(func, config, register_function_name):
        if not config:
            logger.info("Initializing config from environment variables and .env file")
            config = BrqConfig()
        else:
            logger.info("Using custom config")
            config = config

        return BrqTaskWrapper(func, config, register_function_name)

    if _func is None:
        return _wrapper
    else:
        return _wrapper(_func, config=config, register_function_name=register_function_name)
