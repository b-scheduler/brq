import os

from brq.configs import BrqConfig
from brq.producer import Producer


async def main():
    config = BrqConfig()
    async with config.open_redis_client() as async_redis_client:
        await Producer(
            async_redis_client,
            redis_prefix=config.redis_key_prefix,
            redis_seperator=config.redis_key_seperator,
            max_message_len=config.producer_max_message_length,
        ).run_job("echo", ["hello"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
