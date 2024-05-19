import os

from brq.producer import Producer
from brq.tools import get_redis_client, get_redis_url


async def main():
    redis_url = get_redis_url(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        cluster=bool(os.getenv("REDIS_CLUSTER", False)),
        tls=bool(os.getenv("REDIS_TLS", False)),
    )
    async with get_redis_client(redis_url) as async_redis_client:
        await Producer(async_redis_client).run_job("echo", ["hello"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
