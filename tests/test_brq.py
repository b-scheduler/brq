import asyncio

import pytest

from brq.consumer import Consumer
from brq.producer import Producer


async def mock_consume(echo_str: str):
    print(echo_str)


async def test_consume_function(async_redis_client):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)

    await producer.emit_job("mock_consume", ["hello"])
    await consumer.initialize()
    await consumer.run()


if __name__ == "__main__":
    pytest.main(["-s", "-vv", __file__])
