import asyncio

import pytest

from brq.consumer import Consumer
from brq.producer import Producer


async def mock_consume(echo_str: str):
    print(echo_str)


called = False


async def mock_consume_raise_exception(echo_str: str):
    global called
    if not called:
        called = True
        raise Exception("raise exception")
    else:
        print(echo_str)
        called = False


@pytest.mark.parametrize("run_parallel", [False, True])
async def test_consume_function(async_redis_client, capfd, run_parallel):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume, run_parallel=run_parallel)

    await producer.run_job("mock_consume", ["hello"])
    await consumer.initialize()
    await consumer.run()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_consume_func_with_name(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_consume,
        register_function_name="mock_consume_with_name",
    )

    await producer.run_job("mock_consume_with_name", ["hello"])
    await consumer.initialize()
    await consumer.run()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_prune(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)

    await producer.run_job("mock_consume", ["hello"])
    await producer.prune("mock_consume")
    await consumer.initialize()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await consumer.cleanup()


async def test_unprocessed_job(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume_raise_exception, process_timeout=0.001)

    await producer.run_job("mock_consume_raise_exception", ["hello"])
    await consumer.initialize()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(0.1)
    await consumer.run()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_process_dead_jobs(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume_raise_exception, expire_time=0.001)

    await producer.run_job("mock_consume_raise_exception", ["hello"])
    await consumer.initialize()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(0.1)
    await consumer._move_expired_jobs()
    assert await producer.count_dead_messages("mock_consume_raise_exception") == 1
    await consumer.process_dead_jobs()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_deferred_job(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)

    await producer.run_job("mock_consume", ["hello"], defer_seconds=1)
    assert await producer.get_deferred_jobs("mock_consume")
    await consumer.initialize()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(1.1)
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


if __name__ == "__main__":
    pytest.main(["-s", "-vv", __file__])
