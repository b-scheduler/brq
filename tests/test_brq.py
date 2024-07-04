import asyncio

import pytest

from brq.browser import Browser
from brq.consumer import Consumer
from brq.producer import Producer


async def mock_consume(echo_str: str):
    print(echo_str)


async def delay_job(echo_str: str, delay_seconds: int = 1):
    await asyncio.sleep(delay_seconds)
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
    browser = Browser(async_redis_client)
    await browser.status()
    await producer.run_job("mock_consume", ["hello"])
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_count_unacked_jobs(async_redis_client):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, delay_job)
    browser = Browser(async_redis_client)
    assert await consumer.count_unacked_jobs("delay_job") == 0

    await browser.status()
    await producer.run_job("delay_job", ["hello"])
    await browser.status()
    await consumer.initialize()
    await browser.status()
    loop = asyncio.get_event_loop()
    loop.create_task(consumer.run())
    await asyncio.sleep(0.1)
    assert await consumer.count_unacked_jobs("delay_job") == 1
    await browser.status()
    await asyncio.sleep(1.1)
    assert await consumer.count_unacked_jobs("delay_job") == 0
    await consumer.cleanup()


async def test_consume_func_with_name(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_consume,
        register_function_name="mock_consume_with_name",
    )
    browser = Browser(async_redis_client)
    await browser.status()
    await producer.run_job("mock_consume_with_name", ["hello"])
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_prune(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)
    browser = Browser(async_redis_client)

    await browser.status()
    await producer.run_job("mock_consume", ["hello"])
    await browser.status()
    await producer.prune("mock_consume")
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await consumer.cleanup()


async def test_unprocessed_job(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_consume_raise_exception,
        process_timeout=0.001,
        retry_cooldown_time=1,
    )
    browser = Browser(async_redis_client)

    await browser.status()
    await producer.run_job("mock_consume_raise_exception", ["hello"])
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(1.1)
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


@pytest.mark.parametrize("enable_dead_queue", [True, False])
async def test_process_dead_jobs(async_redis_client, capfd, enable_dead_queue):
    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_consume_raise_exception,
        expire_time=0.001,
        enable_dead_queue=enable_dead_queue,
    )
    browser = Browser(async_redis_client)
    assert await producer.count_dead_messages("mock_consume_raise_exception") == 0

    await browser.status()
    await producer.run_job("mock_consume_raise_exception", ["hello"])
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(0.1)
    await consumer._move_expired_jobs()
    if enable_dead_queue:
        assert await producer.count_dead_messages("mock_consume_raise_exception") == 1
    else:
        assert await producer.count_dead_messages("mock_consume_raise_exception") == 0
    await consumer.process_dead_jobs()
    await browser.status()

    out, err = capfd.readouterr()
    if enable_dead_queue:
        assert "hello" in out
    await consumer.cleanup()


async def test_deferred_job(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)
    browser = Browser(async_redis_client)

    await browser.status()
    await producer.run_job("mock_consume", ["hello"], defer_seconds=1)
    await browser.status()
    assert await producer.get_deferred_jobs("mock_consume")
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()
    out, err = capfd.readouterr()
    assert "hello" not in out
    await asyncio.sleep(1.1)
    await browser.status()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


async def test_not_unique_deferred_job(
    async_redis_client,
    capfd,
):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)
    browser = Browser(async_redis_client)

    await browser.status()
    assert not await producer.deferred_job_exists("mock_consume", ["hello"])
    await producer.run_job("mock_consume", ["hello"], defer_seconds=2, unique=False)
    await producer.run_job("mock_consume", ["hello"], defer_seconds=2, unique=False)
    await browser.status()
    assert await producer.deferred_job_exists("mock_consume", ["hello"])
    assert len(await producer.get_deferred_jobs("mock_consume")) == 1
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()
    out, err = capfd.readouterr()
    assert "hello" not in out

    await asyncio.sleep(2.1)
    await browser.status()
    await consumer.run()
    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


if __name__ == "__main__":
    pytest.main(["-s", "-vv", __file__])
