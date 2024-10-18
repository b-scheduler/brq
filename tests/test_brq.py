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


async def mock_always_consume_raise_exception(echo_str: str):
    raise Exception("raise exception")


callback_called = False


async def mock_callback(job, exception_or_result, consumer):
    global callback_called
    if callback_called:
        raise RuntimeError("Callback already called. Please reset it.")
    callback_called = True


@pytest.mark.parametrize("run_parallel", [False, True])
async def test_consume_function(async_redis_client, capfd, run_parallel):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume, run_parallel=run_parallel)
    browser = Browser(async_redis_client)
    await browser.status()
    await producer.run_job("mock_consume", ["hello"])
    jobs = [job async for job in producer.walk_jobs("mock_consume")]
    assert len(jobs) == 1
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()
    assert "hello" in out
    await consumer.cleanup()


@pytest.mark.parametrize("run_parallel", [False, True])
async def test_consume_with_callback_function(async_redis_client, capfd, run_parallel):
    global callback_called
    callback_called = False
    assert not callback_called

    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_consume,
        run_parallel=run_parallel,
        awaitable_function_callback=mock_callback,
    )
    browser = Browser(async_redis_client)
    await browser.status()
    await producer.run_job("mock_consume", ["hello"])
    jobs = [job async for job in producer.walk_jobs("mock_consume")]
    assert len(jobs) == 1
    await browser.status()
    await consumer.initialize()
    await browser.status()
    await consumer.run()
    await browser.status()

    out, err = capfd.readouterr()

    assert "hello" in out
    assert callback_called
    await consumer.cleanup()


async def test_count_jobs(async_redis_client, redis_version):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, delay_job)
    browser = Browser(async_redis_client)
    assert await consumer.count_unacked_jobs("delay_job") == 0
    assert await consumer.count_unprocessed_jobs("delay_job") == 0
    assert await consumer.count_undelivered_jobs("delay_job") == 0

    await browser.status()
    await producer.run_job("delay_job", ["hello"])
    assert await consumer.count_unacked_jobs("delay_job") == 0
    assert await consumer.count_undelivered_jobs("delay_job") == 1
    assert await consumer.count_unprocessed_jobs("delay_job") == 1
    await browser.status()
    await consumer.initialize()
    assert await consumer.count_unacked_jobs("delay_job") == 0
    if redis_version == "7":
        assert await consumer.count_undelivered_jobs("delay_job") == 1
        assert await consumer.count_unprocessed_jobs("delay_job") == 1
        assert await consumer.count_stream_added("delay_job") == 1
    else:
        assert await consumer.count_undelivered_jobs("delay_job") == None
        assert await consumer.count_unprocessed_jobs("delay_job") == 0
        assert await consumer.count_stream_added("delay_job") == None
    await browser.status()
    loop = asyncio.get_event_loop()
    loop.create_task(consumer.run())
    await asyncio.sleep(0.1)
    if redis_version == "7":
        assert await consumer.count_undelivered_jobs("delay_job") == 0
    else:
        assert await consumer.count_undelivered_jobs("delay_job") == None
    assert await consumer.count_unacked_jobs("delay_job") == 1
    assert await consumer.count_unprocessed_jobs("delay_job") == 1
    await browser.status()
    await asyncio.sleep(1.1)
    assert await consumer.count_unacked_jobs("delay_job") == 0
    assert await consumer.count_unprocessed_jobs("delay_job") == 0
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


@pytest.mark.parametrize("enable_dead_queue", [True, False])
async def test_delete_dead_jobs(async_redis_client, capfd, enable_dead_queue):
    producer = Producer(async_redis_client)
    consumer = Consumer(
        async_redis_client,
        mock_always_consume_raise_exception,
        expire_time=0.001,
        enable_dead_queue=enable_dead_queue,
    )
    browser = Browser(async_redis_client)
    assert await producer.count_dead_messages("mock_always_consume_raise_exception") == 0

    await browser.status()
    await producer.run_job("mock_always_consume_raise_exception", ["hello"])
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
        assert await producer.count_dead_messages("mock_always_consume_raise_exception") == 1
        dead_messages = await producer.get_dead_messages("mock_always_consume_raise_exception")
        for job in dead_messages.values():
            await producer.remove_dead_message("mock_always_consume_raise_exception", job)
        assert await producer.count_dead_messages("mock_always_consume_raise_exception") == 0
    else:
        assert await producer.count_dead_messages("mock_always_consume_raise_exception") == 0
    await consumer.cleanup()


async def test_deferred_job(async_redis_client, capfd):
    producer = Producer(async_redis_client)
    consumer = Consumer(async_redis_client, mock_consume)
    browser = Browser(async_redis_client)

    await browser.status()
    await producer.run_job("mock_consume", ["hello"], defer_seconds=2)
    await browser.status()
    assert await producer.get_deferred_jobs("mock_consume")
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
