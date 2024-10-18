![](https://img.shields.io/github/license/wh1isper/brq)
![](https://img.shields.io/github/v/release/wh1isper/brq)
![](https://img.shields.io/docker/image-size/wh1isper/brq)
![](https://img.shields.io/pypi/dm/brq)
![](https://img.shields.io/github/last-commit/wh1isper/brq)
![](https://img.shields.io/pypi/pyversions/brq)
[![codecov](https://codecov.io/github/b-scheduler/brq/graph/badge.svg?token=84A7BQZIS2)](https://codecov.io/github/b-scheduler/brq)

# brq

`brq` is a lightweight python library for job queue based on the redis stream, with no central server and self-organized by `Consumer`.

![Architecture.png](./assets/Architecture.png)

## Prerequisites

Redis >= 6.2, tested with latest redis 6/7 docker image. Recommended to use redis>=7, which includes more inspection features.

## Install

`pip install brq`

## Feature

> See [examples](./examples) for running examples.

- Defer job and automatic retry error job
- Dead queue for unprocessable job, you can process it later
- Multiple consumers in one consumer group
- No scheduler needed, consumer handles itself
- Using callback function to process job result or exception

## Configuration

If using `BrqConfig`(for example, `@task`), you can use a `.env` file and environment variables to configure brq. The prefix of environment variables is `BRQ_`.

> For example, `BRQ_REDIS_PORT=6379 python consumer.py` for specifying redis port.

See [configs](./brq/configs.py) for more details.

## Echo job overview

### Producer

```python
import os

from brq.producer import Producer
from brq.configs import BrqConfig


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
```

### Consumer

The only thing you need is `@task`, and the target function can be `sync` or `async` and `sync` function will be converted to `async` function and run in a thread automatically.

```python
from brq import task


@task
def echo(message):
    print(f"Received message: {message}")


if __name__ == "__main__":
    # Run the task once, for local debug
    # echo("hello")

    # Run as a daemon
    echo.serve()
```

This is the same as the following, the classic way...But more flexible.

```python
import os

from brq.consumer import Consumer
from brq.daemon import Daemon
from brq.tools import get_redis_client, get_redis_url


async def echo(message):
    print(message)


async def main():
    redis_url = get_redis_url(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        cluster=bool(os.getenv("REDIS_CLUSTER", "false") in ["True", "true", "1"]),
        tls=bool(os.getenv("REDIS_TLS", "false") in ["True", "true", "1"]),
        username=os.getenv("REDIS_USERNAME", ""),
        password=os.getenv("REDIS_PASSWORD", ""),
    )
    async with get_redis_client(redis_url) as async_redis_client:
        daemon = Daemon(Consumer(async_redis_client, echo))
        await daemon.run_forever()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

## Technical details: deferred jobs

We can use `defer_until` as a `datetime` or `defer_hours`+`defer_minutes`+`defer_seconds` to calculate a timestamp based on current redis timestamp. And use `unique` to set the job to be unique or not.

By default, `unique=True` means `Job` with the **exactly** same `function_name`, `args` and `kwargs` will be unique, which allows the same `Job` to add into the deferred queue more than once. In this case, we differentiate tasks by the current redis timestamp(`Job.create_at`) and an additional uuid(`Job.uid`), just like `redis stream` did.

If `unique=False`, the same `Job` will be added into the deferred queue only once. Duplicates will update the job's defer time. In this case, you can use your own uuid in `args`(or `kwargs`) to differentiate `Job`.

## Develop

Install pre-commit before commit

```
pip install pre-commit
pre-commit install
```

Install package locally

```
pip install -e .[test]
```

Run unit-test before PR

```
pytest -v
```
