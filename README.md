![](https://img.shields.io/github/license/wh1isper/brq)
![](https://img.shields.io/github/v/release/wh1isper/brq)
![](https://img.shields.io/docker/image-size/wh1isper/brq)
![](https://img.shields.io/pypi/dm/brq)
![](https://img.shields.io/github/last-commit/wh1isper/brq)
![](https://img.shields.io/pypi/pyversions/brq)
[![codecov](https://codecov.io/gh/Wh1isper/brq/graph/badge.svg?token=84A7BQZIS2)](https://codecov.io/gh/Wh1isper/brq)

> This project is inspired by [arq](https://github.com/samuelcolvin/arq).
> Not intentionally dividing the community, I desperately needed a redis queue based on redis stream for work reasons and just decided to open source it.
>
> You should also consider [arq](https://github.com/samuelcolvin/arq) as more of a library: https://github.com/samuelcolvin/arq/issues/437

# brq

![Architecture.png](./assets/Architecture.png)

## Prerequisites

Redis >= 6.2, tested with latest redis 6/7 docker image

## Install

`pip install brq`

## Feature

> See [examples](%22./examples%22) for running examples.

- Defer job and automatic retry error job
- Dead queue for unprocessable job, you can process it later
- Multiple consumers in one consumer group
- No scheduler needed, consumer handles itself

## Echo job overview

### Producer

```python
import os

from brq.producer import Producer
from brq.tools import get_redis_client, get_redis_url


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
        await Producer(async_redis_client).run_job("echo", ["hello"])


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

### Consumer

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
