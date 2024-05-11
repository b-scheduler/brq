![](https://img.shields.io/github/license/wh1isper/brq)
![](https://img.shields.io/github/v/release/wh1isper/brq)
![](https://img.shields.io/pypi/dm/brq)
![](https://img.shields.io/github/last-commit/wh1isper/brq)
![](https://img.shields.io/pypi/pyversions/brq)

> This project is inspired by [arq](https://github.com/samuelcolvin/arq).
> Not intentionally dividing the community, I desperately needed a redis queue based on redis stream for work reasons and just decided to open source it.
>
> You should also consider [arq](https://github.com/samuelcolvin/arq) as more of a library: https://github.com/samuelcolvin/arq/issues/437

# brq

![Architecture.png](./assets/Architecture.png)

## Prerequisites

Redis >= 6.2, tested with latest docker image

## Install

`pip install brq`

## Usage

TBD

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
