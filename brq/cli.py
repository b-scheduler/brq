import asyncio
from functools import wraps

import click

from brq.browser import Browser
from brq.tools import get_redis_client, get_redis_url

from .envs import *


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.command()
@click.option(
    "--redis-host",
    default="localhost",
    help="Redis host",
    type=str,
    envvar=REDIS_HOST_ENV,
)
@click.option(
    "--redis-port",
    default=6379,
    help="Redis port",
    type=int,
    envvar=REDIS_PORT_ENV,
)
@click.option(
    "--redis-db",
    default=0,
    help="Redis db",
    type=int,
    envvar=REDIS_DB_ENV,
)
@click.option(
    "--redis-cluster",
    default=False,
    help="Redis cluster",
    type=bool,
    envvar=REDIS_CLUSTER_ENV,
)
@click.option(
    "--redis-tls",
    default=False,
    help="Redis TLS",
    type=bool,
    envvar=REDIS_TLS_ENV,
)
@click.option(
    "--redis-username",
    default="",
    help="Redis username",
    type=str,
    envvar=REDIS_USERNAME_ENV,
)
@click.option(
    "--redis-password",
    default="",
    help="Redis password",
    type=str,
    envvar=REDIS_PASSWORD_ENV,
)
@click.option("--redis-url", required=False, help="Redis URL", type=str, envvar=REDIS_URL_ENV)
@click.option(
    "--redis-prefix",
    required=False,
    help="Redis prefix",
    type=str,
    envvar=REDIS_PREFIX_ENV,
)
@click.option(
    "--redis-seperator",
    required=False,
    help="Redis seperator",
    type=str,
    envvar=REDIS_SEPERATOR_ENV,
)
@click.option(
    "--group-name",
    required=False,
    help="Group name",
    type=str,
    envvar=CONSUMER_GROUP_NAME_ENV,
)
@coro
async def browser(
    redis_host,
    redis_port,
    redis_db,
    redis_cluster,
    redis_tls,
    redis_username,
    redis_password,
    redis_url,
    redis_prefix,
    redis_seperator,
    group_name,
):
    redis_url = redis_url or get_redis_url(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        cluster=redis_cluster,
        tls=redis_tls,
        username=redis_username,
        password=redis_password,
    )
    async with get_redis_client(redis_url, is_cluster=redis_cluster) as redis_client:
        await Browser(
            redis_client,
            redis_prefix=redis_prefix,
            redis_seperator=redis_seperator,
            group_name=group_name,
        ).status()


@click.group()
def cli():
    pass


cli.add_command(browser)
