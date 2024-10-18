"""Top-level package for brq."""

__author__ = "wh1isper"
__email__ = "jizhongsheng957@gmail.com"
__version__ = "0.5.1.dev0"


from .configs import BrqConfig
from .consumer import Consumer
from .decorator import task
from .producer import Producer
from .tools import get_redis_client, get_redis_url

__all__ = [
    "task",
    "Consumer",
    "Producer",
    "get_redis_client",
    "get_redis_url",
    "BrqConfig",
]
