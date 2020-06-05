from .sync.redis import Redis
from .decoder import Error
# TODO (misc) keep this in sync
from .errors import *


try:
    from .nonsync.redis import Redis as AsyncRedis
except ImportError:
    class AsyncRedis:
        def __init__(self, *args, **kwargs):
            raise Exception("Using JustRedis asynchronously requires the anyio library to be installed")
except SyntaxError:
    class AsyncRedis:
        def __init__(self, *args, **kwargs):
            raise Exception("Your python version is too old, and is not supported for async I/O using JustRedis")


__all__ = "AsyncRedis", "Redis", "RedisError", "CommunicationError", "ConnectionPoolError", "ProtocolError", "PipelinedExceptions", "Error"
