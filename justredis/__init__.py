from .sync.redis import Redis
from .decoder import Error

# TODO (misc) keep this in sync
from .errors import *


try:
    from .nonsync.redis import Redis as AsyncRedis
except ImportError:

    class AsyncRedis:
        def __init__(self, *args, **kwargs):
            raise Exception("Using JustRedis asynchronously requires the AnyIO library to be installed.")


except SyntaxError:

    class AsyncRedis:
        def __init__(self, *args, **kwargs):
            raise Exception("Using JustRedis asynchronously requires Python 3.6 or above.")


except AttributeError as e:

    class AsyncRedis:
        def __init__(self, e=e, *args, **kwargs):
            raise Exception(e.args[0])


__all__ = "AsyncRedis", "Redis", "RedisError", "CommunicationError", "ConnectionPoolError", "ProtocolError", "PipelinedExceptions", "Error"
