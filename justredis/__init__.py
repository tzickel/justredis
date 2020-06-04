from .sync.redis import Redis
from .decoder import Error
# TODO (misc) keep this in sync
from .errors import *


__all__ = ["Redis", "RedisError", "CommunicationError", "ConnectionPoolError", "ProtocolError", "PipelinedExceptions", "Error"]


try:
    from .nonsync.redis import Redis as AsyncRedis
    __all__.append("AsyncRedis")
except ImportError:
    # TODO (misc) emit a warning ?
    pass
