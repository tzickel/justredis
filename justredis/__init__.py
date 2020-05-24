from .sync.redis import SyncRedis
from .decoder import Error

# TODO keep this in sync
from .errors import *

Redis = SyncRedis

__all__ = "SyncRedis", "RedisError", "CommunicationError", "ConnectionPoolError", "Error", "Redis"
