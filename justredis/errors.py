class RedisError(Exception):
    pass


class CommunicationError(RedisError):
    pass


class ConnectionPoolError(RedisError):
    pass
