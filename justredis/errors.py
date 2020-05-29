class RedisError(Exception):
    pass


class CommunicationError(RedisError):
    pass


class ConnectionPoolError(RedisError):
    pass


class ProtocolError(RedisError):
    pass


class PipelinedExceptions(RedisError):
    pass
