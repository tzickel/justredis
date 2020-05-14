class RedisError(Exception):
    pass


class CommunicationError(RedisError):
    pass
