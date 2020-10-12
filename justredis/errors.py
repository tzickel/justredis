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

# Sentinel errors
# TODO (design) should this inherit from CommunicationError?
class NoReplicaFound(Exception):
    pass

class NoSentinelFound(Exception):
    pass

class NoEndpointFound(Exception):
    pass
