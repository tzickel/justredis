from .connection import SocketWrapper
from .connectionpool import ConnectionPool


class Redis:
    def __init__(self, address=None, max_connections=None, wait_timeout=None, timeout=None, username=None, password=None, client_name=None, resp_version=-1, socket_factory=SocketWrapper, connection_pool=ConnectionPool, **kwargs):
        kwargs.update({
            'address': address,
            'max_connections': max_connections,
            'wait_timeout': wait_timeout,
            'timeout': timeout,
            'username': username,
            'password': password,
            'client_name': client_name,
            'resp_version': resp_version,
            'socket_factory': socket_factory,
        })
        self._connection_pool = connection_pool(**kwargs)

    def __call__(self, *cmd):
        conn = self._connection_pool.take()
        try:
            return conn(*cmd)
        finally:
            self._connection_pool.release(conn)

