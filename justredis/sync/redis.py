from inspect import isclass


from .connectionpool import SyncConnectionPool
from .cluster import SyncClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url


# TODO get callback when slots have changes (maybe listen to other connections?) (or invalidate open connections)
# TODO recursive modifiers ?
# TODO why do we need this class ? to save state maybe (for recrusive modifiers?)
# TODO allow MRO registration for spealized commands !
class SyncRedis:
    @classmethod
    def from_url(cls, url, **kwargs):
        res = parse_url(url)
        res.update(kwargs)
        return cls(**res)

    def __init__(self, pool_factory=SyncClusterConnectionPool, **kwargs):
        # TODO docstring the kwargs
        """
            Possible arguments:
            database (0): The default redis database number (SELECT) for this instance
            pool_factory ('auto'): 

            decoder (bytes): By default strings are kept as bytes, 'unicode'
            encoder
            username
            password
            client_name
            resp_version
            socket_factory
            connect_retry
            buffersize
            For any pool:

            addresses

            # For all connection pools
            max_connections
            wait_timeout
            
            # For all sockets
            address
            connect_timeout
            socket_timeout

            # For TCP based sockets
            tcp_keepalive
            tcp_nodelay
        """
        if pool_factory == 'pool':
            pool_factory = SyncConnectionPool
        elif pool_factory == 'auto':
            pool_factory = SyncClusterConnectionPool
        if isclass(pool_factory):
            self._connection_pool = pool_factory(**kwargs)
        else:
            self._connection_pool = pool_factory
        self._settings = kwargs

    def __del__(self):
        self.close()

    def close(self):
        if self._connection_pool:
            self._connection_pool.close()
        self._connection_pool = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd):
        return self._connection_pool(*cmd, **self._settings)

    def connection(self, push=False):
        wrapper = PushConnection if push else Connection
        return wrapper(self._connection_pool, **self._settings)

    def endpoints(self):
        return self._connection_pool.endpoints()

    def multiple_endpoints(self, *cmd, endpoints=True):
        return self._connection_pool.multiple_endpoints(*cmd, endpoints=endpoints, **self._settings)

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return SyncRedis(self._connection_pool, **settings)


class Connection:
    def __init__(self, pool, **kwargs):
        self._pool = pool
        self._conn = pool.connection(**kwargs)

    def __del__(self):
        self.close()

    def close(self):
        self._conn.release(self._conn)
        self._conn = None
        self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd):
        return self._conn(*cmd)


class PushConnection:
    def __init__(self, pool, **kwargs):
        self._pool = pool
        self._conn = pool.connection(**kwargs)

    def __del__(self):
        self.close()

    def close(self):
        # no good way in redis API to reset state of a connection
        self._conn.close()
        self._conn.release(self._conn)
        self._conn = None
        self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd, **kwargs):
        return self._conn.push_command(*cmd)

    def next_message(self, timeout=None, **kwargs):
        return self._conn.next_message(timeout=timeout, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        return self._conn.next_message()
