from .connectionpool import SyncConnectionPool
from .cluster import SyncClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url


def merge_dicts(parent, child):
    if not parent and not child:
        return None
    elif not parent:
        return child
    elif not child:
        return parent
    tmp = parent.copy()
    tmp.update(child)
    return tmp


# We do this seperation to allow changing per command and connection settings easily
class ModifiedRedis:
    def __init__(self, connection_pool, **kwargs):
        self._connection_pool = connection_pool
        self._settings = kwargs

    def __del__(self):
        self.close()

    def close(self):
        self._connection_pool = self._settings = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection_pool(*cmd)
        else:
            return self._connection_pool(*cmd, **settings)

    def connection(self, *cmds, push=False, **kwargs):
        if cmds:
            raise Exception('Please specify the key')
        wrapper = PushConnection if push else Connection
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            conn = self._connection_pool.connection()
            return wrapper(conn)
        else:
            conn = self._connection_pool.connection(**settings)
            return wrapper(conn, **settings)

    def endpoints(self):
        return self._connection_pool.endpoints()

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return ModifiedRedis(self._connection_pool, **settings)


# TODO get callback when slots have changes (maybe listen to other connections?) (or invalidate open connections)
# TODO allow MRO registration for spealized commands !
class SyncRedis(ModifiedRedis):
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
        super(SyncRedis, self).__init__(pool_factory(**kwargs), **kwargs)

    def __del__(self):
        self.close()

    def close(self):
        if self._connection_pool:
            self._connection_pool.close()
        self._connection_pool = None


class ModifiedConnection:
    def __init__(self, connection, **kwargs):
        self._connection = connection
        self._settings = kwargs

    def __del__(self):
        self.close()

    def close(self):
        self._connection = self._settings = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection(*cmd)
        else:
            return self._connection(*cmd, **settings)

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return ModifiedConnection(self._connection, **settings)


class Connection(ModifiedConnection):
    def __init__(self, connection, **kwargs):
        self._connection_context = connection
        super(Connection, self).__init__(connection.__enter__(), **kwargs)

    def __del__(self):
        self.close()

    def close(self):
        if self._connection_context:
            self._connection_context.__exit__(None, None, None)
        self._connection = None
        self._connection_context = None
        self._settings = None


class PushConnection(Connection):
    def __call__(self, *cmd, **kwargs):
        # TODO handle **kwargs and merge dict
        return self._connection.push_command(*cmd)

    def next_message(self, timeout=None, **kwargs):
        # TODO handle **kwargs and merge dict
        return self._connection.pushed_message(timeout=timeout, **kwargs)

    def __iter__(self):
        return self

    def __next__(self):
        return self._connection.pushed_message()
