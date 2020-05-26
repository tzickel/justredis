from .connectionpool import SyncConnectionPool
from .cluster import SyncClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url, merge_dicts


# TODO (misc) document all the kwargs everywhere
# TODO (api) internal remove from connectionpool the __enter__/__exit__ and use take(**kwargs)/release


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

    def connection(self, *args, push=False, **kwargs):
        if args:
            raise ValueError("Please specify the connection arguments as named arguments (i.e. push=..., key=...)")
        wrapper = PushConnection if push else Connection
        settings = merge_dicts(self._settings, kwargs)
        # TODO (api) should we put the **settings here too ?
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


# TODO (api) should we implement an callback for when slots have changed ?
# TODO (api) allow some registration method for speacialized commands ?
class Redis(ModifiedRedis):
    @classmethod
    def from_url(cls, url, **kwargs):
        res = parse_url(url)
        res.update(kwargs)
        return cls(**res)

    def __init__(self, pool_factory=SyncClusterConnectionPool, **kwargs):
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
        if pool_factory == "pool":
            pool_factory = SyncConnectionPool
        elif pool_factory == "auto":
            pool_factory = SyncClusterConnectionPool
        # TODO (api) should we put the **settings here too ?
        super(Redis, self).__init__(pool_factory(**kwargs))

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
    def close(self):
        # We close the connection here, since it's both hard to reset the state of the connection, and this is usually not done / at low frequency.
        self._connection.close()
        super(PushConnection, self).close()

    def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection.push_command(*cmd)
        else:
            return self._connection.push_command(*cmd, **settings)

    def next_message(self, *args, timeout=None, **kwargs):
        if args:
            raise ValueError("Please specify the next_message arguments as named arguments (i.e. timeout=...)")
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection.pushed_message(timeout=timeout)
        else:
            return self._connection.pushed_message(timeout=timeout, **settings)

    def __iter__(self):
        return self

    def __next__(self):
        return self._connection.pushed_message()
