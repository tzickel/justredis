from .connectionpool import SyncConnectionPool
from .cluster import SyncClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url


class SyncRedis:
    @classmethod
    def from_url(cls, url, **kwargs):
        res = parse_url(url)
        res.update(kwargs)
        return cls(**res)

    def __init__(self, database=0, pool_factory=SyncClusterConnectionPool, **kwargs):
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
        self._database = database
        if pool_factory == 'pool':
            pool_factory = SyncConnectionPool
        elif pool_factory == 'auto':
            pool_factory = SyncClusterConnectionPool
        self._connection_pool = pool_factory(**kwargs)

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

    def __call__(self, *cmd, _database=None):
        if _database is None:
            _database = self._database
        return self._connection_pool(*cmd, _database=_database)

    # Do not use this connection for push commands (monitor / pubsub/ etc...)
    def connection(self, key, _database=None):
        if _database is None:
            _database = self._database
        return self._connection_pool.connection(key, _database)

    def pubsub(self):
        return SyncPubSub(self)

    def monitor(self):
        return SyncMonitor(self)

    def database(self, database):
        return SyncDatabase(self, database)


class SyncDatabase:
    def __init__(self, redis, database):
        self._redis = redis
        self._database = database

    def __del__(self):
        self.close()
    
    def close(self):
        self._redis = None

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd):
        return self._redis(*cmd, _database=self._database)

    def connection(self, key):
        return self._redis.connection(key, _database=self._database)


class SyncPersistentConnection:
    def __init__(self, redis, address=None):
        self._redis = redis
        self._address = address
        self._conn = None

    def __del__(self):
        self.close()
    
    def close(self):
        if self._conn:
            self._disconnect()
            self._redis._connection_pool.release(self._conn)
            self._conn = None
        self._redis = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next_message()

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _check_next_message(self):
        pass

    # returns False when a new connection was created
    def _check_connection(self):
        conn = self._conn
        if conn is None:
            self._conn = self._redis._connection_pool.take(self._address)
        elif conn.closed():
            self._redis._connection_pool.release(conn)
            self._conn = self._redis._connection_pool.take(self._address)
        else:
            return True
        self._connect()
        return False

    # We don't hide here a connection failure, because the client should be aware that a temporary disconnection has happened
    # TODO (api) maybe notify connection failure here, as an None result, or non exceptional ?
    def next_message(self, timeout=False, decoder=None):
        self._check_connection()
        self._check_next_message()
        return self._conn.pushed_message(timeout, decoder)


# TODO (api) should I provide optional different encoding here for channel/pattern names ?
# If a disconnection happens, you can recall the command to continue trying
class SyncPubSub(SyncPersistentConnection):
    def __init__(self, *args, **kwargs):
        self._channels = set()
        self._patterns = set()
        super(SyncPubSub, self).__init__(*args, **kwargs)

    def _connect(self):
        if self._channels:
            self._conn.push_command(b'SUBSCRIBE', *self._channels)
        if self._patterns:
            self._conn.push_command(b'PSUBSCRIBE', *self._patterns)

    def _disconnect(self):
        if self._conn:
            # We could do something smarter here, and unsubscribe from everything, but is this an often done command ?
            self._conn.close()

    def _check_next_message(self):
        if not self._channels and not self._patterns:
            raise Exception('Not listening to anything')

    def subscribe(self, *channels):
        self._channels.update(channels)
        if self._check_connection():
            self._conn.push_command(b'SUBSCRIBE', *channels)

    def psubscribe(self, *patterns):
        self._patterns.update(patterns)
        if self._check_connection():
            self._conn.push_command(b'PSUBSCRIBE', *patterns)

    def unsubscribe(self, *channels):
        channels_to_remove = self._channels & set(channels)
        self._channels -= channels_to_remove
        if self._check_connection():
            self._conn.push_command(b'UNSUBSCRIBE', *channels_to_remove)

    def punsubscribe(self, *patterns):
        patterns_to_remove = self._patterns & set(patterns)
        self._patterns -= patterns_to_remove
        if self._check_connection():
            self._conn.push_command(b'PUNSUBSCRIBE', *patterns_to_remove)


class SyncMonitor(SyncPersistentConnection):
    def _connect(self):
        self._conn.push_command(b'MONITOR')

    def _disconnect(self):
        if self._conn:
            # We can't recover a MONITOR connection, so close the connection
            self._conn.close()
