from contextlib import contextmanager

from .connectionpool import SyncConnectionPool
from ..decoder import Error
from ..utils import parse_url


class SyncRedis:
    @classmethod
    def from_url(cls, url, **kwargs):
        kwargs.update(parse_url(url))
        return cls(**kwargs)

    def __init__(self, database=0, **kwargs):
        # TODO docstring the kwargs
        """
            Possible arguments:
            database (0): The default database number for this instance
            decoder (bytes): By default strings are kept as bytes, 'unicode'
            encoder
            username
            password
            client_name
            resp_version
            socket_factory
            connect_retry
            buffersize
            max_connections
            wait_timeout
            address
            connect_timeout
            socket_timeout
            tcp_keepalive
            tcp_nodelay
        """
        self._database = database
        self._connection_pool = SyncConnectionPool(**kwargs)

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
        conn = self._connection_pool.take()
        try:
            if _database is None:
                _database = self._database
            return conn(*cmd, database=_database)
        finally:
            self._connection_pool.release(conn)

    # TODO give an optional key later for specific server
    # TODO disable and document not to use this for monitor / pubsub / other push commands
    @contextmanager
    def connection(self, _database=None):
        conn = self._connection_pool.take()
        try:
            if _database is None:
                _database = self._database
            conn.set_database(_database)
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                conn._command(b'DISCARD')
            except Error:
                pass
            self._connection_pool.release(conn)

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

    def connection(self):
        return self._redis.connection(_database=self._database)


class SyncPersistentConnection:
    def __init__(self, redis):
        self._redis = redis
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

    def _check_connection(self):
        conn = self._conn
        if conn == None:
            self._conn = self._redis._connection_pool.take()
        elif conn.closed():
            self._redis._connection_pool.release(conn)
            self._conn = self._redis._connection_pool.take()
        else:
            return True
        # TODO this can throw, tough luck ?
        self._connect()
        return False

    def next_message(self, timeout=False):
        self._check_connection()
        try:
            return self._conn.pushed_message(timeout)
        # TODO something better here ?
        except ConnectionError:
            self._check_connection()
            return self._conn.pushed_message(timeout)


# TODO think better about disconnect in the middle senario
# TODO encoding / decoding here ?
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

    # We could do something smarter here, and unsubscribe from everything, but is this an often done command ?
    def _disconnect(self):
        if self._conn:
            self._conn.close()

    def subscribe(self, *channels):
        self._channels.update(channels)
        if not self._check_connection():
            self._conn.push_command(b'SUBSCRIBE', *channels)

    def psubscribe(self, *patterns):
        self._patterns.update(patterns)
        if not self._check_connection():
            self._conn.push_command(b'PSUBSCRIBE', *patterns)

    def unsubscribe(self, *channels):
        raise NotImplementedError()

    def punsubscribe(self, *patterns):
        raise NotImplementedError()


class SyncMonitor(SyncPersistentConnection):
    def _connect(self):
        self._conn.push_command(b'MONITOR')

    def _disconnect(self):
        if self._conn:
            # We can't recover a MONITOR connection?
            self._conn.close()
