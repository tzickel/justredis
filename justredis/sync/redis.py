from .connectionpool import SyncConnectionPool
from ..utils import parse_url


class SyncRedis:
    @classmethod
    def from_url(cls, url, **kwargs):
        kwargs.update(parse_url(url))
        return cls(**kwargs)

    # TODO docstring the kwargs
    def __init__(self, database=0, **kwargs):
        self._database = database
        self._connection_pool = SyncConnectionPool(**kwargs)

    def __del__(self):
        self.close()

    def close(self):
        self._connection_pool.close()

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

    def multi(self):
        return SyncMultiCommand(self, database=self._database)

    def watch(self, *keys):
        raise NotImplementedError()

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

    def __call__(self, *cmd):
        return self._redis(*cmd, _database=self._database)

    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

    def multi(self):
        return SyncMultiCommand(self, self._database)

    def watch(self, *keys):
        raise NotImplementedError()


class SyncMultiCommand:
    def __init__(self, redis, database=0):
        self._redis = redis
        self._database = database
        self._result = None

    def __del__(self):
        self.close()
    
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self):
        pass


class SyncPersistentConnection:
    def __init__(self, redis, retries=3):
        self._redis = redis
        self._retries = retries
        self._conn = None

    def __del__(self):
        self.close()
    
    def close(self):
        self._disconnect()
        if self._conn:
            self._redis._connection_pool.release(self._conn)

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

    # TODO retries
    def _check_connection(self):
        conn = self._conn
        if conn == None:
            self._conn = self._redis._connection_pool.take()
        elif conn.closed():
            self._redis._connection_pool.release(conn)
            self._conn = self._redis._connection_pool.take()
        else:
            return
        # TODO this can throw
        self._connect()

    def next_message(self, timeout=False):
        self._check_connection()
        return self._conn.pushed_message(timeout)


class SyncPubSub(SyncPersistentConnection):
    def __init__(self, *args, **kwargs):
        super(SyncPubSub, self).__init__(*args, **kwargs)

    def subscribe(self, channel):
        self._check_connection()
        self._conn.push_command(b'SUBSCRIBE', channel)


class SyncMonitor(SyncPersistentConnection):
    def _connect(self):
        self._conn.push_command(b'MONITOR')

    def _disconnect(self):
        if self._conn:
            # We can't recover a MONITOR connection?
            self._conn.close()
