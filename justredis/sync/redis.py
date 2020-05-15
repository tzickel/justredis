from .connectionpool import SyncConnectionPool


class SyncRedis:
    def __init__(self, **kwargs):
        self._connection_pool = SyncConnectionPool(**kwargs)

    def __del__(self):
        self.close()

    def close(self):
        self._connection_pool.close()

    def __call__(self, *cmd, _database=0, _encoder=None, _decoder=None):
        conn = self._connection_pool.take()
        try:
            return conn(*cmd)
        finally:
            self._connection_pool.release(conn)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def multi(self):
        return SyncMultiCommand(self)

    def watch(self, *keys):
        pass

    def pubsub(self):
        pass

    def monitor(self):
        return SyncMonitor(self)


class SyncMultiCommand:
    def __init__(self, redis):
        self._redis = redis

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

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


class SyncMonitor(SyncPersistentConnection):
    def _connect(self):
        self._conn.push_command('MONITOR')

    def _disconnect(self):
        if self._conn:
            # We can't recover a MONITOR connection?
            self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next_message()
