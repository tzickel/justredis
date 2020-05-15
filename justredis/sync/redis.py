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

    def watch(self, *keys):
        pass

    def multi(self):
        pass

    def pubsub(self):
        pass


class PersistentConnection:
    def __init__(self):
        pass

