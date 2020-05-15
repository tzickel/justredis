from .connectionpool import ConnectionPool


class Redis:
    def __init__(self, connection_pool=ConnectionPool, **kwargs):
        self._connection_pool = connection_pool(**kwargs)

#    def __del__(self):
#        self.close()

    def close(self):
        if self._connection_pool is not None:
            self._connection_pool.close()
        self._connection_pool = None

    def __call__(self, *cmd):
        conn = self._connection_pool.take()
        try:
            return conn(*cmd)
        finally:
            self._connection_pool.release(conn)
