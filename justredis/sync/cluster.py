from threading import Lock
from contextlib import contextmanager

from .connectionpool import SyncConnectionPool
from ..decoder import Error



# TODO think about multithreading here...
class SyncClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        if addresses is None:
            addresses = (('localhost', 6379))
        self._settings = kwargs
        self._connections = {}
        self._endpoints = []
        self._lock = Lock()
        self._last_connection = None

    def __del__(self):
        self.close()

    def close(self):
        pass

    # TODO we need to handle when last_connection points to a member of the pool that isn't valid anymore..
    def take(self, address=None):
        #if not address and self._last_connection and self._last_connection.
        if address:
            pool = self._connections.get(address)
            if pool is None:
                with self._lock:
                    pool = self._connections.get(address)
                    if pool is None:
                        pool = SyncConnectionPool(address=address, **self._settings)
                        self._connections[address] = pool
            return pool.take()
        else:
            raise NotImplementedError()

    def release(self, conn):
        address = conn.peername()
        pool = self._connections.get(address)
        # The connection might have been discarded.
        if pool is None:
            conn.close()
            return
        pool.release(conn)

    def __call__(self, *cmd, _database=0):
        conn = self.take_by_cmd(*cmd)
        try:
            return conn(*cmd, database=_database)
        finally:
            self.release(conn)

    @contextmanager
    def connection(self, key, _database=0):
        conn = self.take_by_key(key)
        try:
            conn.set_database(_database)
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                conn._command(b'DISCARD')
            except Error:
                pass
            self.release(conn)
