from collections import deque
from contextlib import contextmanager
from threading import Semaphore


from .connection import SyncConnection
from ..errors import ConnectionPoolError
from ..decoder import Error


class SyncConnectionPool:
    def __init__(self, max_connections=None, wait_timeout=None, **connection_settings):
        self._max_connections = max_connections
        self._wait_timeout = wait_timeout
        self._connection_settings = connection_settings

        self._limit = Semaphore(max_connections) if max_connections else None
        self._connections_available = deque()
        self._connections_in_use = set()

    def __del__(self):
        self.close()
    
    # This won't close connections which are in use currently
    # TODO add a force option ?
    def close(self):
        for connection in self._connections_available:
            connection.close()
        self._connections_available.clear()
        self._connections_in_use.clear()
        self._limit = Semaphore(self._max_connections) if self._max_connections else None

    # TODO if address is set, throw an exception ?
    def take(self, address=None):
        try:
            while True:
                conn = self._connections_available.popleft()
                if not conn.closed():
                    break
                if self._limit is not None:
                    self._limit.release()
        except IndexError:
            if self._limit is not None and not self._limit.acquire(True, self._wait_timeout):
                raise ConnectionPoolError('Could not acquire an connection form the pool')
            try:
                conn = SyncConnection(**self._connection_settings)
            except Exception:
                if self._limit is not None:
                    self._limit.release()
                raise
        self._connections_in_use.add(conn)
        return conn

    def release(self, conn):
        try:
            self._connections_in_use.remove(conn)
        # If this fails, it's a connection from a previous cycle, don't reuse it
        except KeyError:
            conn.close()
            return
        if not conn.closed():
            self._connections_available.append(conn)
        elif self._limit is not None:
            self._limit.release()

    def __call__(self, *cmd, _database=0):
        conn = self.take()
        try:
            return conn(*cmd, database=_database)
        finally:
            self.release(conn)

    @contextmanager
    def connection(self, key, _database=0):
        conn = self.take()
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
