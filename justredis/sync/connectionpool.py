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
    # TODO (api) add a force option ?
    def close(self):
        # We do this first, so if another thread calls release it won't get back to the pool
        self._connections_in_use.clear()
        for connection in self._connections_available:
            connection.close()
        self._connections_available.clear()
        self._limit = Semaphore(self._max_connections) if self._max_connections else None

    def take(self):
        # TODO (correctness) cluster depends on this failing if closed !
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
        # TODO (correctness) should we release the self._limit here as well ? (or just make close forever)
        # If this fails, it's a connection from a previous cycle, don't reuse it
        except KeyError:
            conn.close()
            return
        if not conn.closed():
            self._connections_available.append(conn)
        elif self._limit is not None:
            self._limit.release()

    def __call__(self, *cmd, **kwargs):
        conn = self.take()
        try:
            return conn(*cmd, **kwargs)
        finally:
            self.release(conn)

    @contextmanager
    def connection(self, **kwargs):
        conn = self.take()
        try:
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                conn._command(b'DISCARD')
            except Exception:
                pass
            self.release(conn)

    def endpoints(self):
        conn = self.take()
        try:
            return [(conn.peername(), ['regular'])]
        finally:
            self.release(conn)
