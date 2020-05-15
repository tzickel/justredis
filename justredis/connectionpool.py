from collections import deque
from threading import Semaphore


from .connection import Connection


class ConnectionPool:
    def __init__(self, max_connections=None, wait_timeout=None, **connection_settings):
        self._connection_settings = connection_settings
        self._max_connections = max_connections
        self._wait_timeout = wait_timeout

        self._limit = Semaphore(max_connections) if max_connections else None
        self._connections_available = deque()
        self._connections_in_use = set()

    def __del__(self):
        self.close()
    
    def close(self):
        for connection in self._connections_available:
            connection.close()
        self._connection_settings = self._max_connections = self._wait_timeout = self._limit = self._connections_available = self._connections_in_use = None
        
    def take(self):
        if self._connections_available is None:
            raise Exception('ConnectionPool already closed')
        try:
            while True:
                conn = self._connections_available.popleft()
                if not conn.closed():
                    break
                if self._limit is not None:
                    self._limit.release()
        except IndexError:
            if self._limit is not None and not self._limit.acquire(True, self._wait_timeout):
                raise Exception('Could not aquire an connection form the pool')
            try:
                conn = Connection(**self._connection_settings)
            except Exception:
                if self._limit is not None:
                    self._limit.release()
                raise
        self._connections_in_use.add(conn)
        return conn

    def release(self, conn):
        # If we are closed, close this pending connection
        if self._connections_available is None:
            conn.close()
            return
        self._connections_in_use.remove(conn)
        if not conn.closed():
            self._connections_available.append(conn)
        elif self._limit is not None:
            self._limit.release()
