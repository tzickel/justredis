from collections import deque

try:
    from contextlib import asynccontextmanager
except:
    from async_generator import asynccontextmanager


from .connection import Connection
from ..errors import ConnectionPoolError
from ..decoder import Error
from .environment import get_environment


# TODO (misc) can we relax the _lock ?


class ConnectionPool:
    def __init__(self, max_connections=None, wait_timeout=None, **kwargs):
        self._max_connections = max_connections
        self._wait_timeout = wait_timeout
        self._connection_settings = kwargs

        self._lock = get_environment(**kwargs).lock()
        self._shield = get_environment(**kwargs).shield
        self._limit = get_environment(**kwargs).semaphore(max_connections) if max_connections else None
        self._connections_available = deque()
        self._connections_in_use = set()
        self._closed = False

    async def aclose(self):
        async with self._lock:
            if not self._closed:
                # We do this first, so if another thread calls release it won't get back to the pool
                for connection in self._connections_available:
                    await connection.aclose()
                for connection in self._connections_in_use:
                    await connection.aclose()
                self._connections_available.clear()
                self._connections_in_use.clear()
                self._limit = get_environment(**self.connection_settings).semaphore(self._max_connections) if self._max_connections else None
            self._closed = True

    async def take(self):
        if self._closed:
            raise ConnectionPoolError("Pool already closed")
        # TODO (correctness) cluster depends on this failing if closed !
        try:
            while True:
                conn = self._connections_available.popleft()
                if not conn.closed():
                    break
                if self._limit is not None:
                    self._limit.release()
        except IndexError:
            if self._limit is not None and not await self._limit.acquire(self._wait_timeout):
                raise ConnectionPoolError("Could not acquire an connection form the pool")
            try:
                conn = await Connection.create(**self._connection_settings)
            except Exception:
                if self._limit is not None:
                    self._limit.release()
                raise
        self._connections_in_use.add(conn)
        return conn

    async def release(self, conn):
        with self._shield():
            async with self._lock:
                try:
                    self._connections_in_use.remove(conn)
                # TODO (correctness) should we release the self._limit here as well ? (or just make close forever)
                # If this fails, it's a connection from a previous cycle, don't reuse it
                except KeyError:
                    await conn.aclose()
                    return
                if not conn.closed():
                    self._connections_available.append(conn)
                elif self._limit is not None:
                    self._limit.release()

    async def __call__(self, *cmd, **kwargs):
        if not cmd:
            raise ValueError("No command provided")
        conn = await self.take()
        try:
            return await conn(*cmd, **kwargs)
        finally:
            await self.release(conn)

    @asynccontextmanager
    async def connection(self, **kwargs):
        conn = await self.take()
        try:
            conn.allow_multi(True)
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                await conn._command(b"DISCARD")
            except Exception:
                pass
            conn.allow_multi(False)
            await self.release(conn)

    async def endpoints(self):
        conn = await self.take()
        try:
            return [(conn.peername(), {"type": "regular"})]
        finally:
            await self.release(conn)
