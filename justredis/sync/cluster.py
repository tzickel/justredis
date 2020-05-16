# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx
from threading import Lock
from contextlib import contextmanager

from .connectionpool import SyncConnectionPool
from ..decoder import Error


# Cluster hash calculation
def calc_hash(key):
    s = key.find(b'{')
    if s != -1:
        e = key.find(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    return crc_hqx(key, 0) % 16384


# TODO think about multithreading here...
# TODO can I lazaly check if there is a cluster ??
class SyncClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        if addresses is None:
            addresses = (('localhost', 6379))
        self._settings = kwargs
        self._connections = {}
        self._endpoints = []
        self._lock = Lock()
        self._last_connection = None
        self._slots = []
        self._clustered = None
        self._command_cache = {}

    def __del__(self):
        self.close()

    def close(self):
        pass

    # TODO add hints protection
    def _update_slots(self):
        if self._clustered is False:
            return
        conn = self.take()
        try:
            try:
                slots = conn(b'CLUSTER', b'SLOTS')
                self._clustered = True
            except Error:
                slots = []
                self._clustered = False
        finally:
            self.release(conn)
        # TODO check response in RESP2/RESP3
        slots.sort(key=lambda x: x[0])
        slots = [(x[1], (x[2][0].decode(), x[2][1])) for x in slots]
        # TODO what to do if we were in a cluster, but not now ?
        if not self._clustered:
            return
        # TODO remove old non needed pools
        self._slots = slots

    def _connection_by_hashslot(self, hashslot):
        if not self._slots:
            self._update_slots()
        if self._clustered == False:
            return self.take()
        if not self._slots:
            raise Exception('Could not find any slots in redis cluster')
        for slot in self._slots:
            # TODO flip logic ?
            if hashslot > slot[0]:
                continue
            break
        address = slot[1]
        conn = self.take(address)
        return conn

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

    def take_by_key(self, key):
        hash = calc_hash(key)
        return self._connection_by_hashslot(hash)

    def take_by_cmd(self, *cmd):
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
        if self._clustered == False:
            conn = self.take()
        else:
            conn = self.take_by_cmd(*cmd)
        try:
            return conn(*cmd, database=_database)
        finally:
            self.release(conn)

    @contextmanager
    def connection(self, key, _database=0):
        if self._clustered == False:
            conn = self.take()
        else:
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
