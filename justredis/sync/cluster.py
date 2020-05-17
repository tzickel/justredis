# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx
from threading import Lock
from contextlib import contextmanager

from .connectionpool import SyncConnectionPool
from ..decoder import Error


def calc_hashslot(key):
    s = key.find(b'{')
    if s != -1:
        e = key.find(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    return crc_hqx(key, 0) % 16384


# TODO (misc) I think I covered the multithreading sensetive parts, make sure
# TODO (misc) should I lazely check if there is a cluster ?
# TODO (misc) add ASKING


# TODO recursion bug in take inside update slots and friends if underliyin gconnection pool allows only for 1 connection, pass an optional connection (maybe not an issue, because we do it, before we take one , but maybe not always)
class SyncClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        if addresses is None:
            address = kwargs.get('address')
            if address:
                addresses = (address, )
            else:
                addresses = (('localhost', 6379), )
        self._initial_addresses = addresses
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
    # TODO (misc) add a hints option when there is a moved
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

    def _get_index_for_command(self, cmd):
        # commands are ascii, yes ? some commands can be larger than cmd[0] for index ? meh, let's be optimistic for now
        if isinstance(cmd, dict):
            cmd = cmd['command']
        command = bytes(cmd[0], 'ascii').upper()
        index = self._command_cache.get(command, -1)
        if index != -1:
            return index
        conn = self.take()
        try:
            command_info = conn(b'COMMAND', b'INFO', command)
        finally:
            self.release(conn)
        command_info = command_info[0]
        if command_info:
            index = command_info[3]
        else:
            index = 0
        self._command_cache[command] = index
        # TODO map unknown to None
        return index

    def _address_pool(self, address):
        pool = self._connections.get(address)
        if pool is None:
            with self._lock:
                pool = self._connections.get(address)
                if pool is None:
                    pool = SyncConnectionPool(address=address, **self._settings)
                    self._connections[address] = pool
        return pool

    # TODO we need to handle when last_connection points to a member of the pool that isn't valid anymore..
    # TODO fix this..
    def take(self, address=None):
        #if not address and self._last_connection and self._last_connection.
        if address:
            return self._address_pool(address).take()
        elif self._last_connection:
            # TODO check health, if bad, update slots
            return self._last_connection.take()
        else:
            endpoints = self._endpoints
            if not endpoints:
                endpoints = self._initial_addresses
            for address in endpoints:
                pool = self._address_pool(address)
                self._last_connection = pool
                break
        return self._last_connection.take()

    def take_by_key(self, key):
        hashslot = calc_hashslot(key)
        return self._connection_by_hashslot(hashslot)

    def take_by_cmd(self, *cmd):
        index = None
        # TODO meh detection (refactor into utils?)
        if isinstance(cmd[0], (tuple, list)):
            for command in cmd:
                index = self._get_index_for_command(*command)
                if index is not None:
                #TODO fix wrong
                    break
        else:
            index = self._get_index_for_command(*cmd)
        if index is None:
            return self.take()
        # TODO WRONG AND LAME
        if isinstance(cmd[0], dict):
            cmd = cmd[0]['command']
        hashslot = calc_hashslot(cmd[index].encode())
        return self._connection_by_hashslot(hashslot)

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
            # TODO handle MOVED error here
            return conn(*cmd, database=_database)
        finally:
            self.release(conn)

    @contextmanager
    def connection(self, key, _database=0):
        if self._clustered == False :
            conn = self.take()
        else:
            conn = self.take_by_key(key)
        try:
            conn.set_database(_database)
            # TODO handle MOVED error here
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                conn._command(b'DISCARD')
            except Error:
                pass
            self.release(conn)
