# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx
from threading import Lock
from contextlib import contextmanager

from .connectionpool import SyncConnectionPool
from ..decoder import Error
from ..utils import is_multiple_commands
from ..encoder import parse_encoding


def calc_hashslot(key):
    s = key.find(b'{')
    if s != -1:
        e = key.find(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    return crc_hqx(key, 0) % 16384


# TODO (correctness) I think I covered the multithreading sensetive parts, make sure
# TODO (misc) should I lazely check if there is a cluster ? (i.e. upgrade from a default connectionpool first)
# TODO (correctness) add ASKING
# TODO (correctness) make sure I dont have an issue where if there is a connection pool limit, I can get into a deadlock here
# TODO (misc) future optimization, if we can't take from last_connection beacuse of connection pool limit, choose another random one.
# TODO (correctness), disable encoding on all of conn commands


class SyncClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        if addresses is None:
            address = kwargs.pop('address', None)
            if address:
                addresses = (address, )
            else:
                addresses = (('localhost', 6379), )
        self._initial_addresses = addresses
        self._settings = kwargs
        self._connections = {}
        self._lock = Lock()
        self._last_connection = None
        self._slots = []
        self._clustered = None
        self._command_cache = {}

    def __del__(self):
        self.close()

    def close(self):
        with self._lock:
            for pool in self._connections.values():
                pool.close()
            self._connections.clear()
            self._last_connection = None

    def _update_slots(self):
        # TODO (misc) or if there is a hint from MOVED, recheck if clustered !
        if self._clustered is False:
            return
        conn = self.take()
        try:
            try:
                slots = conn(b'CLUSTER', b'SLOTS', attributes=False, decode=None)
                self._clustered = True
            except Error:
                slots = []
                self._clustered = False
        finally:
            self.release(conn)
        # TODO (correctness) check response in RESP2/RESP3
        slots.sort(key=lambda x: x[0])
        slots = [(x[1], (x[2][0].decode(), x[2][1])) for x in slots]
        # We weren't in a cluster before, and we aren't now
        if self._clustered == False:
            return
        # Remove connections which are not a part of the cluster anymore
        with self._lock:
            previous_connections = set(self._connections.keys())
            new_connections = set([x[1] for x in slots])
            connections_to_remove = previous_connections - new_connections
            for address in connections_to_remove:
               self._connections[address].close()
               del self._connections[address]
            self._slots = slots

            # TODO (misc) we can optimize this to only invalidate self._last_connection if it's not in new_connections

            if self._last_connection_peername not in new_connections:
                pass
            # Since this is the only place we modify the slots list, let's make sure last_connection is still valid !

    def _connection_by_hashslot(self, hashslot):
        if not self._slots:
            self._update_slots()
        if self._clustered == False:
            return self.take()
        if not self._slots:
            raise Exception('Could not find any slots in the redis cluster')
        for slot in self._slots:
            # TODO flip logic ?
            if hashslot > slot[0]:
                continue
            break
        address = slot[1]
        return self.take(address)

    def _get_index_for_command(self, *cmd):
        # commands are ascii, yes ? some commands can be larger than cmd[0] for index ? meh, let's be optimistic for now
        # TODO (misc) refactor this to utils
        if isinstance(cmd[0], dict):
            cmd = cmd[0]['command']
        command = bytes(cmd[0], 'ascii').upper()
        index = self._command_cache.get(command, -1)
        if index != -1:
            return index
        conn = self.take()
        try:
            # TODO (coorectness) see how this is in RESP2/RESP3 (also encoding !!!!!!!!!!!)
            command_info = conn(b'COMMAND', b'INFO', command)
        finally:
            self.release(conn)
        command_info = command_info[0]
        if command_info:
            index = command_info[3]
        else:
            index = 0
        """if index == 0:
            conn = self.take()
            try:
                # TODO (coorectness) see how this is in RESP2/RESP3 (also encoding !!!!!!!!!!!)
                command = [b'COMMAND', b'GETKEYS']
                command.extend(cmd)
                command_info = conn(*command)
                import pdb; pdb.set_trace()
            except Exception:
                pass
            finally:
                self.release(conn)"""
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
    # TODO (misc) make sure the address got here from _slots (or risk stale data)
    # TODO fix this..
    def take(self, address=None):
        if address:
            return self._address_pool(address).take()
        elif self._last_connection:
            # TODO check health, if bad, update slots
            try:
                return self._last_connection.take()
            except:
                self._last_connection = None
        ###endpoints = self.endpoints()
        endpoints = [x[1] for x in self._slots]
        if not endpoints:
            endpoints = self._initial_addresses
        for address in endpoints:
            pool = self._address_pool(address)
            self._last_connection = pool
            break
        # TODO make this atomicaly
        conn = self._last_connection.take()
        self._last_connection_peername = conn.peername()
        return conn

    def take_by_key(self, key):
        if not isinstance(key, (bytes, bytearray)):
            key = parse_encoding(self._settings.get('encoder', None))(key)
        hashslot = calc_hashslot(key)
        return self._connection_by_hashslot(hashslot)

    def take_by_cmd(self, *cmd):
        index = None
        if is_multiple_commands(*cmd):
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
        # TODO maybe see if command is movablekeys, and only do this then, for optimizations
        if index == 0:
            conn = self.take()
            try:
                # TODO (correctness) see how this is in RESP2/RESP3 (also encoding !!!!!!!!!!!)
                command = [b'COMMAND', b'GETKEYS']
                command.extend(cmd)
                command_info = conn(*command)
                key = command_info[0]
            except Exception:
                # TODO (what to do here?)
                return self.take()
                pass
            finally:
                self.release(conn)
        else:
            if len(cmd) - 1 < index:
                return self.take()
            else:
                key = cmd[index].encode()
        hashslot = calc_hashslot(key)
        return self._connection_by_hashslot(hashslot)

    def release(self, conn):
        # TODO (correctness) is the peername always 100% the same as the slot address ? to be on the safe side we can store both @ metadata
        address = conn.peername()
        pool = self._connections.get(address)
        # The connection might have been discharged
        if pool is None:
            conn.close()
            return
        pool.release(conn)

    def __call__(self, *cmd, endpoints=False, **kwargs):
        if endpoints == 'masters':
            # TODO send kwargs
            return self._on_all_masters(*cmd)
        elif endpoints != False:
            raise ValueError('Only supported endpoints options is "masters" or False')
        else:
            if self._clustered == False:
                conn = self.take()
            else:
                # TODO (misc) can we defend against _database != 0 here, when self_clustered is still None ? let just the server complaint and thats it...
                conn = self.take_by_cmd(*cmd)
            try:
                return conn(*cmd, **kwargs)
            # TODO (document) PartialError response here won't handle those MOVED ?
            except Error as e:
                if e.args[0].startswith(b'MOVED '):
                    # TODO (misc) here is a case to reuse this connection instead of getting a new one inside _update_slots
                    self._update_slots()
                    return self(*cmd, **kwargs)
                raise
            finally:
                self.release(conn)

    # TODO (documentation) because of MOVED semantics, it's best that on exception you should re-get a new cnonection each time (even on watch error)
    @contextmanager
    def connection(self, key=None, **kwargs):
        if self._clustered == False or key is None:
            conn = self.take()
        else:
            # TODO (misc) defend against _database != 0
            conn = self.take_by_key(key)
        try:
            # TODO handle MOVED error here
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                conn._command(b'DISCARD')
            except Error:
                pass
            self.release(conn)

    # TODO accept **kwargs !
    def _on_all_masters(self, *cmd):
        if self._clustered == False:
            # TODO (correctness) fix the output to include the address
            return {'local': self(*cmd)}
        elif self._clustered is None:
            self._update_slots()
        res = {}
        # TODO (correctness) if this loop fails, catch and return partial error... just like multiple concurrent commands
        for address in self.endpoints():
            if 'master' not in address[1]:
                continue
            address = address[0]
            conn = self.take(address)
            try:
                res[address] = conn(*cmd)
            except:
                self.release(conn)
        return res

    # TODO (misc) thread safety
    def endpoints(self):
        if self._clustered is None:
            self._update_slots()
        if self._clustered:
            return [(x[1], ['master']) for x in self._slots]
        else:
            return [(x, ['regular']) for x in self._initial_addresses]
