from binascii import crc_hqx
from contextlib import contextmanager
from random import choice

from .environment import get_environment
from .connectionpool import ConnectionPool
from ..decoder import Error
from ..utils import is_multiple_commands
from ..encoder import parse_encoding


def calc_hashslot(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}")
        if e > s + 1:
            key = key[s + 1 : e]
    return crc_hqx(key, 0) % 16384


# TODO (correctness) I think I covered the multithreading sensetive parts, make sure
# TODO (misc) should I lazely check if there is a cluster ? (i.e. upgrade from a default connectionpool first)
# TODO (correctness) add ASKING
# TODO (correctness) make sure I dont have an issue where if there is a connection pool limit, I can get into a deadlock here
# TODO (misc) future optimization, if we can't take from last_connection beacuse of connection pool limit, choose another random one.
# TODO (correctness) disable encoding on all of conn commands
# TODO (correctness) when invalidating last_connection, run slots update ?


class ClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        if addresses is None:
            address = kwargs.pop("address", None)
            if address:
                addresses = (address,)
            else:
                addresses = (("localhost", 6379),)
        self._initial_addresses = addresses
        self._settings = kwargs
        self._connections = {}
        self._lock = get_environment(**kwargs).lock()
        self._last_connection = None
        self._last_connection_peername = None
        self._slots = []
        # None = unknown, False = Nop, True = Yep
        self._clustered = None
        # Command info results
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
        # TODO (misc) or if there is a hint from MOVED, recheck if clustered?
        if self._clustered is False:
            return
        conn = self.take()
        try:
            slots = conn(b"CLUSTER", b"SLOTS", attributes=False, decode=None)
            self._clustered = True
        except Error:
            slots = []
            self._clustered = False
        except Exception:
            # This is done to invalidate a potentially bad server, and pick up another one randomally next try
            self._last_connection = self._last_connection_peername = None
            raise
        finally:
            self.release(conn)
        # Our test coverage should make sure RESP2/3 answer the same here
        # TODO (misc) what todo about holes in hashslots in response ?
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

            if connections_to_remove:
                # TODO (misc) an optimization can only do this if it's not in new_connections
                self._last_connection = self._last_connection_peername = None

    def _connection_by_hashslot(self, hashslot):
        if not self._slots:
            self._update_slots()
        if self._clustered == False:
            return self.take()
        if not self._slots:
            raise Exception("Could not find any slots in the redis cluster")
        for slot in self._slots:
            if hashslot <= slot[0]:
                break
        address = slot[1]
        return self.take(address)

    def _get_info_for_command(self, *cmd):
        # commands are ascii, yes ? some commands can be larger than cmd[0] for index ? meh, let's be optimistic for now
        # TODO (misc) refactor this to utils
        if isinstance(cmd[0], dict):
            cmd = cmd[0]["command"]
        command = cmd[0]
        encode = getattr(command, "encode", None)
        if encode:
            command = encode("ascii")
        command = command.upper()
        info = self._command_cache.get(command)
        if info:
            return info
        conn = self.take()
        try:
            command_info = conn(b"COMMAND", b"INFO", command, attribues=None, decode=None)
        except Exception:
            # This is done to invalidate a potentially bad server, and pick up another one randomally next try
            self._last_connection = self._last_connection_peername = None
            raise
        finally:
            self.release(conn)
        command_info = command_info[0]
        # TODO (misc) on null, cache it too ?
        if command_info:
            self._command_cache[command] = command_info
        return command_info

    def _address_pool(self, address):
        pool = self._connections.get(address)
        if pool is None:
            # with self._lock:
            pool = self._connections.get(address)
            if pool is None:
                pool = ConnectionPool(address=address, **self._settings)
                self._connections[address] = pool
        return pool

    # TODO (misc) make sure the address got here from _slots (or risk stale data)
    def take(self, address=None):
        # if address:
        # return self._address_pool(address).take()
        # TODO (misc) is this best ?
        with self._lock:
            if address:
                return self._address_pool(address).take()
            if self._last_connection:
                try:
                    # TODO (misc) maybe do a health check here ? if there is an exception it will be invalidated anyhow for the next time...
                    return self._last_connection.take()
                except Exception:
                    self._last_connection = None
            endpoints = [x[1] for x in self._slots.copy()]
            if not endpoints:
                endpoints = self._initial_addresses
            # TODO (correctness) should we pick up randomally, or go each one in the list on each failure ?
            address = choice(endpoints)
            pool = self._address_pool(address)
            self._last_connection = pool
            conn = self._last_connection.take()
            self._last_connection_peername = conn.peername()
            return conn

    def take_by_key(self, key, **kwargs):
        if not isinstance(key, (bytes, bytearray)):
            encode = kwargs.get("encoder", self._settings.get("encoder"))
            key = parse_encoding(encode)(key)
        hashslot = calc_hashslot(key)
        return self._connection_by_hashslot(hashslot)

    def take_by_cmd(self, *cmd, **kwargs):
        if is_multiple_commands(*cmd):
            found = False
            # Some commands have no key information (like MULTI) so scan for one which does
            for command in cmd:
                info = self._get_info_for_command(*command)
                if info is None:
                    continue
                index = info[3]
                if index != 0:
                    found = True
                    cmd = command
                    break
            if not found:
                cmd = cmd[0]
        else:
            info = self._get_info_for_command(*cmd)
        # This should happen only if command doesn't exist
        if info is None:
            return self.take()
        if isinstance(cmd[0], dict):
            cmd = cmd[0]["command"]
        # TODO (misc) maybe see if command is movablekeys, and only do this then, for optimizations
        index = info[3]
        if index == 0:
            conn = self.take()
            try:
                command = [b"COMMAND", b"GETKEYS"]
                command.extend(cmd)
                # TODO (misc) what do we want to do if an exception happened here ?
                encode = kwargs.get("encoder", self._settings.get("encoder"))
                command_info = conn(*command, encoder=encode, attributes=False, decoder=None)
                key = command_info[0]
            except Error:
                return self.take()
            finally:
                self.release(conn)
        else:
            # The command did not specify the key, usually this will result in a usage error ?
            if len(cmd) - 1 < index:
                return self.take()
            else:
                key = cmd[index]
        return self.take_by_key(key, **kwargs)

    def release(self, conn):
        if self._clustered:
            # TODO (correctness) is the peername always 100% the same as the slot address ? to be on the safe side we can store both @ metadata
            address = conn.peername()
            pool = self._connections.get(address)
        else:
            # TODO (correctness) risky, if last_connection somehow changed (multiple fallback address?), we might be returning to the wrong one, does it matter than ?!?
            pool = self._last_connection
        # The connection might have been discharged
        if pool is None:
            conn.close()
            return
        pool.release(conn)

    def __call__(self, *cmd, endpoint=False, **kwargs):
        if endpoint == "masters":
            return self._on_all(*cmd, **kwargs)
        if self._clustered == False:
            conn = self.take()
        elif endpoint:
            conn = self.take(endpoint)
        else:
            # TODO (misc) can we defend against _database != 0 here, when self_clustered is still None ? let just the server complaint and thats it...
            conn = self.take_by_cmd(*cmd, **kwargs)
        # TODO (correctness) on I/O error, update slots as well...
        try:
            return conn(*cmd, **kwargs)
        finally:
            seen_moved = conn.seen_moved()
            self.release(conn)
            if seen_moved:
                self._update_slots()
                # If the user specified he wants a specific endpoint, we won't force the issue on him.
                # Also if ths cmd is multiple commands, we won't know which one failed and which didn't, so we don't try as well.
                if endpoint == False and not is_multiple_commands(*cmd):
                    return self(*cmd, **kwargs)

    # TODO (documentation) because of MOVED semantics, it's best that on exception you should re-get a new connection each time (even on watch error)
    @contextmanager
    def connection(self, key=None, endpoint=None, **kwargs):
        if key and endpoint:
            raise ValueError("Cannot specify both key and endpoint when taking a connection")
        if endpoint:
            conn = self.take(endpoint)
        elif self._clustered == False or key is None:
            conn = self.take()
        else:
            # TODO (misc) defend against _database != 0
            # TODO (correctness) encoding can be here in kwargs...
            conn = self.take_by_key(key, **kwargs)
        # TODO (correctness) on I/O error, update slots as well...
        try:
            yield conn
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                if not conn.closed():
                    conn._command(b"DISCARD")
            except Error:
                pass
            finally:
                # We need to handle the option where there was an moved error, to not have a recursion of a connection always trying the wrong server
                seen_moved = conn.seen_moved()
                self.release(conn)
                if seen_moved:
                    self._update_slots()

    def _on_all(self, *cmd, filter="master", **kwargs):
        if self._clustered is None:
            self._update_slots()
        if self._clustered == False:
            # This will be always filled by the _update_slots (atleast)
            return {self._last_connection_peername: self(*cmd, **kwargs)}
        res = {}
        for address in self.endpoints():
            if address[1]["type"] != filter:
                continue
            address = address[0]
            try:
                res[address] = self(*cmd, endpoint=address, **kwargs)
            except Exception as e:
                res[address] = e
        return res

    def endpoints(self):
        if self._clustered is None:
            self._update_slots()
        if self._clustered:
            return [(x[1], {"type": "master"}) for x in self._slots.copy()]
        else:
            # This will be always filled by the _update_slots (atleast)
            return [(self._last_connection_peername, {"type": "regular"})]
