from binascii import crc_hqx

try:
    from contextlib import asynccontextmanager
except:
    from async_generator import asynccontextmanager
from random import choice

from .environment import get_environment
from .connectionpool import ConnectionPool
from ..decoder import Error, Result
from ..utils import is_multiple_commands
from ..encoder import parse_encoding
from ..errors import CommunicationError


def calc_hashslot(key):
    s = key.find(b"{")
    if s != -1:
        e = key.find(b"}")
        if e > s + 1:
            key = key[s + 1 : e]
    return crc_hqx(key, 0) % 16384


# TODO (correctness) I think I covered the multithreading sensetive parts, make sure
# TODO (misc) should I lazely check if there is a cluster ? (i.e. upgrade from a default connectionpool first)
# TODO (correctness) thinkg about how to ASK redirect in connection taking and pipelining
# TODO (correctness) make sure I dont have an issue where if there is a connection pool limit, I can get into a deadlock here
# TODO (misc) future optimization, if we can't take from last_connection beacuse of connection pool limit, choose another random one.
# TODO (correctness) disable encoding on all of conn commands
# TODO (correctness) when invalidating last_connection, run slots update ?
# TODO (misc) should _update_slots be called on each I/O error always ?


class ClusterConnectionPool:
    def __init__(self, addresses=None, **kwargs):
        address = kwargs.pop("address", None)
        if addresses is None:
            if address:
                addresses = (address,)
            else:
                addresses = (("localhost", 6379),)
        elif address is not None:
            raise ValueError("Do not provide both addresses, and address")
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
        self._closed = False

    async def aclose(self):
        async with self._lock:
            if not self._closed:
                for pool in self._connections.values():
                    await pool.aclose()
                self._connections.clear()
                self._last_connection = None
            self._closed = True

    async def _update_slots(self):
        # TODO (misc) or if there is a hint from MOVED, recheck if clustered?
        if self._clustered == False:
            return
        conn = await self.take()
        try:
            slots = await conn(b"CLUSTER", b"SLOTS")
            self._clustered = True
        except Error:
            slots = []
            self._clustered = False
        except Exception:
            # This is done to invalidate a potentially bad server, and pick up another one randomally next try
            self._last_connection = self._last_connection_peername = None
            raise
        finally:
            await self.release(conn)
        # TODO (misc) what todo about holes in hashslots in response ?
        if isinstance(slots, Result):
            slots = slots.data
        slots.sort(key=lambda x: x[0])
        if slots and isinstance(slots[0][2][0], bytes):
            slots = [(x[1], (x[2][0].decode(), x[2][1])) for x in slots]
        else:
            slots = [(x[1], (x[2][0], x[2][1])) for x in slots]
        # We weren't in a cluster before, and we aren't now
        if self._clustered == False and not self._slots:
            return
        # Remove connections which are not a part of the cluster anymore
        async with self._lock:
            previous_connections = set(self._connections.keys())
            new_connections = set([x[1] for x in slots])
            connections_to_remove = previous_connections - new_connections
            for address in connections_to_remove:
                await self._connections[address].aclose()
                del self._connections[address]
            self._slots = slots

            if connections_to_remove:
                # TODO (misc) an optimization can only do this if it's not in new_connections
                self._last_connection = self._last_connection_peername = None

    async def _connection_by_hashslot(self, hashslot):
        if not self._slots:
            await self._update_slots()
        if self._clustered == False:
            return await self.take()
        if not self._slots:
            # TODO (misc) Is this the correct exception type?
            raise ValueError("Could not find any slots in the redis cluster")
        for slot in self._slots:
            if hashslot <= slot[0]:
                break
        address = slot[1]
        return await self.take(address)

    async def _get_info_for_command(self, *cmd):
        # commands are ascii, yes ? some commands can be larger than cmd[0] for index ? meh, let's be optimistic for now
        command = cmd[0]
        encode = getattr(command, "encode", None)
        if encode:
            command = encode("ascii")
        command = command.upper()
        info = self._command_cache.get(command)
        if info:
            return info
        conn = await self.take()
        try:
            command_info = await conn(b"COMMAND", b"INFO", command)
        except Exception:
            # This is done to invalidate a potentially bad server, and pick up another one randomally next try
            self._last_connection = self._last_connection_peername = None
            raise
        finally:
            await self.release(conn)
        if isinstance(command_info, Result):
            command_info = command_info.data
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
    async def take(self, address=None):
        # if address:
        # return self._address_pool(address).take()
        # TODO (misc) is this best ?
        async with self._lock:
            if address:
                return await self._address_pool(address).take()
            if self._last_connection:
                try:
                    # TODO (misc) maybe do a health check here ? if there is an exception it will be invalidated anyhow for the next time...
                    return await self._last_connection.take()
                except Exception:
                    self._last_connection = None
            endpoints = [x[1] for x in self._slots.copy()]
            # TODO (correctness) maybe after I/O failure (repeated?) always go back to initial address ? or just remove an entry from the connection when it's invalid, till it's empty ?
            if not endpoints:
                endpoints = self._initial_addresses
            # TODO (correctness) should we pick up randomally, or go each one in the list on each failure ?
            address = choice(endpoints)
            pool = self._address_pool(address)
            self._last_connection = pool
            # TODO (corectness) on error, try the next one immidiatly
            conn = await self._last_connection.take()
            self._last_connection_peername = conn.peername()
            return conn

    async def take_by_key(self, key, **kwargs):
        if not isinstance(key, (bytes, bytearray)):
            encode = kwargs.get("encoder", self._settings.get("encoder"))
            key = parse_encoding(encode)(key)
        hashslot = calc_hashslot(key)
        return await self._connection_by_hashslot(hashslot)

    async def take_by_cmd(self, *cmd, **kwargs):
        if is_multiple_commands(*cmd):
            found = False
            # Some commands have no key information (like MULTI) so scan for one which does
            for command in cmd:
                info = await self._get_info_for_command(*command)
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
            info = await self._get_info_for_command(*cmd)
        # This should happen only if command doesn't exist
        if info is None:
            return await self.take()
        # TODO (misc) maybe see if command is movablekeys, and only do this then, for optimizations
        index = info[3]
        if index == 0:
            conn = await self.take()
            try:
                command = [b"COMMAND", b"GETKEYS"]
                command.extend(cmd)
                # TODO (misc) what do we want to do if an exception happened here ?
                command_info = await conn(*command, attributes=False, decoder=None)
                key = command_info[0]
            # This happens if the command has no key info, so any connection is good
            except Error:
                return await self.take()
            finally:
                await self.release(conn)
        else:
            # The command does not contain the key, usually this will result in a usage error ?
            if len(cmd) - 1 < index:
                return await self.take()
            else:
                key = cmd[index]
        return await self.take_by_key(key, **kwargs)

    async def release(self, conn):
        if self._clustered:
            # TODO (correctness) is the peername always 100% the same as the slot address ? to be on the safe side we can store both @ metadata
            address = conn.peername()
            pool = self._connections.get(address)
        else:
            # TODO (correctness) risky, if last_connection somehow changed (multiple fallback address?), we might be returning to the wrong one, does it matter then ?!?
            pool = self._last_connection
        # The connection might have been discharged
        if pool is None:
            await conn.aclose()
            return
        await pool.release(conn)

    async def __call__(self, *cmd, endpoint=False, **kwargs):
        if not cmd:
            raise ValueError("No command provided")
        if endpoint == "masters":
            return await self._on_all(*cmd)
        if self._clustered == False:
            conn = await self.take()
        elif endpoint:
            conn = await self.take(endpoint)
        else:
            conn = await self.take_by_cmd(*cmd)
        try:
            return await conn(*cmd, **kwargs)
        except CommunicationError:
            await self._update_slots()
            raise
        finally:
            seen_moved = conn.seen_moved()
            seen_asked = conn.seen_asked()
            await self.release(conn)
            if seen_moved:
                await self._update_slots()
                # If the user specified he wants a specific endpoint, we won't force the issue on him.
                # Also if ths cmd is multiple commands, we won't know which one failed and which didn't, so we don't try as well.
                if endpoint == False and not is_multiple_commands(*cmd):
                    return await self(*cmd, **kwargs)
            elif seen_asked:
                if endpoint == False and not is_multiple_commands(*cmd):
                    return await self(*cmd, **kwargs, endpoint=seen_asked, asking=True)

    @asynccontextmanager
    async def connection(self, key=None, endpoint=None, **kwargs):
        if key and endpoint:
            raise ValueError("Cannot specify both key and endpoint when taking a connection")
        if endpoint:
            conn = await self.take(endpoint)
        elif self._clustered == False or key is None:
            conn = await self.take()
        else:
            conn = await self.take_by_key(key, **kwargs)
        try:
            conn.allow_multi(True)
            yield conn
        except CommunicationError:
            await self._update_slots()
            raise
        finally:
            # We need to clean up the connection back to a normal state.
            try:
                if not conn.closed():
                    await conn._command(b"DISCARD")
            except Error:
                pass
            finally:
                # We need to handle the option where there was an moved error, to not have a recursion of a connection always trying the wrong server
                seen_moved = conn.seen_moved()
                conn.allow_multi(False)
                await self.release(conn)
                if seen_moved:
                    await self._update_slots()

    async def _on_all(self, *cmd, filter="master", **kwargs):
        if self._clustered is None:
            await self._update_slots()
        if self._clustered == False:
            # This will be always filled by the _update_slots (atleast)
            return {self._last_connection_peername: await self(*cmd, **kwargs)}
        # TODO (api) on an error here, raise an exception ?
        res = {}
        for address in await self.endpoints():
            if address[1]["type"] != filter:
                continue
            address = address[0]
            try:
                res[address] = await self(*cmd, endpoint=address, **kwargs)
            except Exception as e:
                res[address] = e
        return res

    async def endpoints(self):
        if self._clustered is None:
            await self._update_slots()
        if self._clustered:
            return [(x[1], {"type": "master"}) for x in self._slots.copy()]
        else:
            # This will be always filled by the _update_slots (atleast)
            return [(self._last_connection_peername, {"type": "regular"})]
