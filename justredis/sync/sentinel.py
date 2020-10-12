from justredis.sync.connectionpool import ConnectionPool
from contextlib import contextmanager
from random import choice

from justredis.errors import CommunicationError
from .environment import get_environment
from .connectionpool import ConnectionPool

# Language is sentinel instances, groups, group leader, leader replicas
# TODO leader or master?

# TODO do we want to store name for lookup ?

# TODO move this errors to the errors.py file ?
# TODO should inherit from CommunicationError ?
class NoLeaderFound(Exception):
    pass

class NoReplicaFound(Exception):
    pass

class NoSentinelFound(Exception):
    pass

class NoEndpointFound(Exception):
    pass


def list2dict(l):
    i = iter(l)
    return dict(zip(i, i))


# if you suplly address it will connect and use the one provided by the server for now on (or allow for mapping)?
# update by error
# allow to use original address for scouting sentinal always ?
class SentinelConnectionPool:
    def __init__(self, group_name, sentinel_password=None, addresses=None, **kwargs):
        self._group_name = group_name
        self._sentinel_password = sentinel_password
        address = kwargs.pop("address", None)
        if addresses is None:
            if address:
                addresses = (address,)
            else:
                addresses = (("localhost", 26379),)
        elif address is not None:
            raise ValueError("Do not provide both addresses and address")
        self._initial_addresses = addresses
        self._settings = kwargs
        self._lock = get_environment(**kwargs).lock()
        self._sentinel_pools = {}
        self._leader_address = None
        self._leader_pool = None
        self._replica_pools = {}
        self._last_faulty_pool = None

    def __del__(self):
        self.close()

    def close(self):
        with self._lock:
            for pool in self._sentinel_pools.values():
                pool.close()
            self._sentinel_pools = {}
            if self._leader_pool is not None:
                self._leader_pool.close()
                self._leader_pool = None
                self._leader_address = None
            for pool in self._replica_pools.values():
                pool.close()
            self._replica_pools = {}
            self._last_faulty_pool = None

    def _update_connections_if_needed(self):
        # TODO should we enter this lock each time ?
        with self._lock:
            need_to_update = False
            if self._last_faulty_pool:
                need_to_update = True
            # TODO fix this with a need update flag...
            elif not self._sentinel_pools or not self._leader_address:# or not self._replica_pools:
                need_to_update = True
            if not need_to_update:
                return

            if not self._sentinel_pools:
                for address in self._initial_addresses:
                    self._sentinel_pools[address] = ConnectionPool(address=address, password=self._sentinel_password, **self._settings)
            finished_update = False
            for address, pool in self._sentinel_pools.items():
                # TODO make sure non of this commands throw an error
                try:
                    # If leader is empty, try the next sentinal instance
                    data = pool("sentinel", "master", self._group_name, decoder="utf8")
                    # TODO should we try the next sentinal instance in case the faulty connection was to the leader?
                    if not data:
                        continue
                    data = list2dict(data)
                    leader_address = (data["ip"], int(data["port"]))
                    data = pool("sentinel", "sentinels", self._group_name, decoder="utf8")
                    sentinel_addresses = [(x["ip"], int(x["port"])) for x in [list2dict(x) for x in data]]
                    # Sentinel does not report itself in the list ?
                    if address not in sentinel_addresses:
                        sentinel_addresses.append(address)
                    # TODO check if this is correct
                    data = pool("sentinel", "replicas", self._group_name, decoder="utf8")
                    replicas_addresses = [(x["ip"], int(x["port"])) for x in [list2dict(x) for x in data]]
                    finished_update = True
                except CommunicationError:
                    # TODO remove the pool from the accounting ?
                    # TODO also expose the exception as an chained ?
                    continue
            # TODO allow for a partial success?
            # TODO always reset faulty self._last_faulty_pool because we are rebuilding everything from fresh?
            if not finished_update:
                raise NoSentinelFound()
            # Sync the leader connection pool
            if leader_address != self._leader_address:
                if self._leader_pool:
                    self._leader_pool.close()
                self._leader_pool = ConnectionPool(address=leader_address, **self._settings)
                self._leader_address = leader_address
            # Sync the sentinels connection pool
            keys_to_remove = []
            for address, pool in self._sentinel_pools.items():
                if address not in sentinel_addresses:
                    pool.close()
                    keys_to_remove.append(address)
            for key in keys_to_remove:
                del self._sentinel_pools[key]
            for address in sentinel_addresses:
                if address not in self._sentinel_pools:
                    self._sentinel_pools[address] = ConnectionPool(address=address, password=self._sentinel_password, **self._settings)
            # Sync the sentinels connection pool
            keys_to_remove = []
            for address, pool in self._replica_pools.items():
                if address not in replicas_addresses:
                    pool.close()
                    keys_to_remove.append(address)
            for key in keys_to_remove:
                del self._replica_pools[key]
            for address in replicas_addresses:
                if address not in self._replica_pools:
                    self._replica_pools[address] = ConnectionPool(address=address, password=self._sentinel_password, **self._settings)
            # If we reached here we can clear the last I/O error encountered
            self._last_faulty_pool = None

    #endpoint can be a specific address that is one of the sentinels or the current leader of a group or one of the group's replicas
    #endpoint can be "leader", "replica" (pick a random, in the future load balance?), "sentinel" (pick a random, in the future load balance?)
    # the default is the group leader
    # this can fail as well (cause of auth and stuff)
    # hmm, different conneciton pool logins here?
    # maybe AUTH to sentinel and master?
    def take(self, endpoint=None):
        self._update_connections_if_needed()
        with self._lock:
            if endpoint is None or endpoint == "leader" or endpoint == self._leader_address:
                return self._leader_pool
            elif endpoint == "replica":
                if not self._replica_pools:
                    raise NoReplicaFound()
                return choice(list(self._replica_pools.values()))
            elif endpoint == "sentinel":
                # This should throw an error at the self._update_connections_if_needed() stage
                return choice(list(self._sentinel_pools.values()))
            elif endpoint in self._sentinel_pools.keys():
                return self._sentinel_pools[endpoint]
            elif endpoint in self._replica_pools.keys():
                return self._replica_pools[endpoint]
            else:
                raise NoEndpointFound("No such endpoint found: %s" % endpoint)

    @contextmanager
    def connection(self, endpoint=None, **kwargs):
        pool = self.take(endpoint)
        conn = pool.take()
        try:
            conn.allow_multi(True)
            yield conn
        except CommunicationError:
            self._last_faulty_pool = pool
            conn.close()
            raise
        finally:
            conn.allow_multi(False)
            pool.release(conn)

    def __call__(self, *cmd, endpoint=None, **kwargs):
        if not cmd:
            raise ValueError("No command provided")
        with self.connection(endpoint) as conn:
            return conn(*cmd, **kwargs)

    # TODO endpoint can be "all_sentinels", "all_replicas", "all" (for commands like info, or query sentinels)
    #def all(self, *cmd, endpoint=None, **kwargs):
        #pass

    def endpoints(self):
        self._update_connections_if_needed()
        ret = []
        with self._lock:
            for address in self._sentinel_pools.keys():
                ret.append((address, {"type": "sentinel"}))
            if self._leader_address:
                ret.append((self._leader_address, {"type": "leader"}))
            for address in self._replica_pools.keys():
                ret.append((address, {"type": "replica"}))
        return ret
