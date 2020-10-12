from justredis.sync.connectionpool import ConnectionPool
from contextlib import contextmanager
from random import choice

from ..errors import CommunicationError, NoReplicaFound, NoSentinelFound, NoEndpointFound
from .environment import get_environment
from .connectionpool import ConnectionPool

# Language is sentinel instances, groups, group leader, leader replicas

# TODO (design) there are similar concepts between this and cluster support at the low level (endpoint mapping) we can refactor this code together to some kind of toplogy manager
# TODO (design) do we want to allow a policy that checks the mapping on each command ?
# TODO (performance) do we want to store name for endpoint lookup by name?

# TODO (deisgn) move this errors to the errors.py file ?
# TODO (misc) expose this errors in __all__
# TODO (deisgn) should this exceptions inherit from CommunicationError ?


# TODO do we want differnet socket timeout options that on the regular ocnection ?
# TODO add replica-first option then only master?


def list2dict(l):
    i = iter(l)
    return dict(zip(i, i))



# Should we allow for a policy where initial addresses will always be used instead of one we got from the first instance we found ?
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
        self._should_update_instances = True

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
            self._should_update_instances = True

    def _update_connections_if_needed(self):
        if self._should_update_instances:
            with self._lock:
                if not self._should_update_instances:
                    return

                if not self._sentinel_pools:
                    for address in self._initial_addresses:
                        self._sentinel_pools[address] = ConnectionPool(address=address, password=self._sentinel_password, **self._settings)

                finished_update = False
                for address, pool in self._sentinel_pools.items():
                    # TODO what should be done if an RedisError is thrown inside this try ?
                    # TODO allow for partial success (i.e. we got the leader, but I/O error afterwards ?)
                    # TODO if we keep getting an I/O error from the results of a sentinel (such as an bad leader) we can try skipping it ?
                    # TODO we can fix this with a random ordering of sentinel list ?
                    try:
                        # TODO should we try the next sentinal instance in case the faulty connection was to the leader?
                        data = pool("sentinel", "master", self._group_name, decoder="utf8")
                        # If leader is empty, try the next sentinal instance
                        # TODO we might want to distinct between no master and no sentinel in the below raise NoSentinelFound()
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
                        # TODO should this pool be removed from accounting ?
                        # TODO if this is the last element in the list, chain it's exception to the raise NoSentinelFound
                        continue
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
                self._should_update_instances = False

    # endpoint can be a specific address that is one of the sentinels or the current leader of a group or one of the group's replicas
    # endpoint can be "leader", "replica" (pick a random, in the future load balance?), "sentinel" (pick a random, in the future load balance?)
    # the default is the group leader
    def take(self, endpoint=None):
        # TODO if replica and empty requested, then update it
        self._update_connections_if_needed()
        with self._lock:
            if endpoint is None or endpoint == "leader" or endpoint == self._leader_address:
                return self._leader_pool
            elif endpoint == "replica":
                if not self._replica_pools:
                    raise NoReplicaFound()
                # TODO don't return replicas which are currently known as down ?
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
    def connection(self, endpoint=None, _allow_multi=True, **kwargs):
        pool = self.take(endpoint)
        conn = pool.take()
        try:
            if _allow_multi:
                conn.allow_multi(True)
            yield conn
        # TODO We should handle the case were an leader has become a replica and this is read only and set _should_update_instances to true
        except CommunicationError:
            self._should_update_instances = True
            # TODO is this needed ?
            conn.close()
            raise
        finally:
            if _allow_multi:
                conn.allow_multi(False)
            pool.release(conn)

    def __call__(self, *cmd, endpoint=None, **kwargs):
        if not cmd:
            raise ValueError("No command provided")
        with self.connection(endpoint, False) as conn:
            return conn(*cmd, **kwargs)

    # TODO future implement this function allowing to target all "sentinel" or all "replica" or "all" (for comands like info)
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
