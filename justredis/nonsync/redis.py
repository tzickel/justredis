from .connectionpool import ConnectionPool
from .cluster import ClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url, merge_dicts


# TODO (misc) document all the kwargs everywhere
# TODO (api) internal remove from connectionpool the __enter__/__exit__ and use take(**kwargs)/release


# We do this seperation to allow changing per command and connection settings easily
class ModifiedRedis:
    def __init__(self, connection_pool, custom_command_class=None, **kwargs):
        self._connection_pool = connection_pool
        self._custom_command_class = custom_command_class
        self._custom_command = custom_command_class(self) if self._custom_command_class else None
        self._settings = kwargs

    async def aclose(self):
        self._connection_pool = self._settings = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return await self._connection_pool(*cmd)
        else:
            return await self._connection_pool(*cmd, **settings)

    async def connection(self, *args, push=False, **kwargs):
        if args:
            raise ValueError("Please specify the connection arguments as named arguments (i.e. push=..., key=...)")
        wrapper = PushConnection if push else Connection
        # TODO (async) do i need await on the connection itself ?
        return await wrapper.create(self._connection_pool.connection(**kwargs), **self._settings)

    async def endpoints(self):
        return await self._connection_pool.endpoints()

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return ModifiedRedis(self._connection_pool, custom_command_class=self._custom_command_class, **settings)

    def __getattr__(self, attribute):
        if not self._custom_command:
            raise AttributeError("No such attribute: %s" % attribute)
        return getattr(self._custom_command, attribute)


# TODO (api) should we implement an callback for when slots have changed ?
class Redis(ModifiedRedis):
    @classmethod
    def from_url(cls, url, **kwargs):
        res = parse_url(url)
        res.update(kwargs)
        return cls(**res)

    def __init__(self, pool_factory=ClusterConnectionPool, custom_command_class=None, **kwargs):
        """
        Currently documented in README.md
        """
        self._connection_pool = None
        if pool_factory == "pool":
            pool_factory = ConnectionPool
        elif pool_factory in ("auto", "cluster"):
            pool_factory = ClusterConnectionPool
        if not hasattr(pool_factory, "__call__"):
            raise AttributeError("A valid pool_factory is required, if you want to set address, use .from_url() or address=(host, port)")
        super(Redis, self).__init__(pool_factory(**kwargs), custom_command_class=custom_command_class)

    async def aclose(self):
        if self._connection_pool:
            await self._connection_pool.aclose()
        self._connection_pool = None


# TODO (api) add a modified_class here as well.
class ModifiedConnection:
    def __init__(self, connection, **kwargs):
        self._connection = connection
        self._settings = kwargs

    async def aclose(self):
        self._connection = self._settings = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return await self._connection(*cmd)
        else:
            return await self._connection(*cmd, **settings)

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return ModifiedConnection(self._connection, **settings)


class Connection(ModifiedConnection):
    @classmethod
    async def create(cls, connection, **kwargs):
        conn = await connection.__aenter__()
        ret = cls(conn, **kwargs)
        ret._connection_context = connection
        return ret

    def __init__(self, connection, **kwargs):
        super(Connection, self).__init__(connection, **kwargs)

    async def aclose(self):
        if self._connection_context:
            # TODO (correctness) is this correct?
            await self._connection_context.__aexit__(None, None, None)
        self._connection = None
        self._connection_context = None
        self._settings = None


class PushConnection(Connection):
    async def aclose(self):
        if self._connection:
            # We close the connection here, since it's both hard to reset the state of the connection, and this is usually not done / at low frequency.
            await self._connection.aclose()
        await super(PushConnection, self).aclose()

    async def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return await self._connection.push_command(*cmd)
        else:
            return await self._connection.push_command(*cmd, **settings)

    async def next_message(self, *args, timeout=None, **kwargs):
        if args:
            raise ValueError("Please specify the next_message arguments as named arguments (i.e. timeout=...)")
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return await self._connection.pushed_message(timeout=timeout)
        else:
            return await self._connection.pushed_message(timeout=timeout, **settings)

    def __iter__(self):
        return self

    # TODO (async) is this correct ?
    async def __next__(self):
        return await self._connection.pushed_message()
