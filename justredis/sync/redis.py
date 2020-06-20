from .connectionpool import ConnectionPool
from .cluster import ClusterConnectionPool
from ..decoder import Error
from ..utils import parse_url, merge_dicts, CommandsInfo
from ..command import RedisCommand


# TODO (api) internal remove from connectionpool the __enter__/__exit__ and use take(**kwargs)/release
# TODO (misc) the name is connection manager not pool

# We do this seperation to allow changing per command and connection settings easily
class ModifiedRedis:
    def __init__(self, connection_pool, custom_command_class=None, **kwargs):
        self._connection_pool = connection_pool
        self._custom_command_class = custom_command_class
        self._custom_command = custom_command_class(self) if self._custom_command_class else None
        self._settings = kwargs
        self._commands_info = CommandsInfo()

    def __del__(self):
        self.close()

    def close(self):
        self._connection_pool = self._settings = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Even if multiple commands are sent, they are not related
    def __call__(self, *cmd, **kwargs):
        if not self._commands_info.initialized():
            with self._connection_pool.connection() as c:
                res = c(RedisCommand(b"COMMAND"))

        cmds = RedisCommand.create(*cmd)
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection_pool(cmds)
        else:
            return self._connection_pool(cmds, **settings)

    def connection(self, *args, push=False, **kwargs):
        if args:
            raise ValueError("Please specify the connection arguments as named arguments (i.e. push=..., key=...)")
        wrapper = PushConnection if push else Connection
        return wrapper.create(self._connection_pool.connection(**kwargs), **self._settings)

    def endpoints(self):
        return self._connection_pool.endpoints()

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
        if pool_factory == "pool":
            pool_factory = ConnectionPool
        elif pool_factory in ("auto", "cluster"):
            pool_factory = ClusterConnectionPool
        super(Redis, self).__init__(pool_factory(**kwargs), custom_command_class=custom_command_class)

    def __del__(self):
        self.close()

    def close(self):
        if self._connection_pool:
            self._connection_pool.close()
        self._connection_pool = None


# TODO (api) add a modified_class here as well.
class ModifiedConnection:
    def __init__(self, connection, **kwargs):
        self._connection = connection
        self._settings = kwargs

    def __del__(self):
        self.close()

    def close(self):
        self._connection = self._settings = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection(*cmd)
        else:
            return self._connection(*cmd, **settings)

    def modify(self, **kwargs):
        settings = self._settings.copy()
        settings.update(kwargs)
        return ModifiedConnection(self._connection, **settings)


class Connection(ModifiedConnection):
    @classmethod
    def create(cls, connection, **kwargs):
        conn = connection.__enter__()
        ret = cls(conn, **kwargs)
        ret._connection_context = connection
        return ret

    def __init__(self, connection, **kwargs):
        super(Connection, self).__init__(connection, **kwargs)

    def __del__(self):
        self.close()

    def close(self):
        if self._connection_context:
            # TODO (correctness) is this correct?
            self._connection_context.__exit__(None, None, None)
        self._connection = None
        self._connection_context = None
        self._settings = None


class PushConnection(Connection):
    def close(self):
        if self._connection:
            # We close the connection here, since it's both hard to reset the state of the connection, and this is usually not done / at low frequency.
            self._connection.close()
        super(PushConnection, self).close()

    def __call__(self, *cmd, **kwargs):
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection.push_command(*cmd)
        else:
            return self._connection.push_command(*cmd, **settings)

    def next_message(self, *args, timeout=None, **kwargs):
        if args:
            raise ValueError("Please specify the next_message arguments as named arguments (i.e. timeout=...)")
        settings = merge_dicts(self._settings, kwargs)
        if settings is None:
            return self._connection.pushed_message(timeout=timeout)
        else:
            return self._connection.pushed_message(timeout=timeout, **settings)

    def __iter__(self):
        return self

    def __next__(self):
        return self._connection.pushed_message()
