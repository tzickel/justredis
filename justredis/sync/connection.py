from .environment import get_environment
from ..decoder import RedisRespDecoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from ..errors import CommunicationError, PipelinedExceptions
from ..utils import get_command_name, is_multiple_commands
from ..command import RedisCommand


# TODO (correctness) watch for manual SELECT and set_database !


not_allowed_push_commands = set([b"MONITOR", b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE"])


class TimeoutError(Exception):
    pass


timeout_error = TimeoutError()


class Connection:
    @classmethod
    def create(cls, username=None, password=None, client_name=None, resp_version=2, socket_factory="tcp", connect_retry=2, database=0, **kwargs):
        ret = cls()
        ret._init(username, password, client_name, resp_version, socket_factory, connect_retry, database, **kwargs)
        return ret

    def __init__(self):
        self._socket = None

    # TODO (api) client_name with connection pool (?)
    def _init(self, username=None, password=None, client_name=None, resp_version=2, socket_factory="tcp", connect_retry=2, database=0, **kwargs):
        resp_version = int(resp_version)
        connect_retry = int(connect_retry)
        database = int(database)

        if resp_version not in (-1, 2, 3):
            raise ValueError("Unsupported RESP protocol version %s" % resp_version)

        environment = get_environment(**kwargs)
        connect_retry += 1
        while connect_retry:
            try:
                self._socket = environment.socket(socket_factory, **kwargs)
                break
            except Exception as e:
                connect_retry -= 1
                if not connect_retry:
                    raise CommunicationError() from e

        self._settings = kwargs
        self._encoder = RedisRespEncoder(**kwargs)
        self._decoder = RedisRespDecoder(**kwargs)
        self._seen_eof = False
        self._peername = self._socket.peername()
        self._seen_moved = False
        self._seen_ask = False
        self._allow_multi = False
        self._default_database = self._last_database = database
        self._client_id = None
        self._cache_client_id = None

        connected = False
        # Try to negotiate RESP3 first if RESP2 is not forced
        if resp_version != 2:
            args = [b"HELLO", b"3"]
            if password is not None:
                if username is not None:
                    args.extend((b"AUTH", username, password))
                else:
                    args.extend((b"AUTH", b"default", password))
            if client_name:
                args.extend((b"SETNAME", client_name))
            try:
                # TODO (misc) do something with the result ?
                self._command(RedisCommand.create(*args))
                connected = True
            except Error as e:
                # This is to seperate an login error from the server not supporting RESP3
                if e.args[0].startswith("ERR "):
                    if resp_version == 3:
                        # TODO (misc) this want have a __cause__ is that ok ? what exception to throw here ?
                        raise Exception("Server does not support RESP3 protocol")
                else:
                    raise
        if not connected:
            if password:
                if username:
                    self._command(b"AUTH", username, password)
                else:
                    self._command(b"AUTH", password)
            if client_name:
                self._command(b"CLIENT", b"SETNAME", client_name)
        if database != 0:
            self._command(b"SELECT", database)

        # actually we need to make sure it's all "bytes" so no decoding enformcent !
        #try:
        #    self._client_id = self._command(b"CLIENT", b"ID")
        #    if hasattr(self._client_id, "data"):
        #        self._client_id = self._client_id.data
        # Only exists in Redis 5
        #except Error:
        #       pass

    def __del__(self):
        self.close()

    def close(self):
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
            self._encoder = None
            self._decoder = None

    # TODO (misc) better check ? (maybe it's closed, but the socket doesn't know it yet..., will be known the next time though)
    def closed(self):
        return self._socket is None

    def has_data(self):
        return self._socket.is_readable()

    def peername(self):
        return self._peername

    # TODO (corectness) implment the set_database here
    def _send(self, cmd):
        try:
            if isinstance(cmd, RedisCommand):
                self._encoder.encode(cmd)
            else:
                self._encoder.encode_multiple(cmd)
            while True:
                data = self._encoder.extract()
                if data is None:
                    break
                self._socket.send(data)
        except ValueError as e:
            raise
        except Exception as e:
            self.close()
            raise CommunicationError("I/O error while trying to send a command") from e

    # TODO (misc) should a decoding error be considered an CommunicationError ?
    def _recv(self, timeout=False):
        try:
            while True:
                res = self._decoder.extract()
                if res == need_more_data:
                    if self._seen_eof:
                        self.close()
                        raise EOFError("Connection reached EOF")
                    else:
                        data = self._socket.recv(timeout)
                        if data == b"":
                            self._seen_eof = True
                        elif data is None:
                            return timeout_error
                        else:
                            self._decoder.feed(data)
                    continue
                return res
        except Exception as e:
            self.close()
            raise CommunicationError("Error while trying to read a reply") from e

    def pushed_message(self, timeout=False, decoder=False, attributes=None):
        orig_decoder = None
        if decoder != False or attributes is not None:
            orig_decoder = self._decoder
            kwargs = self._settings.copy()
            if decoder != False:
                kwargs["decoder"] = decoder
            if attributes is not None:
                kwargs["attributes"] = attributes
            self._decoder = RedisRespDecoder(**kwargs)
        try:
            res = self._recv(timeout)
            if res == timeout_error:
                return None
            return res
        finally:
            if orig_decoder is not None:
                self._decoder = orig_decoder

    def push_command(self, *cmd):
        self._send(*cmd)

    def set_database(self, database):
        if database is None:
            if self._default_database != self._last_database:
                self._command(b"SELECT", self._default_database)
                self._last_database = self._default_database
        else:
            if database != self._last_database:
                self._command(b"SELECT", database)
                self._last_database = database

    def __call__(self, cmd):
        if isinstance(cmd, RedisCommand):
            self._command(cmd)
        else:
            self._commands(cmd)

    def _validate(self, cmd):
        name = cmd.name()
        if name in not_allowed_push_commands:
            raise ValueError("Command %s is not allowed to be called directly, use the appropriate API instead" % name)
        if name == b"MULTI" and not self._allow_multi:
            raise ValueError("Take a connection if you want to use MULTI command.")

    def _command(self, cmd):
        try:
            self._validate(cmd)
            self._send(cmd)
            res = self._recv()
            if res == timeout_error:
                self.close()
                raise timeout_error
            cmd.set_result(res)
            if isinstance(res, Error):
                raise res
        except Exception as e:
            cmd.set_result(e)
            raise

    def _commands(self, cmds):
        found_errors = False
        for cmd in cmds:
            self._validate(cmd)
        self._send(cmds)
        res = []
        found_errors = False
        for cmd in cmds:
            try:
                result = self._recv()
                if isinstance(result, Error):
                    found_errors = True
                if result == timeout_error:
                    self.close()
            except Exception as e:
                result = e
                found_errors = True
            res.append(result)
        if found_errors:
            raise PipelinedExceptions(res)
        return res

    def allow_multi(self, allow):
        self._allow_multi = allow

"""    @property
    def cache_client_id(self):
        return self._cache_client_id

    @cache_client_id.setter
    def cache_client_id(self, value):
        optin = self._settings.get("cache_optin", False)
        optout = self._settings.get("cache_optout", False)
        if optin and output:
            raise Exception("Can't set both OPTIN and OPTOUT")
        prefixes = self._settings.get("cache_prefixes", None)
        cmd = [b"CLIENT", b"TRACKING", b"on", b"REDIRECT", value]
        self._command(*cmd)
        self._cache_client_id = value
"""