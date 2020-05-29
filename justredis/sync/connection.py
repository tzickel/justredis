from .environment import SocketTimeout, get_environment
from ..decoder import RedisRespDecoder, RedisResp2Decoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from ..errors import CommunicationError
from ..utils import get_command_name, is_multiple_commands


not_allowed_push_commands = set([b"MONITOR", b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE"])


class TimeoutError(Exception):
    pass


timeout_error = TimeoutError()


class Connection:
    # TODO (api) client_name with connection pool (?)
    # TODO (api) default to resp_version=2 ?
    # TODO (documentation) the username/password/client_name need the decoding of whatever **kwargs is passed
    def __init__(self, username=None, password=None, client_name=None, resp_version=-1, socket_factory="tcp", connect_retry=2, database=0, **kwargs):
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
        self._encoder = RedisRespEncoder(**kwargs)
        self._decoder = RedisRespDecoder(**kwargs)
        self._seen_eof = False
        self._peername = self._socket.peername()

        self._default_database = self._last_database = database

        self._seen_moved = False

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
                self._command(*args)
                connected = True
            except Error as e:
                # This is to seperate an login error from the server not supporting RESP3
                if e.args[0].startswith(b"ERR"):
                    if resp_version == 3:
                        # TODO (api) this want have a __cause__ is that ok ? what exception to throw here ?
                        raise Exception("Server does not support RESP3 protocol")
                else:
                    raise
        if not connected:
            self._decoder = RedisResp2Decoder(**kwargs)
            if password:
                if username:
                    self._command(b"AUTH", username, password)
                else:
                    self._command(b"AUTH", password)
            if client_name:
                self._command(b"CLIENT", b"SETNAME", client_name)
        if database != 0:
            self._command((b"SELECT", database))

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

    def peername(self):
        return self._peername

    # TODO (misc) an encoding error will close the connection (this can be fixed though)
    def _send(self, *cmd, multiple=False, **kwargs):
        try:
            if multiple:
                for _cmd in cmd:
                    self._encoder.encode(*_cmd[0], **_cmd[1])
            else:
                self._encoder.encode(*cmd, **kwargs)
            while True:
                data = self._encoder.extract()
                if data is None:
                    break
                self._socket.send(data)
        except ValueError as e:
            self.close()
            raise CommunicationError("Data encoding error while trying to send a command") from e
        except Exception as e:
            self.close()
            raise CommunicationError("I/O error while trying to send a command") from e

    # TODO (misc) should a decoding error be considered an CommunicationError ?
    def _recv(self, timeout=False, **kwargs):
        try:
            while True:
                res = self._decoder.extract(**kwargs)
                if res == need_more_data:
                    if self._seen_eof:
                        self.close()
                        raise EOFError("Connection reached EOF")
                    else:
                        data = self._socket.recv(timeout)
                        if data == b"":
                            self._seen_eof = True
                        else:
                            self._decoder.feed(data)
                    continue
                return res
        except SocketTimeout:
            return timeout_error
        except Exception as e:
            self.close()
            raise CommunicationError("I/O error while trying to read a reply") from e

    def pushed_message(self, **kwargs):
        res = self._recv(**kwargs)
        if res == timeout_error:
            return None
        return res

    def push_command(self, *cmd, **kwargs):
        if is_multiple_commands(*cmd):
            self._send(*cmd, **kwargs)
        else:
            send = []
            for _cmd in cmd:
                if isinstance(_cmd, dict):
                    command = _cmd.pop("command")
                    tmp_kwargs = kwargs.copy()
                    tmp_kwargs.update(_cmd)
                else:
                    command = _cmd
                    tmp_kwargs = kwargs
                send.append((command, tmp_kwargs))
            self._send(*send, multiple=True)

    def set_database(self, database):
        if database is None:
            if self._default_database != self._last_database:
                self._command(b"SELECT", self._default_database)
                self._last_database = self._default_database
        else:
            if database != self._last_database:
                self._command(b"SELECT", database)
                self._last_database = database

    # TODO (correctness) if we see SELECT we should update it manually ! what about SELECT in scripts, etc :/
    def __call__(self, *cmd, database=None, **kwargs):
        if not cmd:
            raise ValueError("No command provided")
        self.set_database(database)
        if is_multiple_commands(*cmd):
            return self._commands(*cmd, **kwargs)
        else:
            return self._command(*cmd, **kwargs)

    def _command(self, *cmd, **kwargs):
        if isinstance(cmd[0], dict):
            cmd = cmd[0]
            command = cmd.pop("command")
            # We need to copy this to not modify the outer dictionary (but it's ok, usually people don't pass dictionary as commands)
            kwargs = kwargs.copy()
            kwargs.update(cmd)
        else:
            command = cmd
        command_name = get_command_name(command)
        if command_name in not_allowed_push_commands:
            raise ValueError("Command %s is not allowed to be called directly, use the appropriate API instead" % cmd)
        if command_name == b"MULTI" and not self.allow_multi:
            raise ValueError("Take a connection if you want to use MULTI command.")
        self._send(*command, **kwargs)
        res = self._recv(**kwargs)
        if isinstance(res, Error):
            if res.args[0].startswith(b"MOVED "):
                self._seen_moved = True
            raise res
        if res == timeout_error:
            self.close()
            raise TimeoutError()
        return res

    def _commands(self, *cmds, **kwargs):
        send = []
        recv = []
        for cmd in cmds:
            if isinstance(cmd, dict):
                command = cmd.pop("command")
                tmp_kwargs = kwargs.copy()
                tmp_kwargs.update(cmd)
            else:
                command = cmd
                tmp_kwargs = kwargs
            command_name = get_command_name(command)
            if command_name in not_allowed_push_commands:
                raise ValueError("Command %s is not allowed to be called directly, use the appropriate API instead" % cmd)
            if command_name == b"MULTI" and not self.allow_multi:
                raise ValueError("Take a connection if you want to use MULTI command.")
            send.append((command, tmp_kwargs))
            recv.append(tmp_kwargs)
        self._send(*send, multiple=True)
        res = []
        found_errors = False
        # TODO (misc) on error, return a partial error ?
        for _recv in recv:
            result = self._recv(**_recv)
            if isinstance(result, Error):
                if result.args[0].startswith(b"MOVED "):
                    self.seen_moved = True
                found_errors = True
            if result == timeout_error:
                self.close()
            res.append(result)
        if found_errors:
            raise Exception(res)
        return res

    def seen_moved(self):
        if self._seen_moved:
            self._seen_moved = False
            return True
        return False

    def allow_multi(self, allow):
        self._allow_multi = allow
