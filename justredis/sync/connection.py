from .environment import get_environment
from ..decoder import RedisRespDecoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from ..errors import CommunicationError, PipelinedExceptions
from ..utils import get_command_name, is_multiple_commands


not_allowed_push_commands = set([b"MONITOR", b"SUBSCRIBE", b"PSUBSCRIBE", b"UNSUBSCRIBE", b"PUNSUBSCRIBE", b"SELECT"])


class TimeoutError(Exception):
    pass


timeout_error = TimeoutError()


class Connection:
    # TODO (api) client_name with connection pool (?)
    # TODO (documentation) the username/password/client_name need the decoding of whatever **kwargs is passed
    def __init__(self, username=None, password=None, client_name=None, resp_version=2, socket_factory="tcp", connect_retry=2, database=0, **kwargs):
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
        self._seen_moved = False
        self._allow_multi = False

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
                if e.args[0].startswith(b"ERR "):
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

    def _send(self, *cmd):
        try:
            if is_multiple_commands(*cmd):
                self._encoder.encode_multiple(*cmd)
            else:
                self._encoder.encode(*cmd)
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

    def pushed_message(self):
        res = self._recv()
        if res == timeout_error:
            return None
        return res

    def push_command(self, *cmd):
        self._send(*cmd)

    def __call__(self, *cmd):
        if not cmd:
            raise ValueError("No command provided")
        if is_multiple_commands(*cmd):
            return self._commands(*cmd)
        else:
            return self._command(*cmd)

    def _command(self, *cmd):
        command_name = get_command_name(cmd)
        if command_name in not_allowed_push_commands:
            raise ValueError("Command %s is not allowed to be called directly, use the appropriate API instead" % cmd)
        if command_name == b"MULTI" and not self._allow_multi:
            raise ValueError("Take a connection if you want to use MULTI command.")
        self._send(*cmd)
        res = self._recv()
        if isinstance(res, Error):
            if res.args[0].startswith(b"MOVED "):
                self._seen_moved = True
            raise res
        if res == timeout_error:
            self.close()
            raise timeout_error
        return res

    def _commands(self, *cmds):
        for cmd in cmds:
            command_name = get_command_name(cmd)
            if command_name in not_allowed_push_commands:
                raise ValueError("Command %s is not allowed to be called directly, use the appropriate API instead" % cmd)
            if command_name == b"MULTI" and not self._allow_multi:
                raise ValueError("Take a connection if you want to use MULTI command.")
        self._send(*cmds)
        res = []
        found_errors = False
        for _recv in recv:
            try:
                result = self._recv()
                if isinstance(result, Error):
                    if result.args[0].startswith(b"MOVED "):
                        self.seen_moved = True
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

    def seen_moved(self):
        if self._seen_moved:
            self._seen_moved = False
            return True
        return False

    def allow_multi(self, allow):
        self._allow_multi = allow

    def is_valid(self):
        self._command(b"PING")
