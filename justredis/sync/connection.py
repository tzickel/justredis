import socket


from ..decoder import RedisRespDecoder, RedisResp2Decoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from ..errors import CommunicationError
from ..utils import get_command_name, is_multiple_commands, get_command
from .sockets import SyncSocketWrapper, SyncUnixDomainSocketWrapper, SyncSslSocketWrapper


not_allowed_commands = b'MONITOR', b'SUBSCRIBE', b'PSUBSCRIBE', b'UNSUBSCRIBE', b'PUNSUBSCRIBE'


class TimeoutError(Exception):
    pass


timeout_error = TimeoutError()


class SyncConnection:
    def __init__(self, username=None, password=None, client_name=None, resp_version=-1, socket_factory=SyncSocketWrapper, connect_retry=2, **kwargs):
        if socket_factory == 'unix':
            socket_factory = SyncUnixDomainSocketWrapper
        elif socket_factory == 'ssl':
            socket_factory = SyncSslSocketWrapper
        connect_retry += 1
        while connect_retry:
            try:
                self._socket = socket_factory(**kwargs)
                break
            except Exception as e:
                connect_retry -= 1
                if not connect_retry:
                    raise CommunicationError() from e
        self._encoder = RedisRespEncoder(**kwargs)
        self._decoder = RedisRespDecoder(**kwargs)
        self._seen_eof = False
        self._peername = self._socket.peername()

        self._last_database = 0

        connected = False
        if resp_version not in (-1, 2, 3):
            raise Exception('Unsupported RESP protocol version %s' % resp_version)
        if resp_version != 2:
            args = [b'HELLO', b'3']
            if password:
                if username:
                    args.extend((b'AUTH', username, password))
                else:
                    args.extend((b'AUTH', b'default', password))
            if client_name:
                args.extend((b'SETNAME', client_name))
            try:
                # TODO (misc) do something with the result ?
                self._command(*args)
                connected = True
            except Error as e:
                # TODO (misc) what to do about error encoding ?
                # This is to seperate an login error from the server not supporting RESP3
                if e.args[0].startswith(b'ERR'):
                    if resp_version == 3:
                        raise Exception('Server does not support RESP3 protocol')
                else:
                    raise
        if not connected:
            self._decoder = RedisResp2Decoder(**kwargs)
            if password:
                if username:
                    self._command(b'AUTH', username, password)
                else:
                    self._command(b'AUTH', password)
            if client_name:
                self._command(b'CLIENT', b'SETNAME', client_name)

    def __del__(self):
        self.close()

    def close(self):
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

    def _send(self, *cmd, multiple=False, encoder=None):
        try:
            if multiple:
                for _cmd in cmd:
                    self._encoder.encode(*_cmd[0], encoder=_cmd[1])
            else:
                self._encoder.encode(*cmd, encoder=encoder)
            while True:
                data = self._encoder.extract()
                if data is None:
                    break
                try:
                    self._socket.send(data)
                except Exception as e:
                    raise CommunicationError() from e
        # TODO BaseException ?
        except Exception:
            self.close()
            raise

    # TODO (misc) should a decoding error be considered an CommunicationError ?
    def _recv(self, timeout=False, decoder=None, attributes=None):
        try:
            while True:
                res = self._decoder.extract(decoder=decoder, with_attributes=attributes)
                if res == need_more_data:
                    if self._seen_eof:
                        self.close()
                        raise EOFError('Connection reached EOF')
                    else:
                        data = self._socket.recv(timeout)
                        if data == b'':
                            self._seen_eof = True
                        else:
                            self._decoder.feed(data)
                    continue
                return res
        except socket.timeout:
            return timeout_error
        except Exception as e:
            self.close()
            raise CommunicationError() from e

    def pushed_message(self, timeout=False, decoder=None):
        res = self._recv(timeout, decoder)
        if res == timeout_error:
            return None
        return res

    # TODO (api) should have encoding as well ?
    # TODO (misc) don't accept multiple commands here
    def push_command(self, *cmd):
        self._send(*cmd)

    def set_database(self, database):
        if database != self._last_database:
            self._command(b'SELECT', database)
            self._last_database = database

    # TODO (correctness) if we see SELECT we should update it manually !
    # TODO handle **kwargs database, what else ?
    def __call__(self, *cmd, database=0, **kwargs):
        if not cmd:
            raise Exception()
        self.set_database(database)
        if is_multiple_commands(*cmd):
            return self._commands(*cmd)
        else:
            return self._command(*cmd)

    def _command(self, *cmd):
        cmd, encoder, decoder, attributes = get_command(*cmd)
        if get_command_name(cmd) in not_allowed_commands:
            raise Exception('Command %s is not allowed to be called directly, use the appropriate API instead' % cmd)
        self._send(*cmd, encoder=encoder)
        res = self._recv(decoder=decoder, attributes=attributes)
        if isinstance(res, Error):
            raise res
        if res == timeout_error:
            self.close()
            raise TimeoutError()
        return res

    def _commands(self, *cmds):
        send = []
        recv = []
        for cmd in cmds:
            cmd, encoder, decoder, attributes = get_command(*cmd)
            if get_command_name(cmd) in not_allowed_commands:
                raise Exception('Command %s is not allowed to be called directly, use the appropriate API instead' % cmd)
            send.append((cmd, encoder))
            recv.append((decoder, attributes))
        # TODO (correctness) check if I need to pass here *send or send
        self._send(send, multiple=True)
        res = []
        found_errors = False
        # TODO (misc) on error, return a partial error ?
        for _recv in recv:
            result = self._recv(decoder=_recv[0], attributes=_recv[1])
            if isinstance(result, Error):
                found_errors = True
            if result == timeout_error:
                self.close()
            res.append(result)
        if found_errors:
            raise Exception(res)
        return res
