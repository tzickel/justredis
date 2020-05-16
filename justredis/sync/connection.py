import socket


from ..decoder import RedisRespDecoder, RedisResp2Decoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from ..errors import CommunicationError
from .sockets import SyncSocketWrapper, SyncUnixDomainSocketWrapper


# TODO better ERROR result handling
class SyncConnection:
    def __init__(self, username=None, password=None, client_name=None, resp_version=-1, socket_factory=SyncSocketWrapper, connect_retry=2, **kwargs):
        if socket_factory == 'unix':
            socket_factory = SyncUnixDomainSocketWrapper
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
        self._push_mode = False

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
                res = self._command(*args)
                connected = True
            except Error:
                # TODO not true, can be wrong password...
                if resp_version == 3:
                    raise Exception('Server does not support RESP3 protocol')
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

    # TODO better check ?
    def closed(self):
        return self._socket == None

    def peername(self):
        return self._socket.peername()

    def _send(self, *cmd, multiple=False, encoder=None):
        if multiple:
            for _cmd in cmd:
                self._encoder.encode(*_cmd, encoder=encoder)
        else:
            self._encoder.encode(*cmd, encoder=encoder)
        while True:
            data = self._encoder.extract()
            if data is None:
                break
            try:
                self._socket.send(data)
            except Exception as e:
                self.close()
                raise CommunicationError() from e

    def _recv(self, timeout=False, decoder=None):
        while True:
            res = self._decoder.extract(decoder=decoder)
            if res == need_more_data:
                if self._seen_eof:
                    self.close()
                    raise CommunicationError() from EOFError('Connection reached EOF')
                else:
                    try:
                        data = self._socket.recv(timeout)
                    # TODO put this somewhere else
                    # TODO this is wrong in conncetion pooling
                    except socket.timeout:
                        return None
                    except Exception as e:
                        self.close()
                        raise CommunicationError() from e
                    if data == b'':
                        self._seen_eof = True
                    else:
                        self._decoder.feed(data)
                continue
            return res

    def pushed_message(self, timeout=False):
        if not self._push_mode:
            raise Exception()
        return self._recv(timeout)

    # TODO should have encoding as well
    # TODO remove push_mode, handle it in a higher level ...
    def push_command(self, *cmd):
        self._push_mode = True
        self._send(*cmd)

    def no_more_push_command(self):
        self._push_mode = False

    def set_database(self, database):
        if database != self._last_database:
            self._command(b'SELECT', database)
            self._last_database = database

    # TODO if we see SELECT we should update it manually !
    def __call__(self, *cmd, database=0):
        if not cmd:
            raise Exception()
        self.set_database(database)
        # TODO meh detection
        if isinstance(cmd[0], (tuple, list)):
            return self._commands(*cmd)
        else:
            return self._command(*cmd)

    # TODO on grabage encoding, we need to kill this connection and start a new one !!!
    def _command(self, *cmd):
        if self._push_mode:
            raise Exception()
        if isinstance(cmd[0], dict):
            cmd = cmd[0]
            encoder = cmd.get('encoder', None)
            decoder = cmd.get('decoder', None)
            cmd = cmd['command']
        else:
            encoder = None
            decoder = None
        self._send(*cmd, encoder=encoder)
        res = self._recv(decoder=decoder)
        if isinstance(res, Error):
            raise res
        return res

    def _commands(self, *cmds):
        if self._push_mode:
            raise Exception()
        # TODO handle dictionary of multiple encoding / decoding here :()
        self._send(*cmds, multiple=True, encoder=encoder)
        res = []
        found_errors = False
        for _ in range(len(cmds)):
            result = self._recv(decoder=decoder)
            if isinstance(result, Error):
                found_errors = True
            res.append(result)
        if found_errors:
            raise Exception(res)
        return res
