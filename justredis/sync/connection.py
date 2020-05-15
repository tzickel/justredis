from ..decoder import RedisRespDecoder, RedisResp2Decoder, need_more_data, Error
from ..encoder import RedisRespEncoder
from .sockets import SyncSocketWrapper
from ..errors import CommunicationError


# TODO different encoder / decoder ?
# TODO better ERROR result handling
# TODO select database?
class SyncConnection:
    def __init__(self, username=None, password=None, client_name=None, resp_version=-1, socket_factory=SyncSocketWrapper, **kwargs):
        try:
            self._socket = socket_factory(**kwargs)
        except Exception as e:
            raise CommunicationError() from e
        self._encoder = RedisRespEncoder()
        self._decoder = RedisRespDecoder()
        self._seen_eof = False
        self._push_mode = False

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
            self._decoder = RedisResp2Decoder()
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

    def closed(self):
        return self._socket == None

    def _send(self, *cmd, multiple=False):
        if multiple:
            for _cmd in cmd:
                self._encoder.encode(*_cmd)
        else:
            self._encoder.encode(*cmd)
        while True:
            data = self._encoder.extract()
            if data is None:
                break
            try:
                self._socket.send(data)
            except Exception as e:
                self.close()
                raise CommunicationError() from e

    def _recv(self, timeout=False):
        while True:
            res = self._decoder.extract()
            if res == need_more_data:
                if self._seen_eof:
                    self.close()
                    raise CommunicationError() from EOFError('Connection reached EOF')
                else:
                    try:
                        data = self._socket.recv(timeout)
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

    def push_command(self, *cmd):
        self._push_mode = True
        self._send(*cmd)

    def no_more_push_command(self):
        self._push_mode = False

    def __call__(self, *cmd):
        if not cmd:
            raise Exception()
        # TODO meh detection
        if isinstance(cmd[0], (tuple, list)):
            return self._commands(*cmd)
        else:
            return self._command(*cmd)

    def _command(self, *cmd):
        if self._push_mode:
            raise Exception()
        self._send(*cmd)
        res = self._recv()
        if isinstance(res, Error):
            raise res
        return res

    def _commands(self, *cmds):
        if self._push_mode:
            raise Exception()
        self._send(*cmds, multiple=True)
        res = []
        found_errors = False
        for _ in range(len(cmds)):
            result = self._recv()
            if isinstance(result, Error):
                found_errors = True
            res.append(result)
        if found_errors:
            raise Exception(res)
        return res
