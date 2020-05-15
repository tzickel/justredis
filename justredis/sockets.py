import socket


# TODO keepalive
class SocketWrapper:
    def __init__(self, address=None, timeout=None, keepalive=None, **kwargs):
        self._buffer = bytearray(2**16)
        self._view = memoryview(self._buffer)
        self._create(address, timeout)

    def close(self):
        self._socket.close()

    def _create(self, address, timeout):
        if address == None:
            address = ('localhost', 6379)
        self._socket = socket.create_connection(address, timeout)

    def send(self, data):
        self._socket.sendall(data)

    def recv(self, timeout=False):
        if timeout is not False:
            old_timeout = self._socket.gettimeout()
            self._socket.settimeout(timeout)
            try:
                r = self._socket.recv_into(self._buffer)
            finally:
                self._socket.settimeout(old_timeout)
        else:
            r = self._socket.recv_into(self._buffer)
        return self._view[:r]


class UnixDomainSocketWrapper(SocketWrapper):
    def _create(self, address, timeout):
        if address == None:
            address = ('/tmp/redis.sock')
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.connect(address)
