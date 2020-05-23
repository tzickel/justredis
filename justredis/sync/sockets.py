import socket
import sys
import ssl


platform = ''
if sys.platform.startswith('linux'):
    platform = 'linux'
elif sys.platform.startswith('darwin'):
    platform = 'darwin'
elif sys.platform.startswith('win'):
    platform = 'windows'


class SyncSocketWrapper:
    def __init__(self, buffersize=2**16, **kwargs):
        self._buffer = bytearray(buffersize)
        self._view = memoryview(self._buffer)
        self._create(**kwargs)

    def close(self):
        self._socket.close()

    def _create(self, address=None, connect_timeout=None, socket_timeout=None, tcp_keepalive=None, tcp_nodelay=None, **kwargs):
        if address is None:
            address = ('localhost', 6379)
        sock = socket.create_connection(address, connect_timeout)
        sock.settimeout(socket_timeout)

        if tcp_nodelay is not None:
            if tcp_nodelay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            else:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
        if tcp_keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if platform == 'linux':
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcp_keepalive)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcp_keepalive // 3)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            elif platform == 'darwin':
                sock.setsockopt(socket.IPPROTO_TCP, 0x10, tcp_keepalive // 3)
            elif platform == 'windows':
                sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, tcp_keepalive * 1000, tcp_keepalive // 3 * 1000))
        self._socket = sock

    def send(self, data):
        #self._socket.sendmsg(data)
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

    def peername(self):
        peername = self._socket.getpeername()
        if self._socket.family == socket.AF_INET6:
            peername = peername[:2]
        return peername


class SyncUnixDomainSocketWrapper(SyncSocketWrapper):
    def _create(self, address=None, connect_timeout=None, socket_timeout=None, **kwargs):
        if address is None:
            address = '/tmp/redis.sock'
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.settimeout(connect_timeout)
        self._socket.connect(address)
        self._socket.settimeout(socket_timeout)


# TODO Older python's might not have recv_into and I'll need to implment this from scratch
# TODO how do cluster hostname work with SSL ?
# TODO does closing this also close the socket itself ?
class SyncSslSocketWrapper(SyncSocketWrapper):
    def _create(self, address=None, ssl_context=None, **kwargs):
        super(SyncSslSocketWrapper, self).__init__(address=address, **kwargs)
        if address is None:
            address = ('localhost', 6379)
        if ssl_context is None:
            ssl_context = ssl.create_default_context()
        self._socket = ssl_context.wrap_socket(self._socket, server_hostname=address[0])
