import socket
import sys


# TODO (correctness) timeout handling in recv should be done here !!!!!


platform = ""
if sys.platform.startswith("linux"):
    platform = "linux"
elif sys.platform.startswith("darwin"):
    platform = "darwin"
elif sys.platform.startswith("win"):
    platform = "windows"


def tcpsocket(address=None, connect_timeout=None, socket_timeout=None, tcp_keepalive=None, tcp_nodelay=True, **kwargs):
    import socket

    if address is None:
        address = ("localhost", 6379)
    sock = socket.create_connection(address, connect_timeout)  # AWAIT
    sock.settimeout(socket_timeout)

    if tcp_nodelay is not None:
        if tcp_nodelay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        else:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
    if tcp_keepalive:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if platform == "linux":
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcp_keepalive)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcp_keepalive // 3)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        elif platform == "darwin":
            sock.setsockopt(socket.IPPROTO_TCP, 0x10, tcp_keepalive // 3)
        elif platform == "windows":
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, tcp_keepalive * 1000, tcp_keepalive // 3 * 1000))
    return sock


def unixsocket(address=None, connect_timeout=None, socket_timeout=None, **kwargs):
    import socket

    if address is None:
        address = "/tmp/redis.sock"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(connect_timeout)
    sock.connect(address)  # AWAIT
    sock.settimeout(socket_timeout)
    return sock


# TODO how do cluster hostname work with SSL ?
# TODO does closing this also close the socket itself ?
def sslsocket(address=None, ssl_context=None, **kwargs):
    import ssl

    if address is None:
        address = ("localhost", 6379)
    sock = tcpsocket(address=address, **kwargs)
    if ssl_context is None:
        ssl_context = ssl.create_default_context()
    return ssl_context.wrap_socket(sock, server_hostname=address[0])


class SocketWrapper:
    def __init__(self, socket_factory, buffersize=2**16, **kwargs):
        self._buffer = bytearray(buffersize)
        self._view = memoryview(self._buffer)
        self._socket = socket_factory(**kwargs)

    def close(self):
        self._socket.close()

    def send(self, data):
        self._socket.sendall(data)  # AWAIT

    def recv(self, timeout=False):
        if timeout is not False:
            old_timeout = self._socket.gettimeout()
            self._socket.settimeout(timeout)
            try:
                r = self._socket.recv_into(self._buffer)  # AWAIT
            finally:
                self._socket.settimeout(old_timeout)
        else:
            r = self._socket.recv_into(self._buffer)  # AWAIT
        return self._view[:r]

    def peername(self):
        peername = self._socket.getpeername()
        # TODO (misc) is there a lib where this is not the case ?, we can also just return the peername in the connect functions.
        if self._socket.family == socket.AF_INET6:
            peername = peername[:2]
        return peername


class ThreadedEnvironment:
    @staticmethod
    def socket(socket_type="tcp", **kwargs):
        if socket_type == "tcp":
            socket_type = tcpsocket
        elif socket_type == "unix":
            socket_type = unixsocket
        elif socket_type == "ssl":
            socket_type = sslsocket
        else:
            raise NotImplementedError("Unknown socket type: %s" % socket_type)
        return SocketWrapper(socket_type, **kwargs)

    @staticmethod
    def semaphore():
        from threading import Semaphore

        class OurSemaphore:
            def __init__(self, value=None):
                self._semaphore = Semaphore(value)

            def release(self):
                self._semaphore.release()

            def acquire(self, blocking=True, timeout=None):
                self._semaphore.acquire(blocking, timeout) # AWAIT

        return OurSemaphore()

    @staticmethod
    def lock():
        from threading import Lock

        class OurLock:
            def __init__(self):
                self._lock = Lock()

            def __enter__(self):
                self._lock.acquire()  # AWAIT

            def __exit__(self, *args):
                self._lock.release()  # AWAIT

        return OurLock()
