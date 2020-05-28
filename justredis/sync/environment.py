import socket

from .environments.threaded import ThreadedEnvironment

# TODO (misc) is there a lib where this is not the case ?
SocketTimeout = socket.timeout


def get_environment(environment=ThreadedEnvironment, **kargs):
    return environment
