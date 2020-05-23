class ThreadedEnvironment:
    @staticmethod
    def socket():
        from socket import socket

        return socket

    @staticmethod
    def semaphore():
        from threading import Semaphore

        return Semaphore

    @staticmethod
    def lock():
        from threading import Lock

        return Lock() # ASYNC


def get_environment(environment=ThreadedEnvironment, **kargs):
    return environment
