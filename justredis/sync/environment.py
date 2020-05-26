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

        class OurLock:
            def __init__(self):
                self._lock = Lock()
            
            def __enter__(self):
                self._lock.acquire() # AWAIT

            def __exit__(self, *args):
                self._lock.release() # AWAIT

        return OurLock()


def get_environment(environment=ThreadedEnvironment, **kargs):
    return environment
