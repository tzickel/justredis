class ThreadedEnvironment:
    @staticmethod
    def socket():
        from socket import socket

        return socket

    @staticmethod
    def semaphore():
        from threading import Semaphore

        class OurSemaphore:
            def __init__(self, value=None):
                self._semaphore = Semaphore(value)

            def release(self):
                self._semaphore.release()

            def acquire(self, blocking=True, timeout=None):
                self._semaphore.acquire(blocking, timeout)

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


def get_environment(environment=ThreadedEnvironment, **kargs):
    return environment
