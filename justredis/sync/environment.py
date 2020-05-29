from .environments.threaded import ThreadedEnvironment


def get_environment(environment=ThreadedEnvironment, **kargs):
    return environment
