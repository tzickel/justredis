from .environments.threaded import ThreadedEnvironment


def get_environment(environment=ThreadedEnvironment, **kargs):
    if environment == "threaded":
        environment = ThreadedEnvironment
    return environment
