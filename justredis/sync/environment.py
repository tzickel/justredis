ThreadedEnvironment = None


def get_environment(environment=None, **kargs):
    global ThreadedEnvironment

    if not environment or environment == "threaded":
        if ThreadedEnvironment is None:
            from .environments.threaded import ThreadedEnvironment
        environment = ThreadedEnvironment
    else:
        raise NotImplementedError("Environment not supported: %r" % environment)

    return environment
