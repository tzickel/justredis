AnyIOEnvironment = None
TrioEnvironment = None


def get_environment(environment=None, **kargs):
    global AnyIOEnvironment, TrioEnvironment

    if not environment or environment == "anyio":
        if not AnyIOEnvironment:
            from .environments.anyio import AnyIOEnvironment
        env = AnyIOEnvironment
    elif environment == "trio":
        if not TrioEnvironment:
            from .environments.trio import TrioEnvironment
        env = TrioEnvironment
    else:
        raise NotImplementedError("Unkown environment: %r" % environment)
    return env
