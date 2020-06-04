from .environments.anyio import AnyIOEnvironment


def get_environment(environment=AnyIOEnvironment, **kargs):
    if environment == "anyio":
        environment = AnyIOEnvironment
    return environment
