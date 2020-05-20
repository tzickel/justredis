import pytest

from justredis import SyncRedis

# TODO (misc) in the future allow direct redis instances ?

def redis_with_client(dockerimage='redis', extraparams=''):
    import redis_server

    instance = redis_server.RedisServer(dockerimage=dockerimage, extraparams=extraparams)
    with SyncRedis(address=('localhost', instance.port)) as r:
        try:
            yield r
        finally:
            instance.close()


@pytest.fixture(params=["redis:6", "redis:5"])
def client(request):
    for item in redis_with_client(request.param):
        yield item


@pytest.fixture(params=["redis:6", "redis:5"])
def client_with_blah_password(request):
    for item in redis_with_client(request.param, extraparams='--requirepass blah'):
        yield item
