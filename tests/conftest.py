import pytest


# TODO in the future allow direct redis instances ?

def redis(dockerimage='redis', extraparams=''):
    import redis_server

    instance = redis_server.RedisServer(dockerimage=dockerimage, extraparams=extraparams)
    try:
        yield 'localhost', instance.port
    finally:
        instance.close()


@pytest.fixture
def redis6():
    for item in redis('redis:6'):
        yield item


@pytest.fixture
def redis5():
    for item in redis('redis:5'):
        yield item


@pytest.fixture
def redis6_with_blah_passwd():
    for item in redis('redis:6', extraparams='--requirepass blah'):
        yield item


@pytest.fixture
def redis5_with_blah_passwd():
    for item in redis('redis:5', extraparams='--requirepass blah'):
        yield item


redis_latest = redis6
