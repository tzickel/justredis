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


def redis_cluster_with_client(dockerimage='redis', extraparams=''):
    import redis_server

    servers = redis_server.start_cluster(3, dockerimage=dockerimage, extraparams=extraparams)
    with SyncRedis(address=('localhost', servers[0].port)) as r:
        yield r


# TODO (misc) No better way to do it pytest ?
def generate_fixture_params(cluster=True):
    params = []
    versions = ('5', '6')
    for version in versions:
        if cluster:
            params.append(("redis:%s" % version, False))
            params.append(("redis:%s" % version, True))
        else:
            params.append("redis:%s" % version)
    return params


@pytest.fixture(scope="module", params=generate_fixture_params())
def client(request):
    if request.param[1]:
        for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(scope="module", params=generate_fixture_params(False))
def client_with_blah_password(request):
    for item in redis_with_client(request.param, extraparams='--requirepass blah'):
        yield item