import pytest
import os
import warnings


# Disable async tests if anyio is not installed
try:
    import anyio
except:
    anyio = None


@pytest.mark.hookwrapper
def pytest_ignore_collect(path, config):
    outcome = yield
    if "async" in str(path) and anyio is None:
        warnings.warn("Skipping async tests because anyio is not installed")
        outcome.force_result(True)


from justredis import Redis

# TODO (misc) in the future allow direct redis instances ?
# TODO (misc) enforce REDIS_PATH as input, don't blindly accept it (so tests run twice)

def get_runtime_params_for_redis(dockerimage="redis"):
    redis_6_path = os.getenv("REDIS_6_PATH")
    redis_5_path = os.getenv("REDIS_5_PATH")
    if dockerimage in ("redis", "redis:6") and redis_6_path:
        return {"extrapath": redis_6_path}
    elif dockerimage == "redis:5" and redis_5_path:
        return {"extrapath": os.getenv("REDIS_5_PATH")}
    elif os.getenv("REDIS_USE_DOCKER"):
        return {"dockerimage": dockerimage}
    else:
        return {}


def redis_with_client(dockerimage="redis", extraparams="", **kwargs):
    from . import redis_server

    if isinstance(dockerimage, (tuple, list)):
        dockerimage = dockerimage[0]
    instance = redis_server.RedisServer(extraparams=extraparams, **get_runtime_params_for_redis(dockerimage))
    with Redis(address=("localhost", instance.port), resp_version=-1, **kwargs) as r:
        try:
            yield r
        finally:
            instance.close()


def redis_cluster_with_client(dockerimage="redis", extraparams=""):
    from . import redis_server

    if isinstance(dockerimage, (tuple, list)):
        dockerimage = dockerimage[0]
    servers, stdout = redis_server.start_cluster(3, extraparams=extraparams, **get_runtime_params_for_redis(dockerimage))
    try:
        with Redis(address=("localhost", servers[0].port), resp_version=-1) as r:
            import time

            wait = 60
            while wait:
                result = r(b"CLUSTER", b"INFO", endpoint="masters")
                ready = True
                for res in result.values():
                    if isinstance(res, Exception):
                        raise res
                    if b"cluster_state:ok" not in res:
                        ready = False
                        break
                if ready:
                    break
                time.sleep(1)
                wait -= 1
            if not wait:
                raise Exception("Cluster is down, could not run test")
            yield r
    finally:
        for server in servers:
            server.close()


# TODO (misc) No better way to do it pytest ?
def generate_fixture_params(cluster=True):
    params = []
    versions = ("5", "6")
    for version in versions:
        if cluster != "only":
            params.append(("redis:%s" % version, False))
        if cluster:
            params.append(("redis:%s" % version, True))
    return params


@pytest.fixture(scope="module", params=generate_fixture_params())
def client(request):
    if request.param[1]:
        for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(scope="module", params=generate_fixture_params("only"))
def cluster_client(request):
    if request.param[1]:
        for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(scope="module", params=generate_fixture_params(False))
def no_cluster_client(request):
    if request.param[1]:
        for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(scope="module", params=generate_fixture_params())
def client_with_blah_password(request):
    for item in redis_with_client(request.param, extraparams="--requirepass blah", password="blah"):
        yield item
