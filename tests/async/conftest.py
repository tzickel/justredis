import os
import pytest

from justredis import AsyncRedis

# TODO (misc) in the future allow direct redis instances ?
# TODO (misc) enforce REDIS_PATH as input, don't blindly accept it (so tests run twice)


def get_runtime_params_for_redis(dockerimage="redis"):
    redis_6_path = os.getenv("REDIS_6_PATH")
    redis_5_path = os.getenv("REDIS_5_PATH")
    if dockerimage in ("redis", "redis:6") and redis_6_path:
        return {"extrapath": redis_6_path}
    elif dockerimage == "redis:5" and redis_5_path:
        return {"extrapath": os.getenv("REDIS_5_PATH")}
    elif os.getenv("REDIS_USE_DOCKER") is not None:
        return {"dockerimage": dockerimage}
    else:
        return {}


async def redis_with_client(dockerimage="redis", extraparams="", **kwargs):
    from .. import redis_server

    if isinstance(dockerimage, (tuple, list)):
        dockerimage = dockerimage[0]
    instance = redis_server.RedisServer(extraparams=extraparams, **get_runtime_params_for_redis(dockerimage))
    try:
        async with AsyncRedis(address=("localhost", instance.port), resp_version=-1, **kwargs) as r:
            yield r
    finally:
        instance.close()


async def redis_cluster_with_client(dockerimage="redis", extraparams=""):
    from .. import redis_server

    if isinstance(dockerimage, (tuple, list)):
        dockerimage = dockerimage[0]
    servers, stdout = redis_server.start_cluster(3, extraparams=extraparams, **get_runtime_params_for_redis(dockerimage))
    try:
        async with AsyncRedis(address=("localhost", servers[0].port), resp_version=-1) as r:
            import anyio

            wait = 60
            while wait:
                result = await r(b"CLUSTER", b"INFO", endpoint="masters")
                ready = True
                for res in result.values():
                    if isinstance(res, Exception):
                        raise res
                    if b"cluster_state:ok" not in res:
                        ready = False
                        break
                if ready:
                    break
                await anyio.sleep(1)
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


@pytest.fixture(params=generate_fixture_params())
async def client(request):
    if request.param[1]:
        async for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        async for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(params=generate_fixture_params("only"))
async def cluster_client(request):
    if request.param[1]:
        async for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        async for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(params=generate_fixture_params(False))
async def no_cluster_client(request):
    if request.param[1]:
        async for item in redis_cluster_with_client(request.param[0]):
            yield item
    else:
        async for item in redis_with_client(request.param[0]):
            yield item


@pytest.fixture(params=generate_fixture_params())
async def client_with_blah_password(request):
    async for item in redis_with_client(request.param, extraparams="--requirepass blah", password="blah"):
        yield item
