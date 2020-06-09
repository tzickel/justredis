import pytest
from justredis import AsyncRedis, Error, CommunicationError


@pytest.mark.anyio
async def test_connection_error():
    with pytest.raises(CommunicationError):
        async with AsyncRedis(address=('127.0.0.222', 11121)) as r:
            await r("set", "a", "b")


@pytest.mark.anyio
async def test_auth(client_with_blah_password):
    address = (await client_with_blah_password.endpoints())[0][0]
    # No password
    async with AsyncRedis(address=address) as r:
        with pytest.raises(Error) as exc_info:
            await r("set", "auth_a", "b")
        assert exc_info.value.args[0].startswith("NOAUTH ")

    # Wrong password
    async with AsyncRedis(address=address, password="nop") as r:
        with pytest.raises(Error) as exc_info:
            await r("set", "auth_a", "b")
        # Changes between Redis 5 and Redis 6
        assert exc_info.value.args[0].startswith("WRONGPASS ") or exc_info.value.args[0].startswith("ERR invalid password")

    # Correct password
    async with AsyncRedis(address=address, password="blah") as r:
        assert await r("set", "auth_a", "b") == b"OK"


@pytest.mark.anyio
async def test_simple(client):
    r = client
    assert await r("set", "simple_a", "a") == b"OK"
    assert await r("set", "simple_b", "b") == b"OK"
    assert await r("set", "simple_c", "c") == b"OK"
    assert await r("set", "simple_{a}b", "d") == b"OK"
    assert await r("get", "simple_a") == b"a"
    assert await r("get", "simple_b") == b"b"
    assert await r("get", "simple_c") == b"c"
    assert await r("get", "simple_{a}b") == b"d"
