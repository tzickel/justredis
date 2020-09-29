try:
    import anyio
except:
    pass
import pytest
from justredis import AsyncRedis, Error, CommunicationError


@pytest.mark.anyio
async def test_connection_error():
    with pytest.raises(CommunicationError):
        async with AsyncRedis(address=("127.0.0.222", 11121)) as r:
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


@pytest.mark.anyio
async def test_modify_database(no_cluster_client):
    r = no_cluster_client
    await r("set", "modify_database_a_0", "a")
    # TODO (api) is this ok ?
    async with await r.modify(database=1).connection(key="a") as c:
        assert await c("get", "modify_database_a_0") == None
        assert await c("set", "modify_database_a_1", "a") == b"OK"


@pytest.mark.anyio
async def test_modify_database_cluster(cluster_client):
    r = cluster_client
    await r("set", "modify_database_cluster_a_0", "a")
    with pytest.raises(Error) as exc_info:
        async with await r.modify(database=1).connection(key="a") as c:
            assert await c("get", "modify_database_a_0") == None
            assert await c("set", "modify_database_a_1", "a") == b"OK"
    assert exc_info.value.args[0] == ("ERR SELECT is not allowed in cluster mode")


@pytest.mark.anyio
async def test_notallowed(client):
    r = client
    with pytest.raises(Error) as exc_info:
        await r("auth", "asd")
    assert exc_info.value.args[0].startswith("ERR ")


@pytest.mark.anyio
async def test_some_encodings(client):
    r = client
    with pytest.raises(ValueError):
        await r("set", "a", True)
    assert await r("incrbyfloat", "float_check", 0.1) == b"0.1"
    with pytest.raises(ValueError):
        await r("set", "a", [1, 2])
    await r("set", "{check}_a", "a")
    await r("set", "{check}_b", "b")
    assert await r("get", "{check}_a", decoder="utf8") == "a"
    assert await r("mget", "{check}_a", "{check}_b", decoder="utf8") == ["a", "b"]


@pytest.mark.anyio
async def test_chunk_encoded_command(client):
    r = client
    assert await r("set", "test_chunk_encoded_command_a", b"test_chunk_encoded_command_a" * 10 * 1024) == b"OK"
    assert await r("get", "test_chunk_encoded_command_a") == b"test_chunk_encoded_command_a" * 10 * 1024
    assert await r("mget", "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500) == [None, None, None]


@pytest.mark.anyio
async def test_eval(client):
    r = client
    assert await r("set", "evaltest", "a") == b"OK"
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert await r("script", "flush") == b"OK"
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert await r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None


@pytest.mark.anyio
async def test_pipeline(client):
    r = client
    assert await r(("set", "abc", "def"), ("get", "abc")) == [b"OK", b"def"]


# TODO (misc) add some extra checks here for invalid states
@pytest.mark.anyio
async def test_multi(client):
    r = client
    async with await r.connection(key="a") as c:
        await c("multi")
        await c("set", "a", "b")
        await c("get", "a")
        assert await c("exec") == [b"OK", b"b"]

    # TODO (misc) kinda lame
    try:
        async with await r.modify(database=2).connection(key="a") as c1:
            await c1("multi")
            await c1("set", "a", "b")
            async with await r.modify(database=3).connection(key="a") as c2:
                await c2("multi")
                await c2("set", "a1", "c")
                await c2("get", "a")
                assert await c2("exec") == [b"OK", None]
            await c1("mget", "a", "a1")
            assert await c1("exec") == [b"OK", [b"b", None]]
        assert await r.modify(database=2)("get", "a") == b"b"
    except Error as e:
        if e.args[0] == "ERR SELECT is not allowed in cluster mode":
            pass
        else:
            raise


@pytest.mark.anyio
async def test_multidiscard(client):
    r = client
    async with await r.connection(key="a") as c:
        await c("multi")
        with pytest.raises(Error):
            await c("nothing")
        await c("discard")
        await c("multi")
        await c("set", "a", "b")
        assert await c("exec") == [b"OK"]


@pytest.mark.anyio
async def test_pubsub(client):
    r = client
    async with await r.connection(push=True) as p:
        await p("subscribe", "hi")
        await p("psubscribe", b"bye")
        assert await p.next_message() == [b"subscribe", b"hi", 1]
        assert await p.next_message() == [b"psubscribe", b"bye", 2]
        assert await p.next_message(timeout=0.1) == None
        await r("publish", "hi", "there")
        assert await p.next_message(timeout=0.1) == [b"message", b"hi", b"there"]
        await r("publish", "bye", "there")
        assert await p.next_message(timeout=0.1) == [b"pmessage", b"bye", b"bye", b"there"]
        await p("ping")
        # RESP2 and RESP3 behave differently here, so check for both
        assert await p.next_message() in (b"PONG", [b"pong", b""])
        await p("ping", b"hi")
        assert await p.next_message() in (b"hi", [b"pong", b"hi"])
        await p("unsubscribe", "hi")


@pytest.mark.anyio
async def test_misc(client):
    r = client
    # This tests an command which redis server says keys start in index 2.
    await r("object", "help")
    # Check command with no keys
    await r("client", "list")


@pytest.mark.anyio
async def test_server_no_cluster(no_cluster_client):
    r = no_cluster_client
    await r("set", "cluster_aa", "a") == b"OK"
    await r("set", "cluster_bb", "b") == b"OK"
    await r("set", "cluster_cc", "c") == b"OK"
    result = await r("keys", "cluster_*", endpoint="masters")
    result = list(result.values())
    result = [i for s in result for i in s]
    assert set(result) == set([b"cluster_aa", b"cluster_bb", b"cluster_cc"])


@pytest.mark.anyio
async def test_server_cluster(cluster_client):
    r = cluster_client
    # TODO (misc) split keys to 3 comps
    await r("set", "cluster_aa", "a") == b"OK"
    await r("set", "cluster_bb", "b") == b"OK"
    await r("set", "cluster_cc", "c") == b"OK"
    result = await r("keys", "cluster_*", endpoint="masters")
    assert len(result) == 3
    result = list(result.values())
    result = [i for s in result for i in s]
    assert set(result) == set([b"cluster_aa", b"cluster_bb", b"cluster_cc"])


@pytest.mark.anyio
async def test_moved_no_cluster(no_cluster_client):
    r = no_cluster_client
    await r("set", "aa", "a") == b"OK"
    await r("set", "bb", "b") == b"OK"
    await r("set", "cc", "c") == b"OK"
    result = await r("get", "aa", endpoint="masters")
    result = list(result.values())
    assert result == [b"a"]


@pytest.mark.anyio
async def test_moved_cluster(cluster_client):
    r = cluster_client
    await r("set", "aa", "a") == b"OK"
    await r("set", "bb", "b") == b"OK"
    await r("set", "cc", "c") == b"OK"
    assert await r("get", "aa") == b"a"
    assert await r("get", "bb") == b"b"
    assert await r("get", "cc") == b"c"
    result = await r("get", "aa", endpoint="masters")
    result = list(result.values())
    assert b"a" in result
    assert len([x for x in result if isinstance(x, Error) and x.args[0].startswith("MOVED ")]) == 2


@pytest.mark.anyio
async def test_cancel(client):
    r = client

    async with anyio.create_task_group() as tg:
        await tg.spawn(r, "blpop", "a", 20)
        await anyio.sleep(1)
        await tg.cancel_scope.cancel()

    await anyio.sleep(1)
    async with anyio.create_task_group() as tg:
        await tg.spawn(r, "blpop", "a", 1)
