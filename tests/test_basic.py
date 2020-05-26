import pytest
from justredis import Redis, Error


# TODO (misc) copy all of misc/example.py into here


def test_auth(client_with_blah_password):
    address = client_with_blah_password.endpoints()[0][0]
    # No password
    with Redis(address=address) as r:
        with pytest.raises(Error) as exc_info:
            r("set", "auth_a", "b")
        assert exc_info.value.args[0].startswith(b"NOAUTH")

    # Wrong password
    with Redis(address=address, password="nop") as r:
        with pytest.raises(Error) as exc_info:
            r("set", "auth_a", "b")
        # Changes between Redis 5 and Redis 6
        assert exc_info.value.args[0].startswith(b"WRONGPASS") or exc_info.value.args[0].startswith(b"ERR invalid password")

    # Correct password
    with Redis(address=address, password="blah") as r:
        assert r("set", "auth_a", "b") == b"OK"


def test_simple(client):
    r = client
    assert r("set", "simple_a", "a") == b"OK"
    assert r("set", "simple_b", "b") == b"OK"
    assert r("set", "simple_c", "c") == b"OK"
    assert r("set", "simple_{a}b", "d") == b"OK"
    assert r("get", "simple_a") == b"a"
    assert r("get", "simple_b") == b"b"
    assert r("get", "simple_c") == b"c"
    assert r("get", "simple_{a}b") == b"d"


def test_modify_database(no_cluster_client):
    r = no_cluster_client
    r('set', 'modify_database_a_0', 'a')
    with r.modify(database=1).connection(key="a") as c:
        assert c('get', 'modify_database_a_0') == None
        assert c("set", "modify_database_a_1", "a") == b"OK"


def test_modify_database_cluster(cluster_client):
    r = cluster_client
    r('set', 'modify_database_cluster_a_0', 'a')
    with pytest.raises(Error) as exc_info:
        with r.modify(database=1).connection(key="a") as c:
            assert c('get', 'modify_database_a_0') == None
            assert c("set", "modify_database_a_1", "a") == b"OK"
    assert exc_info.value.args[0] == (b"ERR SELECT is not allowed in cluster mode")


def test_notallowed(client):
    with pytest.raises(Error) as exc_info:
        client("auth", "asd")
    assert exc_info.value.args[0].startswith(b"ERR")


def test_some_encodings(client):
    with pytest.raises(ValueError):
        client("set", "a", True)
    assert client("incrbyfloat", "float_check", 0.1) == b"0.1"
    with pytest.raises(ValueError):
        client("set", "a", [1, 2])
    client("set", "{check}_a", "a")
    client("set", "{check}_b", "b")
    assert client({"command": ("get", "{check}_a"), "decoder": "utf8"}) == "a"
    assert client({"command": ("mget", "{check}_a", "{check}_b"), "decoder": "utf8"}) == ["a", "b"]


def test_chunk_encoded_command(client):
    assert client("set", "test_chunk_encoded_command_a", b"test_chunk_encoded_command_a" * 10 * 1024) == b"OK"
    assert client("get", "test_chunk_encoded_command_a") == b"test_chunk_encoded_command_a" * 10 * 1024
    assert client("mget", "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500) == [None, None, None]


def test_eval(client):
    assert client("set", "evaltest", "a") == b"OK"
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert client("script", "flush") == b"OK"
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert client("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None


# TODO (misc) add some extra checks here for invalid states
def test_multi(client):
    with client.connection(key="a") as c:
        c("multi")
        c("set", "a", "b")
        c("get", "a")
        assert c("exec") == [b"OK", b"b"]

    # TODO (misc) kinda lame
    try:
        with client.modify(database=2).connection(key="a") as c1:
            c1("multi")
            c1("set", "a", "b")
            with client.modify(database=3).connection(key="a") as c2:
                c2("multi")
                c2("set", "a1", "c")
                c2("get", "a")
                assert c2("exec") == [b"OK", None]
            c1("mget", "a", "a1")
            assert c1("exec") == [b"OK", [b"b", None]]
        assert client.modify(database=2)("get", "a") == b"b"
    except Error as e:
        if e.args[0] == b"ERR SELECT is not allowed in cluster mode":
            pass
        else:
            raise


def test_multidiscard(client):
    with client.connection(key="a") as c:
        c("multi")
        with pytest.raises(Error):
            c("nothing")
        c("discard")
        c("multi")
        c("set", "a", "b")
        assert c("exec") == [b"OK"]


def test_pubsub(client):
    with client.connection(push=True) as pubsub:
        pubsub("subscribe", "hi")
        pubsub("psubscribe", b"bye")
        assert pubsub.next_message() == [b"subscribe", b"hi", 1]
        assert pubsub.next_message() == [b"psubscribe", b"bye", 2]
        assert pubsub.next_message(timeout=0.1) == None
        client("publish", "hi", "there")
        assert pubsub.next_message(timeout=0.1) == [b"message", b"hi", b"there"]
        client("publish", "bye", "there")
        assert pubsub.next_message(timeout=0.1) == [b"pmessage", b"bye", b"bye", b"there"]
        pubsub("ping")
        # RESP2 and RESP3 behave differently here, so check for both
        assert pubsub.next_message() in (b"PONG", [b"pong", b""])
        pubsub("ping", b"hi")
        assert pubsub.next_message() in (b"hi", [b"pong", b"hi"])
        pubsub("unsubscribe", "hi")


def test_misc(client):
    # This tests an command which redis server says keys start in index 2.
    client("object", "help")
    # Check command with no keys
    client("client", "list")


def test_server_no_cluster(no_cluster_client):
    r = no_cluster_client
    r("set", "cluster_aa", "a") == b"OK"
    r("set", "cluster_bb", "b") == b"OK"
    r("set", "cluster_cc", "c") == b"OK"
    result = r("keys", "cluster_*", endpoint="masters")
    result = list(result.values())
    result = [i for s in result for i in s]
    assert set(result) == set([b"cluster_aa", b"cluster_bb", b"cluster_cc"])


def test_server_cluster(cluster_client):
    r = cluster_client
    # TODO split keys to 3 comps
    r("set", "cluster_aa", "a") == b"OK"
    r("set", "cluster_bb", "b") == b"OK"
    r("set", "cluster_cc", "c") == b"OK"
    result = r("keys", "cluster_*", endpoint="masters")
    assert len(result) == 3
    result = list(result.values())
    result = [i for s in result for i in s]
    assert set(result) == set([b"cluster_aa", b"cluster_bb", b"cluster_cc"])


def test_moved_no_cluster(no_cluster_client):
    r = no_cluster_client
    r("set", "aa", "a") == b"OK"
    r("set", "bb", "b") == b"OK"
    r("set", "cc", "c") == b"OK"
    result = r("get", "aa", endpoint="masters")
    result = list(result.values())
    assert result == [b"a"]


def test_moved_cluster(cluster_client):
    r = cluster_client
    r("set", "aa", "a") == b"OK"
    r("set", "bb", "b") == b"OK"
    r("set", "cc", "c") == b"OK"
    assert r("get", "aa") == b"a"
    assert r("get", "bb") == b"b"
    assert r("get", "cc") == b"c"
    result = r("get", "aa", endpoint="masters")
    result = list(result.values())
    assert b"a" in result
    assert len([x for x in result if isinstance(x, Error) and x.args[0].startswith(b"MOVED ")]) == 2
