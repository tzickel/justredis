import pytest
from justredis import Redis, Error, CommunicationError


# TODO (misc) copy all of misc/example.py into here


def test_connection_error():
    with pytest.raises(CommunicationError):
        with Redis(address=('127.0.0.222', 11121)) as r:
            r("set", "a", "b")



def test_auth(client_with_blah_password):
    address = client_with_blah_password.endpoints()[0][0]
    # No password
    with Redis(address=address) as r:
        with pytest.raises(Error) as exc_info:
            r("set", "auth_a", "b")
        assert exc_info.value.args[0].startswith("NOAUTH ")

    # Wrong password
    with Redis(address=address, password="nop") as r:
        with pytest.raises(Error) as exc_info:
            r("set", "auth_a", "b")
        # Changes between Redis 5 and Redis 6
        assert exc_info.value.args[0].startswith("WRONGPASS ") or exc_info.value.args[0].startswith("ERR invalid password")

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
    r("set", "modify_database_a_0", "a")
    with r.modify(database=1).connection(key="a") as c:
        assert c("get", "modify_database_a_0") == None
        assert c("set", "modify_database_a_1", "a") == b"OK"


def test_modify_database_cluster(cluster_client):
    r = cluster_client
    r("set", "modify_database_cluster_a_0", "a")
    with pytest.raises(Error) as exc_info:
        with r.modify(database=1).connection(key="a") as c:
            assert c("get", "modify_database_a_0") == None
            assert c("set", "modify_database_a_1", "a") == b"OK"
    assert exc_info.value.args[0] == ("ERR SELECT is not allowed in cluster mode")


def test_notallowed(client):
    r = client
    with pytest.raises(Error) as exc_info:
        r("auth", "asd")
    assert exc_info.value.args[0].startswith("ERR ")


def test_some_encodings(client):
    r = client
    with pytest.raises(ValueError):
        r("set", "a", True)
    assert r("incrbyfloat", "float_check", 0.1) == b"0.1"
    with pytest.raises(ValueError):
        r("set", "a", [1, 2])
    r("set", "{check}_a", "a")
    r("set", "{check}_b", "b")
    assert r("get", "{check}_a", decoder="utf8") == "a"
    assert r("mget", "{check}_a", "{check}_b", decoder="utf8") == ["a", "b"]


def test_chunk_encoded_command(client):
    r = client
    assert r("set", "test_chunk_encoded_command_a", b"test_chunk_encoded_command_a" * 10 * 1024) == b"OK"
    assert r("get", "test_chunk_encoded_command_a") == b"test_chunk_encoded_command_a" * 10 * 1024
    assert r("mget", "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500, "test_chunk_encoded_command_a" * 3500) == [None, None, None]


def test_eval(client):
    r = client
    assert r("set", "evaltest", "a") == b"OK"
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None
    assert r("script", "flush") == b"OK"
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltest") == b"a"
    assert r("eval", "return redis.call('get',KEYS[1])", 1, "evaltestno") == None


def test_pipeline(client):
    r = client
    assert r(("set", "abc", "def"), ("get", "abc")) == [b"OK", b"def"]


# TODO (misc) add some extra checks here for invalid states
def test_multi(client):
    r = client
    with r.connection(key="a") as c:
        c("multi")
        c("set", "a", "b")
        c("get", "a")
        assert c("exec") == [b"OK", b"b"]

    # TODO (misc) kinda lame
    try:
        with r.modify(database=2).connection(key="a") as c1:
            c1("multi")
            c1("set", "a", "b")
            with r.modify(database=3).connection(key="a") as c2:
                c2("multi")
                c2("set", "a1", "c")
                c2("get", "a")
                assert c2("exec") == [b"OK", None]
            c1("mget", "a", "a1")
            assert c1("exec") == [b"OK", [b"b", None]]
        assert r.modify(database=2)("get", "a") == b"b"
    except Error as e:
        if e.args[0] == "ERR SELECT is not allowed in cluster mode":
            pass
        else:
            raise


def test_multidiscard(client):
    r = client
    with r.connection(key="a") as c:
        c("multi")
        with pytest.raises(Error):
            c("nothing")
        c("discard")
        c("multi")
        c("set", "a", "b")
        assert c("exec") == [b"OK"]


def test_pubsub(client):
    r = client
    with r.connection(push=True) as p:
        p("subscribe", "hi")
        p("psubscribe", b"bye")
        assert p.next_message() == [b"subscribe", b"hi", 1]
        assert p.next_message() == [b"psubscribe", b"bye", 2]
        assert p.next_message(timeout=0.1) == None
        r("publish", "hi", "there")
        assert p.next_message(timeout=0.1) == [b"message", b"hi", b"there"]
        r("publish", "bye", "there")
        assert p.next_message(timeout=0.1) == [b"pmessage", b"bye", b"bye", b"there"]
        p("ping")
        # RESP2 and RESP3 behave differently here, so check for both
        assert p.next_message() in (b"PONG", [b"pong", b""])
        p("ping", b"hi")
        assert p.next_message() in (b"hi", [b"pong", b"hi"])
        p("unsubscribe", "hi")


def test_misc(client):
    r = client
    # This tests an command which redis server says keys start in index 2.
    r("object", "help")
    # Check command with no keys
    r("client", "list")


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
    assert len([x for x in result if isinstance(x, Error) and x.args[0].startswith("MOVED ")]) == 2
