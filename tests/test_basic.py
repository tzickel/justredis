import pytest
from justredis import SyncRedis, Error


# TODO (misc) copy all of example.py into here
# TODO (misc) how to test with multiple version of server, and client settings


def test_auth(redis6_with_blah_passwd):
    # No password
    with SyncRedis(address=redis6_with_blah_passwd) as r:
        with pytest.raises(Error) as exc_info:
            r('set', 'a', 'b')
        assert exc_info.value.args[0].startswith(b'NOAUTH')

    # Wrong password
    with SyncRedis(address=redis6_with_blah_passwd, password='nop') as r:
        with pytest.raises(Error) as exc_info:
            r('set', 'a', 'b')
        assert exc_info.value.args[0].startswith(b'WRONGPASS')

    # Correct password
    with SyncRedis(address=redis6_with_blah_passwd, password='blah') as r:
        assert r('set', 'a', 'b') == b'OK'


def test_simple(redis6_with_client):
    assert redis6_with_client('set', 'a', 'b') == b'OK'
    assert redis6_with_client('get', 'a') == b'b'


def test_notallowed(redis6_with_client):
    with pytest.raises(Error) as exc_info:
        redis6_with_client('auth', 'asd')
    assert exc_info.value.args[0].startswith(b'ERR')


def test_some_encodings(redis6_with_client):
    with pytest.raises(ValueError):
        redis6_with_client('set', 'a', True)
    assert redis6_with_client('incrbyfloat', 'float_check', 0.1) == b'0.1'
    with pytest.raises(ValueError):
        redis6_with_client('set', 'a', [1, 2])
    redis6_with_client('set', 'check_a', 'a')
    redis6_with_client('set', 'check_b', 'b')
    assert redis6_with_client({'command': ('get', 'check_a'), 'decoder': 'utf8'}) == 'a'
    assert redis6_with_client({'command': ('mget', 'check_a', 'check_b'), 'decoder': 'utf8'}) == ['a', 'b']


def test_chunk_encoded_command(redis6_with_client):
    assert redis6_with_client('set', 'test_chunk_encoded_command_a', b'test_chunk_encoded_command_a'*10*1024) == b'OK'
    assert redis6_with_client('get', 'test_chunk_encoded_command_a') == b'test_chunk_encoded_command_a'*10*1024
    assert redis6_with_client('mget', 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500) == [None, None, None]


def test_eval(redis6_with_client):
    assert redis6_with_client('set', 'evaltest', 'a') == b'OK'
    assert redis6_with_client('eval', "return redis.call('get','evaltest')", 0) == b'a'
    assert redis6_with_client('eval', "return redis.call('get','evaltestno')", 0) == None
    assert redis6_with_client('eval', "return redis.call('get','evaltest')", 0) == b'a'
    assert redis6_with_client('eval', "return redis.call('get','evaltestno')", 0) == None
    assert redis6_with_client('script', 'flush') == b'OK'
    assert redis6_with_client('eval', "return redis.call('get','evaltest')", 0) == b'a'
    assert redis6_with_client('eval', "return redis.call('get','evaltestno')", 0) == None


# TODO (misc) add some extra checks here for invalid states
def test_multi(redis6_with_client):
    with redis6_with_client.connection('a') as c:
        c('multi')
        c('set', 'a', 'b')
        c('get', 'a')
        assert c('exec') == [b'OK', b'b']

    with redis6_with_client.database(2).connection('a') as c1:
        c1('multi')
        c1('set', 'a', 'b')
        with redis6_with_client.database(3).connection('a') as c2:
            c2('multi')
            c2('set', 'a1', 'c')
            c2('get', 'a')
            assert c2('exec') == [b'OK', None]
        c1('mget', 'a', 'a1')
        assert c1('exec') == [b'OK', [b'b', None]]
    assert redis6_with_client.database(2)('get', 'a') == b'b'


def test_multidiscard(redis6_with_client):
    with redis6_with_client.connection('a') as c:
        c('multi')
        with pytest.raises(Error):
            c('nothing')
        c('discard')
        c('multi')
        c('set', 'a', 'b')
        assert c('exec') == [b'OK']


def test_pubsub(redis6_with_client):
    with redis6_with_client.pubsub() as pubsub:
        with pytest.raises(Exception):
            pubsub.next_message()
        pubsub.subscribe('hi')
        pubsub.psubscribe(b'bye')
        assert pubsub.next_message() == [b'subscribe', b'hi', 1]
        assert pubsub.next_message() == [b'psubscribe', b'bye', 2]
        assert pubsub.next_message(0.1) == None
        redis6_with_client('publish', 'hi', 'there')
        assert pubsub.next_message(0.1) == [b'message', b'hi', b'there']
        redis6_with_client('publish', 'bye', 'there')
        assert pubsub.next_message(0.1) == [b'pmessage', b'bye', b'bye', b'there']
        pubsub.ping()
        # TODO (meh) this is different in RESP2/3 (cause of PUSH notation...)
        assert pubsub.next_message() == b'PONG'
        pubsub.ping(b'hi')
        assert pubsub.next_message() == b'hi'
        pubsub.unsubscribe('hi')
