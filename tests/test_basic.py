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
    with pytest.raises(ValueError) as exc_info:
        redis6_with_client('set', 'a', True)
    assert redis6_with_client('incrbyfloat', 'float_check', 0.1) == b'0.1'
    with pytest.raises(ValueError) as exc_info:
        redis6_with_client('set', 'a', [1, 2])
    redis6_with_client('set', 'check_a', 'a')
    redis6_with_client('set', 'check_b', 'b')
    assert redis6_with_client({'command': ('get', 'check_a'), 'decoder': 'utf8'}) == 'a'
    assert redis6_with_client({'command': ('mget', 'check_a', 'check_b'), 'decoder': 'utf8'}) == ['a', 'b']


def test_chunk_encoded_command(redis6_with_client):
    assert redis6_with_client('set', 'test_chunk_encoded_command_a', b'test_chunk_encoded_command_a'*10*1024) == b'OK'
    assert redis6_with_client('get', 'test_chunk_encoded_command_a') == b'test_chunk_encoded_command_a'*10*1024
    assert redis6_with_client('mget', 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500, 'test_chunk_encoded_command_a' * 3500) == [None, None, None]
