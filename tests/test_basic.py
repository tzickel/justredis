import pytest
from justredis import SyncRedis, Error


def test_auth(redis6_with_blah_passwd):
    # No password
    r = SyncRedis(address=redis6_with_blah_passwd, pool_factory='pool')

    with pytest.raises(Error) as exc_info:
        r('set', 'a', 'b')
    assert exc_info.value.args[0].startswith(b'NOAUTH')

    # Wrong password
    r = SyncRedis(address=redis6_with_blah_passwd, password='nop', pool_factory='pool')

    with pytest.raises(Error) as exc_info:
        r('set', 'a', 'b')
    assert exc_info.value.args[0].startswith(b'WRONGPASS')

    # Correct password
    r = SyncRedis(address=redis6_with_blah_passwd, password='blah', pool_factory='pool')

    assert(r('set', 'a', 'b') == b'OK')
