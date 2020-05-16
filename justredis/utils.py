# binascii requires python to be compiled with zlib ?
from binascii import crc_hqx
from urllib.parse import urlparse


# TODO complete all possibile options
# TODO do I need to url encoding escape something ?
# TODO how to parse multiple addresses?
def parse_url(url):
    result = urlparse(url)
    res = {}

    if result.scheme == 'redis':
        pass
    elif result.scheme == 'redis-socket' or result.scheme == 'unix':
        res['socket_factory'] = 'unix'
    else:
        raise NotImplementedError('Not implmented connection scheme: %s' % result.scheme)

    if result.username:
        res['username'] = result.username
    if result.password:
        res['password'] = result.password

    return res


# Cluster hash calculation
def calc_hash(key):
    s = key.find(b'{')
    if s != -1:
        e = key.find(b'}')
        if e > s + 1:
            key = key[s + 1:e]
    return crc_hqx(key, 0) % 16384
