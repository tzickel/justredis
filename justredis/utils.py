from urllib.parse import urlparse


# TODO move to utils
# TODO complete all possibile options
# TODO do I need to url encoding escape something ?
# TODO how to parse unix path
def parse_url(url):
    result = urlparse(url)
    res = {}

    if result.scheme == 'redis':
        pass
    elif result.scheme == 'redis-socket':
        res['socket_factory'] = 'unix'
    else:
        raise NotImplementedError('Not implmented connection scheme: %s' % result.scheme)

    if result.username:
        res['username'] = result.username
    if result.password:
        res['password'] = result.password

    return res
