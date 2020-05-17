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


def get_command_name(cmd):
    if isinstance(cmd[0], dict):
        cmd = cmd[0]['command']
    cmd = cmd[0]
    cmd = cmd.upper()
    if isinstance(cmd, str):
        cmd = cmd.encode()
    return cmd


# TODO (misc) can we do this better ?
def is_multiple_commands(*cmd):
    if isinstance(cmd[0], (tuple, list)):
        return True
    else:
        return False
