from urllib.parse import urlparse


# TODO (correctness) complete all possibile options
# TODO (correctness) do I need to url encoding escape something ?
# TODO (misc) how to parse multiple addresses?
def parse_url(url):
    result = urlparse(url)
    res = {}

    if result.scheme == "redis":
        pass
    elif result.scheme == "redis-socket" or result.scheme == "unix":
        res["socket_factory"] = "unix"
    else:
        raise NotImplementedError("Not implmented connection scheme: %s" % result.scheme)

    if result.username:
        res["username"] = result.username
    if result.password:
        res["password"] = result.password

    return res


def merge_dicts(parent, child):
    if not parent and not child:
        return None
    elif not parent:
        return child
    elif not child:
        return parent
    tmp = parent.copy()
    tmp.update(child)
    return tmp


# TODO (misc) can we do all those commands better, maybe with a special class for CustomCommand parameters?
def get_command_name(cmd):
    cmd = cmd[0]
    cmd = cmd.upper()
    if isinstance(cmd, str):
        cmd = cmd.encode()
    return cmd


def is_multiple_commands(*cmd):
    if isinstance(cmd[0], (tuple, list, dict)):
        return True
    else:
        return False
