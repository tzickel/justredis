from urllib.parse import urlparse, parse_qsl


# TODO (correctness) do I need to url encoding escape something ?
# TODO (misc) the validation and conversion from strings should be done at the other side
def parse_url(url):
    result = urlparse(url)
    res = {}

    if result.scheme == "redis":
        res["socket_factory"] = "tcp"
    elif result.scheme == "redis-socket" or result.scheme == "unix":
        res["socket_factory"] = "unix"
    elif result.scheme == "rediss" or result.scheme == "ssl":
        res["socket_factory"] = "ssl"
    else:
        raise NotImplementedError("Not implmented connection scheme: %s" % result.scheme)

    if result.username:
        if result.password:
            res["username"] = result.username
            res["password"] = result.password
        else:
            res["password"] = result.username

    if res["socket_factory"] == "unix":
        res["address"] = result.path
    else:
        addresses = result.netloc.split("@")[-1].split(",")
        parsed_addresses = []
        for address in addresses:
            data = address.split(":", 1)
            if data[0] == "":
                data[0] = "localhost"
            if len(data) == 1:
                parsed_addresses.append((data[0], 6379))
            else:
                parsed_addresses.append((data[0], int(data[1])))

        if len(parsed_addresses) == 1:
            res["address"] = parsed_addresses[0]
        else:
            res["addresses"] = parsed_addresses

        if result.path and result.path != "/":
            res["database"] = result.path[1:]

    if result.query:
        res.update(dict(parse_qsl(result.query)))

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
    if isinstance(cmd[0], (tuple, list)):
        return True
    else:
        return False
