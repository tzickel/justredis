from collections import deque


def encode(encoding="utf-8", errors="strict"):
    def encode_with_encoding(inp, encoding=encoding, errors=errors):
        if isinstance(inp, (bytes, bytearray, memoryview)):
            return inp
        elif isinstance(inp, str):
            return inp.encode(encoding, errors)
        elif isinstance(inp, bool):
            raise ValueError("Invalid input for encoding")
        elif isinstance(inp, int):
            return b"%d" % inp
        elif isinstance(inp, float):
            return b"%r" % inp
        raise ValueError("Invalid input for encoding")

    return encode_with_encoding


utf8_encode = encode()


def parse_encoding(encoding):
    if encoding is None:
        return utf8_encode
    elif isinstance(encoding, str):
        return encode(encoding=encoding)
    elif isinstance(encoding, (tuple, list)):
        return encode(*encoding)
    elif isinstance(encoding, dict):
        return encode(**encoding)
    else:
        raise ValueError("Invalid encoding: %r" % encoding)


# We add data to encode in 2 steps to avoid an invalid encoding causing to drop the connections
class RedisRespEncoder:
    def __init__(self, encoder=None, cutoff_size=6000, **kwargs):
        self._encoder = parse_encoding(encoder)
        self._cutoff_size = cutoff_size
        self._chunks = deque()

    def encode(self, *cmd):
        data = []
        add_data = data.append
        encoder = self._encoder
        add_data(b"*%d\r\n" % len(cmd))
        for arg in cmd:
            arg = encoder(arg)
            if isinstance(arg, memoryview):
                length = arg.nbytes
            else:
                length = len(arg)
            add_data(b"$%d\r\n" % length)
            add_data(arg)
            add_data(b"\r\n")
        self._chunks.extend(data)

    def encode_multiple(self, *cmds):
        data = []
        add_data = data.append
        encoder = self._encoder
        for cmd in cmds:
            add_data(b"*%d\r\n" % len(cmd))
            for arg in cmd:
                arg = encoder(arg)
                if isinstance(arg, memoryview):
                    length = arg.nbytes
                else:
                    length = len(arg)
                add_data(b"$%d\r\n" % length)
                add_data(arg)
                add_data(b"\r\n")
        self._chunks.extend(data)

    def extract(self):
        cutoff_size = self._cutoff_size
        chunks = self._chunks
        if not cutoff_size:
            ret = b"".join(chunks)
            chunks.clear()
            return ret
        length = 0
        ret = []
        while True:
            if length > cutoff_size or not chunks:
                if not ret:
                    return None
                elif len(ret) == 1:
                    return ret[0]
                else:
                    return b"".join(ret)
            item = chunks.popleft()
            item_len = len(item)
            if item_len > cutoff_size:
                if length == 0:
                    return item
                else:
                    chunks.appendleft(item)
                    return b"".join(ret)
            else:
                ret.append(item)
                length += item_len
