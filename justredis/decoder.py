from collections import OrderedDict

from .errors import ProtocolError


# TODO (misc) We can add helpful error messages on which part the parsing failed, should we do that ?
# TODO (misc) Should we make ProtocolError the catch all chain ?
# TODO (misc) There is allot of code duplication here, we can merge most of it.


# Python 2 bytearray implementation is less efficient, luckily it's EOL
class Buffer:
    def __init__(self):
        self._buffer = bytearray()

    def append(self, data):
        self._buffer += data

    def __len__(self):
        return len(self._buffer)

    def skip_if_startswith(self, data):
        if self._buffer.startswith(data):
            del self._buffer[: len(data)]
            return True
        return False

    def takeline(self):
        idx = self._buffer.find(b"\r\n")
        if idx == -1:
            return None
        ret = self._buffer[:idx]
        del self._buffer[: idx + 2]
        return ret

    def take(self, nbytes):
        ret = self._buffer[:nbytes]
        del self._buffer[:nbytes]
        return ret

    def skip(self, nbytes):
        del self._buffer[:nbytes]


class NeedMoreData:
    pass


class Result:
    def __init__(self, data, attr=None):
        self.data = data
        self.attr = attr

    def __repr__(self):
        rep = "%s: %s " % (type(self).__name__, self.data)
        if self.attr:
            rep += "[%s]" % self.attr
        return rep


class String(Result):
    def __bytes__(self):
        return bytes(self.data)


# TODO (misc) rename this to ReplyError ?
class Error(Result, Exception):
    pass


class Number(Result):
    pass


class Null(Result):
    pass


class Double(Result):
    pass


class Boolean(Result):
    pass


class BigNumber(Result):
    pass


class Array(Result):
    pass


class Map(Result):
    pass


class Set(Result):
    pass


class Push(Result):
    pass


need_more_data = NeedMoreData()


def parse_decoding(decoding):
    if decoding is None:
        return bytes
    elif isinstance(decoding, str):
        return lambda x, decoding=decoding: x.decode(encoding=decoding)
    elif isinstance(decoding, (tuple, list)):
        return lambda x, decoding=decoding: x.decode(*decoding)
    elif isinstance(decoding, dict):
        return lambda x, decoding=decoding: x.decode(**decoding)
    else:
        raise ValueError("Invalid decoding: %r" % decoding)


class RedisRespDecoder:
    # TODO (misc) maybe add decoder, and push_decoder ?
    def __init__(self, decoder=None, attributes=False, **kwargs):
        self._decoder = parse_decoding(decoder)
        self._attributes = attributes
        self._buffer = Buffer()
        self._result = iter(self._extract_generator())

    def feed(self, data):
        self._buffer.append(data)

    def extract(self):
        # We don't do an try/finally here, since if an error occured, the connection should be closed anyhow...
        return next(self._result)

    def _extract_generator(self):
        buffer = self._buffer
        array_stack = []
        last_array = None
        last_attribute = None
        _need_more_data = need_more_data

        while True:
            # Handle aggregate data
            while last_array is not None:
                # Still more data needed to fill aggregate response
                if last_array[0] != len(last_array[1]):
                    break
                # Result is an array
                if last_array[2] == 0:
                    msg_type = Array
                    msg = last_array[1]
                # Result is an map
                elif last_array[2] == 1:
                    msg_type = Map
                    i = iter(last_array[1])
                    # TODO (misc) is this the best way to deal with y ?
                    msg = OrderedDict([(bytes(y) if y.__hash__ is None else y, next(i)) for y in i])
                # Result is an set
                elif last_array[2] == 2:
                    msg_type = Set
                    msg = set(last_array[1])
                # Result is an attribute
                elif last_array[2] == 3:
                    i = iter(last_array[1])
                    # TODO (misc) is this the best way to deal with y ?
                    last_attribute = OrderedDict([(bytes(y) if y.__hash__ is None else y, next(i)) for y in i])
                # Result is an push
                elif last_array[2] == 4:
                    msg_type = Push
                    msg = last_array[1]
                else:
                    raise ProtocolError("Unknown aggregate type")

                # If it's an attribute, nothing to do
                if last_array[2] == 3:
                    if array_stack:
                        last_array = array_stack.pop()
                    else:
                        last_array = None
                else:
                    if array_stack:
                        tmp = array_stack.pop()
                        if self._attributes:
                            msg = msg_type(msg, last_array[3])
                        # For now this isn't done, since we handle Push in unique connections
                        # elif msg_type is Push:
                        # msg = msg_type(msg)
                        tmp[1].append(msg)
                        last_array = tmp
                    else:
                        if self._attributes:
                            msg = msg_type(msg, last_array[3])
                        # For now this isn't done, since we handle Push in unique connections
                        # elif msg_type is Push:
                        # msg = msg_type(msg)
                        last_array = None
                        yield msg

            # General RESP3 parsing
            while len(buffer) == 0:
                yield _need_more_data

            # Simple string
            if buffer.skip_if_startswith(b"+"):
                msg_type = String
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = self._decoder(msg)
            # Simple error
            elif buffer.skip_if_startswith(b"-"):
                msg_type = Error
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = bytes(msg).decode("utf-8", "replace")
            # Number
            elif buffer.skip_if_startswith(b":"):
                msg_type = Number
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = int(msg)
            # Blob string and Verbatim string
            elif buffer.skip_if_startswith(b"$") or buffer.skip_if_startswith(b"="):
                msg_type = String
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                # Streamed string
                if length == b"?":
                    chunks = []
                    while True:
                        while True:
                            chunk_size = buffer.takeline()
                            if chunk_size is not None:
                                break
                            yield _need_more_data
                        chunk_size = bytes(chunk_size)
                        assert chunk_size[0] == 59
                        chunk_size = int(chunk_size[1:])
                        if chunk_size == 0:
                            break
                        while True:
                            if len(buffer) >= chunk_size + 2:
                                break
                            yield _need_more_data
                        chunks.append(buffer.take(chunk_size))
                        buffer.skip(2)
                    msg = self._decoder(b"".join(chunks))
                    chunks = None
                else:
                    length = int(length)
                    # Legacy RESP2 support
                    if length == -1:
                        msg = None
                        msg_type = Null
                    else:
                        while True:
                            if len(buffer) >= length + 2:
                                break
                            yield _need_more_data
                        msg = self._decoder(buffer.take(length))
                        buffer.skip(2)
            # Array
            elif buffer.skip_if_startswith(b"*"):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                # Streamed array
                if length == b"?":
                    length = None
                else:
                    length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 0, last_attribute]
                    last_attribute = None
                    continue
            # Set
            elif buffer.skip_if_startswith(b"~"):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                # Streamed set
                if length == b"?":
                    length = None
                else:
                    length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 2, last_attribute]
                    last_attribute = None
                    continue
            # Null
            elif buffer.skip_if_startswith(b"_"):
                msg_type = Null
                while True:
                    line = buffer.takeline()
                    if line is not None:
                        break
                    yield _need_more_data
                assert len(line) == 0
                msg = None
            # Double
            elif buffer.skip_if_startswith(b","):
                msg_type = Double
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = float(msg)
            # Boolean
            elif buffer.skip_if_startswith(b"#"):
                msg_type = Boolean
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                if msg == b"t":
                    msg = True
                elif msg == b"f":
                    msg = False
            # Blob error
            elif buffer.skip_if_startswith(b"!"):
                msg_type = Error
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    while True:
                        if len(buffer) >= length + 2:
                            break
                        yield _need_more_data
                    msg = buffer.take(length)
                    buffer.skip(2)
                    msg = bytes(msg).decode("utf-8", "replace")
            # Big number
            elif buffer.skip_if_startswith(b"("):
                msg_type = BigNumber
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = int(msg)
            # Map
            elif buffer.skip_if_startswith(b"%"):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                # Streamed map
                if length == b"?":
                    length = None
                else:
                    length = int(length) * 2
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 1, last_attribute]
                    last_attribute = None
                    continue
            # Attribute
            elif buffer.skip_if_startswith(b"|"):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length) * 2
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 3]
                    continue
            # Push
            elif buffer.skip_if_startswith(b">"):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                    msg_type = Null
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 4, last_attribute]
                    last_attribute = None
                    continue
            # End of streaming aggregate type
            elif buffer.skip_if_startswith(b"."):
                while True:
                    tmp = buffer.takeline()
                    if tmp is not None:
                        break
                    yield _need_more_data
                assert tmp == b""
                assert last_array[0] == None
                last_array[0] = len(last_array[1])
                continue
            else:
                raise ProtocolError("Unknown type: %s" % bytes(buffer.take(1)).decode())

            # Handle legacy RESP2 Null
            if msg is None and msg_type is not Null:
                msg_type = Null

            if self._attributes:
                msg = msg_type(msg, last_attribute)
            # We still enforce this types, because of ambiguity with other types
            elif msg_type == Error:
                msg = msg_type(msg)

            last_attribute = None

            if last_array:
                last_array[1].append(msg)
            else:
                yield msg
