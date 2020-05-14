# Notice in Python 2 bytearray implementation is less efficient, luckily it's EOL
class Buffer:
    def __init__(self, as_bytes=True):
        self._as_bytes = as_bytes
        self._buffer = bytearray()
    
    def append(self, data):
        self._buffer += data

    def __len__(self):
        return len(self._buffer)

    def skip_if_startswith(self, data):
        if self._buffer.startswith(data):
            del self._buffer[:len(data)]
            return True
        return False

    def takeline(self):
        idx = self._buffer.find(b'\r\n')
        if idx == -1:
            return None
        ret = self._buffer[:idx]
        del self._buffer[:idx + 2]
        return bytes(ret) if self._as_bytes else ret

    def take(self, nbytes):
        ret = self._buffer[:nbytes]
        del self._buffer[:nbytes]
        return bytes(ret) if self._as_bytes else ret

    def skip(self, nbytes):
        del self._buffer[:nbytes]


class ProtocolError(Exception):
    pass


class NeedMoreData:
    pass


class Result:
    def __init__(self, data, attr=None):
        self.data = data
        self.attr = attr
    
    def __repr__(self):
        rep = '%s: %s ' % (type(self).__name__, self.data)
        if self.attr:
            rep += '[%s]' % self.attr
        return rep


class String(Result):
    def __bytes__(self):
        return bytes(self.data)


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


def strip_metadata(data):
    if isinstance(data, (Array, Set)):
        data = [strip_metadata(x) for x in data.data]
    elif isinstance(data, Map):
        data = {x: strip_metadata(y) for x, y in data.data.items()}
    else:
        if not isinstance(data, (Error, BigNumber)):
            data = data.data
    return data


class RedisRespDecoder:
    def __init__(self, strip_metadata=False):
        self._buffer = Buffer()
        self._strip_metadata = strip_metadata
        self._result = iter(self._extract_generator())

    def feed(self, data):
        self._buffer.append(data)

    def extract(self, just_data=None):
        res = next(self._result)
        if res == need_more_data:
            return res
        # TODO do this better ?
        if just_data or (just_data is None and self._strip_metadata):
            return strip_metadata(res)
        else:
            return res

    # TODO on error clear buffer and abort
    def _extract_generator(self):
        buffer = self._buffer
        array_stack = []
        last_array = None
        # TODO this works wrong....
        last_attribute = None
        _need_more_data = need_more_data
        while True:
            # Handle aggregate data
            while last_array is not None:
                # Still more data needed to fill array response
                if last_array[0] != len(last_array[1]):
                    break
                # Result is an array
                if last_array[2] == 0:
                    msg_type = Array
                    msg = last_array[1]
                # Result is an map
                elif last_array[2] == 1:
                    msg_type = Map
                    protocol_error = 'Could not parse Map type result'
                    i = iter(last_array[1])
                    msg = dict([(bytes(y), next(i)) for y in i])
                # Result is an set
                elif last_array[2] == 2:
                    msg_type = Set
                    msg = set(last_array[1])
                # Result is an attribute
                elif last_array[2] == 3:
                    protocol_error = 'Could not parse Attribute type result'
                    i = iter(last_array[1])
                    last_attribute = dict([(bytes(y), next(i)) for y in i])
                # Result is an push
                elif last_array[2] == 4:
                    msg_type = Push
                    msg = last_array[1]
                else:
                    raise ProtocolError('Unknown aggregate type')
                if array_stack:
                    tmp = array_stack.pop()
                    if last_array[2] != 3:
                        msg = msg_type(msg, last_array[3])
                        tmp[1].append(msg)
                    last_array = tmp
                else:
                    if last_array[2] != 3:
                        msg = msg_type(msg, last_array[3])
                        last_array = None
                        yield msg
                    else:
                        last_array = None

            # General RESP3 parsing
            while len(buffer) == 0:
                yield _need_more_data

            # Simple string
            if buffer.skip_if_startswith(b'+'):
                msg_type = String
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
            # Simple error
            elif buffer.skip_if_startswith(b'-'):
                msg_type = Error
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
            # Number
            elif buffer.skip_if_startswith(b':'):
                msg_type = Number
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                protocol_error = 'Could not parse integer'
                msg = int(msg)
            # Blob string and Verbatim string
            elif buffer.skip_if_startswith(b'$') or buffer.skip_if_startswith(b'='):
                msg_type = String
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                # Streamed strings
                if length == b'?':
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
                    msg = b''.join(chunks)
                else:
                    length = int(length)
                    # Legacy RESP2 support
                    if length == -1:
                        msg = None
                    else:
                        while True:
                            if len(buffer) >= length + 2:
                                break
                            yield _need_more_data
                        msg = buffer.take(length)
                        buffer.skip(2)
            # Array
            elif buffer.skip_if_startswith(b'*'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                if length == b'?':
                    length = None
                else:
                    length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 0, last_attribute]
                    last_attribute = None
                    continue
            # Set
            elif buffer.skip_if_startswith(b'-'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                if length == b'?':
                    length = None
                else:
                    length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 2, last_attribute]
                    last_attribute = None
                    continue
            # Null
            elif buffer.skip_if_startswith(b'_'):
                msg_type = Null
                while True:
                    line = buffer.takeline()
                    if line is not None:
                        break
                    yield _need_more_data
                assert len(line) == 0
                msg = None
            # Double
            elif buffer.skip_if_startswith(b','):
                msg_type = Double
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = float(msg)
            # Boolean
            elif buffer.skip_if_startswith(b'#'):
                msg_type = Boolean
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                if msg == b't':
                    msg = True
                elif msg == b'f':
                    msg = False
            # Blob error
            elif buffer.skip_if_startswith(b'!'):
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
                else:
                    while True:
                        if len(buffer) >= length + 2:
                            break
                        yield _need_more_data
                    msg = buffer.take(length)
                    buffer.skip(2)
            # Big number
            elif buffer.skip_if_startswith(b'('):
                msg_type = BigNumber
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
            # Map
            elif buffer.skip_if_startswith(b'%'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                if length == b'?':
                    length = None
                else:
                    length = int(length) * 2
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 1, last_attribute]
                    last_attribute = None
                    continue
            # Attribute
            elif buffer.skip_if_startswith(b'|'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length) * 2
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 3]
                    continue
            # Push
            elif buffer.skip_if_startswith(b'>'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 4, last_attribute]
                    last_attribute = None
                    continue
            # End of streaming aggregate type
            elif buffer.skip_if_startswith(b'.'):
                while True:
                    tmp = buffer.takeline()
                    if tmp is not None:
                        break
                    yield _need_more_data
                assert tmp == b''
                last_array[0] = len(last_array[1])
                continue
            else:
                raise ProtocolError('Unknown type: %s' % bytes(buffer.take(1)))

            # TODO handle edge cases like error, RESP2 Null types, etc...
            msg = msg_type(msg, last_attribute)
            last_attribute = None
            if last_array:
                last_array[1].append(msg)
            else:
                yield msg


class RedisResp2Decoder:
    def __init__(self):
        self._buffer = Buffer()
        self._result = iter(self._extract_generator())

    def feed(self, data):
        self._buffer.append(data)

    def extract(self, just_data=None):
        res = next(self._result)
        return res

    # TODO on error clear buffer and abort
    def _extract_generator(self):
        buffer = self._buffer
        array_stack = []
        last_array = None
        _need_more_data = need_more_data
        while True:
            # Handle aggregate data
            while last_array is not None:
                # Still more data needed to fill aggregate response
                if last_array[0] != len(last_array[1]):
                    break
                msg = last_array[1]
                if array_stack:
                    tmp = array_stack.pop()
                    tmp[1].append(msg)
                    last_array = tmp
                else:
                    last_array = None
                    yield msg

            # General RESP2 parsing
            while len(buffer) == 0:
                yield _need_more_data

            # Simple string
            if buffer.skip_if_startswith(b'+'):
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
            # Simple error
            elif buffer.skip_if_startswith(b'-'):
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                msg = Error(msg)
            # Number
            elif buffer.skip_if_startswith(b':'):
                while True:
                    msg = buffer.takeline()
                    if msg is not None:
                        break
                    yield _need_more_data
                protocol_error = 'Could not parse integer'
                msg = int(msg)
            # Blob string
            elif buffer.skip_if_startswith(b'$'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                else:
                    while True:
                        if len(buffer) >= length + 2:
                            break
                        yield _need_more_data
                    msg = buffer.take(length)
                    buffer.skip(2)
            # Array
            elif buffer.skip_if_startswith(b'*'):
                while True:
                    length = buffer.takeline()
                    if length is not None:
                        break
                    yield _need_more_data
                length = int(length)
                # Legacy RESP2 support
                if length == -1:
                    msg = None
                else:
                    if last_array is not None:
                        array_stack.append(last_array)
                    last_array = [length, [], 0, None]
                    continue
            else:
                raise ProtocolError('Unknown RESP2 type: %s' % bytes(buffer.take(1)))

            if last_array:
                last_array[1].append(msg)
            else:
                yield msg
