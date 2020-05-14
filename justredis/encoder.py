from collections import deque
from itertools import chain


def encode(encoding='utf-8', errors='strict'):
    def encode_with_encoding(inp, encoding=encoding, errors=errors):
        if isinstance(inp, (bytes, bytearray, memoryview)):
            return inp
        elif isinstance(inp, str):
            return inp.encode(encoding, errors)
        elif isinstance(inp, bool):
            raise ValueError('Invalid input for encoding')
        elif isinstance(inp, int):
            return b'%d' % inp
        elif isinstance(inp, float):
            return b'%r' % inp
        raise ValueError('Invalid input for encoding')
    return encode_with_encoding


utf8_encode = encode()


class RedisRespEncoder:
    def __init__(self, encoder=utf8_encode, cutoff_size=4096):
        self._encoder = encoder
        self._cutoff_size = cutoff_size
        self._compressed_chunks = deque()
        self._uncompressed_chunks = deque()
        self._uncompressed_length = 0

    def _add_data(self, data):
        data_length = len(data)
        cutoff_size = self._cutoff_size
        if self._uncompressed_length > cutoff_size or data_length > cutoff_size:
            if self._uncompressed_length:
                chunk = b''.join(self._uncompressed_chunks)
                self._uncompressed_chunks.clear()
                self._uncompressed_length = 0
                self._compressed_chunks.append(chunk)
            if data_length > cutoff_size:
                self._compressed_chunks.append(data)
            else:
                self._uncompressed_chunks.append(data)
                self._uncompressed_length += data_length
        else:
            self._uncompressed_chunks.append(data)
            self._uncompressed_length += data_length

    def encode(self, *cmd):
        _add_data = self._add_data
        encoder = self._encoder
        _add_data(b'*%d\r\n' % len(cmd))
        for arg in cmd:
            arg = encoder(arg)
            if isinstance(arg, memoryview):
                length = arg.nbytes
            else:
                length = len(arg)
            _add_data(b'$%d\r\n' % length)
            _add_data(arg)
            _add_data(b'\r\n')
    
    def extract(self):
        if self._compressed_chunks:
            return self._compressed_chunks.popleft()
        if self._uncompressed_chunks:
            self._uncompressed_length = 0
            if len(self._uncompressed_chunks) == 1:
                return self._uncompressed_chunks.popleft()
            else:
                ret = b''.join(self._uncompressed_chunks)
                self._uncompressed_chunks.clear()
                return ret
        return None
