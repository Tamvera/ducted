"""Minimal XDR Unpacker replacing the deprecated xdrlib module."""
import struct


class Unpacker:
    """Minimal XDR unpacker replacing the deprecated xdrlib module."""

    def __init__(self, data):
        self._buf = data
        self._pos = 0

    def unpack_uint(self):
        """Unpack a 32-bit unsigned integer."""
        i = self._pos
        self._pos = i + 4
        return struct.unpack_from('>I', self._buf, i)[0]

    def unpack_uhyper(self):
        """Unpack a 64-bit unsigned integer."""
        i = self._pos
        self._pos = i + 8
        return struct.unpack_from('>Q', self._buf, i)[0]

    def unpack_float(self):
        """Unpack a 32-bit float."""
        i = self._pos
        self._pos = i + 4
        return struct.unpack_from('>f', self._buf, i)[0]

    def unpack_fstring(self, n):
        """Unpack a fixed-length opaque byte string (padded to 4 bytes)."""
        i = self._pos
        self._pos = i + n + (-n % 4)
        return self._buf[i:i+n]

    unpack_fopaque = unpack_fstring

    def unpack_opaque(self):
        """Unpack a variable-length opaque byte string."""
        n = self.unpack_uint()
        return self.unpack_fstring(n)

    def unpack_string(self):
        """Unpack a variable-length string."""
        return self.unpack_opaque()

    def unpack_array(self, item_fn):
        """Unpack an array using item_fn to decode each element."""
        n = self.unpack_uint()
        return [item_fn() for _ in range(n)]

    def get_buffer(self):
        """Return remaining unpacked bytes."""
        return self._buf[self._pos:]

    # typo in original sFlow counter struct — treat as unsigned int
    unpack_unsigend = unpack_uint
