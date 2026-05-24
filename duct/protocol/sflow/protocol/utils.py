"""
.. module:: utils
   :synopsis: SFlow protocol utility functions

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import struct
import socket


def unpack_address(u):
    """Unpack a binary network address
    """
    addrtype = u.unpack_uint()

    if addrtype == 1:
        address = u.unpack_fopaque(4)
    elif addrtype == 2:
        address = u.unpack_fopaque(16)
    else:
        address = None

    return address

class IPv4Address(object):
    """IPv4 address object
    """
    def __init__(self, addr_int):
        self.addr_int = addr_int
        self.na = struct.pack(b'!L', addr_int)

    def __str__(self):
        return socket.inet_ntoa(self.na)

    def asString(self):
        """Return a human readable string for the IP
        """
        return str(self)

    def __repr__(self):
        return "<IPv4Address ip=\"%s\">" % str(self)
