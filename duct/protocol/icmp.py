"""
.. module:: icmp
   :synopsis: Native ICMP protocol implementation

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import socket
import time
import fcntl
import random
import struct
import asyncio


class IP(object):
    """IP header decoder"""
    def __init__(self, packet):
        self.readPacket(packet)

    def readPacket(self, packet):
        vl = struct.unpack('!b', packet[0:1])[0]
        l = (vl & 0xf) * 4
        self.offset = struct.unpack('!H', packet[6:8])
        self.payload = packet[l:]


class EchoPacket(object):
    """ICMP Echo packet encoder and decoder"""
    def __init__(self, seq=0, eid=None, data=None, packet=None):
        if packet:
            self.decodePacket(packet)
            self.packet = packet
        else:
            self.eid = eid
            self.seq = seq
            self.data = data
            self.encodePacket()

    def calculateChecksum(self, buf):
        if isinstance(buf, str):
            buf = buf.encode('latin-1')
        nleft = len(buf)
        chksum = 0
        pos = 0
        while nleft > 1:
            chksum = buf[pos] * 256 + (buf[pos + 1] + chksum)
            pos += 2
            nleft -= 2
        if nleft == 1:
            chksum = chksum + buf[pos] * 256
        chksum = (chksum >> 16) + (chksum & 0xFFFF)
        chksum += (chksum >> 16)
        return (~chksum) & 0xFFFF

    def encodePacket(self):
        head = struct.pack('!bb', 8, 0)
        echo = struct.pack('!HH', self.seq, self.eid)
        if isinstance(self.data, str):
            data_bytes = self.data.encode('latin-1')
        else:
            data_bytes = self.data
        chk = self.calculateChecksum(head + b'\x00\x00' + echo + data_bytes)
        chk = struct.pack('!H', chk)
        self.packet = head + chk + echo + data_bytes

    def decodePacket(self, packet):
        self.icmp_type, self.code, self.chk, self.seq, self.eid = struct.unpack(
            '!bbHHH', packet[:8])
        self.data = packet[8:]
        rc = packet[:2] + b'\x00\x00' + packet[4:]
        mychk = self.calculateChecksum(rc)
        self.valid = (mychk == self.chk)

    def __repr__(self):
        return "<type=%s code=%s chk=%s seq=%s data=%s valid=%s>" % (
            self.icmp_type, self.code, self.chk, self.seq,
            len(self.data), self.valid)


class _ICMPPinger:
    """Internal state machine for an async ICMP ping"""

    def __init__(self, sock, dst, count, inter, maxwait, size, loop):
        self.sock = sock
        self.dst = dst
        self.count = count
        self.inter = inter
        self.maxwait = maxwait
        self.size = size - 36
        self.loop = loop
        self.seq = 0
        self.start = 0
        self.id_base = random.randint(0, 40000)
        self.recv = []
        self._done = loop.create_future()
        self._send_handle = None

    def _data_received(self):
        try:
            datagram, _ = self.sock.recvfrom(4096)
        except BlockingIOError:
            return
        now = int(time.time() * 1000000)
        packet = IP(datagram)
        icmp = EchoPacket(packet=packet.payload)
        if icmp.valid and icmp.code == 0 and icmp.icmp_type == 0:
            if (icmp.eid - icmp.seq) == self.id_base:
                ts = icmp.data[:8]
                delta = (now - struct.unpack('!Q', ts)[0]) / 1000.0
                self.maxwait = (self.maxwait + delta) / 2.0
                self.recv.append((icmp.seq, delta))

    def _create_data(self, n):
        s = b''
        c = 33
        for _ in range(n):
            s += bytes([c])
            c = c + 1 if c < 126 else 33
        return s

    def _send_echo(self):
        data = struct.pack('!Q', int(time.time() * 1000000))
        data += self._create_data(self.size)
        pkt = EchoPacket(seq=self.seq, eid=self.id_base + self.seq, data=data)
        self.sock.sendto(pkt.packet, (self.dst, 0))
        self.seq += 1

        if self.seq < self.count:
            self._send_handle = self.loop.call_later(self.inter, self._send_echo)
        else:
            tdelay = (self.maxwait * self.count) / 1000.0
            elapsed = time.time() - self.start
            remaining = max(tdelay - elapsed, 0.05)
            self.loop.call_later(remaining, self._finish)

    def _finish(self):
        r = len(self.recv)
        loss = int(100 * (self.count - r) / float(self.count))
        avg_latency = sum(d for _, d in self.recv) / float(r) if r else None
        if not self._done.done():
            self._done.set_result((loss, avg_latency))

    async def run(self):
        self.sock.connect((self.dst, random.randint(33434, 33534)))
        self.start = time.time()
        self.loop.add_reader(self.sock.fileno(), self._data_received)
        try:
            self._send_echo()
            return await asyncio.wait_for(
                self._done, timeout=(self.maxwait * self.count / 1000.0) + 2.0
            )
        except asyncio.TimeoutError:
            r = len(self.recv)
            loss = int(100 * (self.count - r) / float(self.count))
            avg_latency = sum(d for _, d in self.recv) / float(r) if r else None
            return (loss, avg_latency)
        finally:
            self.loop.remove_reader(self.sock.fileno())
            if self._send_handle:
                self._send_handle.cancel()


async def ping(dst, count, inter=0.2, maxwait=1000, size=64):
    """Send ICMP echo requests to `dst` `count` times.

    Returns (packet_loss_percent, avg_latency_ms) or (100, None) on failure.

    Requires CAP_NET_RAW or root privileges.
    """
    loop = asyncio.get_event_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
    sock.setblocking(False)

    fd = sock.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    try:
        pinger = _ICMPPinger(sock, dst, count, inter, maxwait, size, loop)
        return await pinger.run()
    finally:
        sock.close()
