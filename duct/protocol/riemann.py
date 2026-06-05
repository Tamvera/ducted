# pylint: disable=E1101
"""
.. module:: riemann
   :synopsis: Riemann protocol module

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import struct
import asyncio
import logging

from duct.ihateprotobuf import proto_pb2
from duct.interfaces import IDuctProtocol

from zope.interface import implementer

log = logging.getLogger(__name__)


class RiemannProtobufMixin(object):
    """Class mix-in for protocol buffer encoding/decoding"""

    def encodeEvent(self, event):
        """Adapt an Event object to a Riemann protobuf Event"""
        pbevent = proto_pb2.Event(
            time=int(event.time),
            state=event.state,
            service=event.service,
            host=event.hostname,
            description=event.description,
            tags=event.tags,
            ttl=event.ttl,
        )

        if event.metric is not None:
            if isinstance(event.metric, int):
                pbevent.metric_sint64 = event.metric
                pbevent.metric_f = float(event.metric)
            else:
                pbevent.metric_d = float(event.metric)
                pbevent.metric_f = float(event.metric)

        if event.attributes is not None:
            for key, value in event.attributes.items():
                attribute = pbevent.attributes.add()
                attribute.key, attribute.value = key, value

        return pbevent

    def encodeMessage(self, events):
        """Encode a list of Duct events as a protobuf message"""
        encoded = [self.encodeEvent(ev) for ev in events
                   if ev.evtype == 'metric']
        return proto_pb2.Msg(events=encoded).SerializeToString()

    def decodeMessage(self, data):
        """Decode a protobuf message"""
        message = proto_pb2.Msg()
        message.ParseFromString(data)
        return message


async def _write_framed(writer, data):
    """Write a 32-bit big-endian length-prefixed message"""
    writer.write(struct.pack('!I', len(data)) + data)
    await writer.drain()


async def _read_framed(reader):
    """Read a 32-bit big-endian length-prefixed message"""
    length_bytes = await reader.readexactly(4)
    length = struct.unpack('!I', length_bytes)[0]
    return await reader.readexactly(length)


@implementer(IDuctProtocol)
class RiemannTCPClient(RiemannProtobufMixin):
    """Asyncio TCP Riemann client with automatic reconnection.

    :param host: Riemann server hostname or list of hostnames for failover
    :param port: Riemann server port
    :param failover: Enable round-robin failover across host list
    """

    def __init__(self, host, port, failover=False):
        self.port = port
        self.failover = failover

        if failover and isinstance(host, list):
            self.hosts = host
        else:
            self.hosts = [host] if isinstance(host, list) else [host]

        self.host_index = 0
        self.reader = None
        self.writer = None
        self.pressure = 0
        self._connect_lock = asyncio.Lock()
        self._reconnect_task = None

    @property
    def current_host(self):
        return self.hosts[self.host_index]

    def _next_host(self):
        if self.failover and len(self.hosts) > 1:
            self.host_index = (self.host_index + 1) % len(self.hosts)

    @property
    def connected(self):
        return self.writer is not None and not self.writer.is_closing()

    async def connect(self):
        """Establish TCP connection to the current host"""
        async with self._connect_lock:
            if self.connected:
                return
            host = self.current_host
            log.info('Connecting to Riemann on %s:%s', host, self.port)
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    host, self.port
                )
                log.info('Connected to Riemann on %s:%s', host, self.port)
            except (ConnectionRefusedError, OSError) as e:
                log.warning('Failed to connect to Riemann %s:%s — %s',
                            host, self.port, e)
                self._next_host()
                self.reader = self.writer = None
                raise

    async def disconnect(self):
        """Close the connection"""
        async with self._connect_lock:
            if self.writer:
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass
                self.reader = self.writer = None

    async def reconnect_loop(self, delay=5.0, max_delay=30.0):
        """Keep trying to reconnect with exponential backoff"""
        current_delay = delay
        while True:
            try:
                await self.connect()
                current_delay = delay
                return
            except Exception:
                log.info('Reconnecting in %ss…', current_delay)
                await asyncio.sleep(current_delay)
                current_delay = min(current_delay * 2, max_delay)

    async def sendEvents(self, events):
        """Send a list of Duct events to Riemann. Fire-and-forget on error."""
        if not self.connected:
            return

        data = self.encodeMessage(events)
        try:
            self.pressure += 1
            await _write_framed(self.writer, data)
            response_data = await _read_framed(self.reader)
            msg = proto_pb2.Msg()
            msg.ParseFromString(response_data)
            self.pressure -= 1
            return msg
        except Exception as e:
            log.warning('Riemann send error: %s — reconnecting', e)
            self.pressure -= 1
            await self.disconnect()
            asyncio.create_task(self.reconnect_loop())


@implementer(IDuctProtocol)
class RiemannUDPProtocol(RiemannProtobufMixin, asyncio.DatagramProtocol):
    """Asyncio UDP datagram protocol for Riemann"""

    def __init__(self, host, port, on_ready=None):
        self.host = host
        self.port = port
        self.transport = None
        self._on_ready = on_ready
        self.pressure = 0

    def connection_made(self, transport):
        self.transport = transport
        if self._on_ready:
            self._on_ready(self)

    def error_received(self, exc):
        log.warning('RiemannUDP error: %s', exc)

    def connection_lost(self, exc):
        self.transport = None

    def sendEvents(self, events):
        """Send events as a UDP datagram (best-effort)"""
        if not self.transport:
            return
        data = self.encodeMessage(events)
        self.transport.sendto(data, (self.host, self.port))
        self.pressure += 1


async def create_riemann_udp(host, port):
    """Create and return a RiemannUDPProtocol bound to an ephemeral port"""
    loop = asyncio.get_event_loop()
    ready = loop.create_future()

    def on_ready(proto):
        ready.set_result(proto)

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: RiemannUDPProtocol(host, port, on_ready=on_ready),
        family=asyncio.socket.AF_INET,
    )
    return await ready
