"""
.. module:: riemann
   :synopsis: Riemann output

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import asyncio
import logging
import ssl
import time
import random

from duct.protocol import riemann
from duct.objects import Output

log = logging.getLogger(__name__)


class RiemannTCP(Output):
    """Riemann TCP output

    **Configuration arguments:**

    :param server: Riemann server hostname (default: localhost)
    :type server: str.
    :param port: Riemann server port (default: 5555)
    :type port: int.
    :param failover: Enable server failover; `server` may be a list
    :type failover: bool.
    :param maxrate: Maximum de-queue rate (0 is no limit)
    :type maxrate: int.
    :param maxsize: Maximum queue size (default: 250000)
    :type maxsize: int.
    :param interval: De-queue interval in seconds (default: 1.0)
    :type interval: float.
    :param pressure: Maximum backpressure (-1 is no limit)
    :type pressure: int.
    :param tls: Use TLS (default: false)
    :type tls: bool.
    :param cert: Client certificate path
    :type cert: str.
    :param key: Client private key path
    :type key: str.
    :param allow_nan: Send events with None metric value (default: true)
    :type allow_nan: bool.
    """

    def __init__(self, *a):
        Output.__init__(self, *a)
        self._tick_task = None

        self.inter = float(self.config.get('interval', 1.0))
        self.pressure = int(self.config.get('pressure', -1))
        self.maxsize = int(self.config.get('maxsize', 250000))
        self.expire = self.config.get('expire', False)
        self.allow_nan = self.config.get('allow_nan', True)
        self.client = None

        maxrate = int(self.config.get('maxrate', 0))
        self.queueDepth = int(maxrate * self.inter) if maxrate > 0 else None

        self.tls = self.config.get('tls', False)
        if self.tls:
            self.cert = self.config['cert']
            self.key = self.config['key']

    async def createClient(self):
        """Create a TCP connection to Riemann and start the drain loop"""
        server = self.config.get('server', 'localhost')
        port = self.config.get('port', 5555)
        failover = self.config.get('failover', False)

        self.client = riemann.RiemannTCPClient(server, port, failover=failover)

        if self.tls:
            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ctx.load_cert_chain(certfile=self.cert, keyfile=self.key)
            self.client._ssl = ctx
        else:
            self.client._ssl = None

        initial = random.choice(self.client.hosts) if failover else server
        log.info('Connecting to Riemann on %s:%s', initial, port)

        try:
            await self.client.connect()
        except Exception:
            asyncio.create_task(self.client.reconnect_loop())

        self._tick_task = asyncio.create_task(self._drain_loop())

    async def _drain_loop(self):
        try:
            while True:
                await asyncio.sleep(self.inter)
                await self._tick()
        except asyncio.CancelledError:
            pass

    async def _tick(self):
        if self.client and self.client.connected:
            if (self.pressure < 0) or (self.client.pressure <= self.pressure):
                await self.emptyQueue()
        elif self.expire:
            now = time.time()
            self.events = [ev for ev in self.events
                           if (now - ev.time) <= ev.ttl]

    async def emptyQueue(self):
        """Drain the event queue to Riemann"""
        if not self.events:
            return

        if self.queueDepth and len(self.events) > self.queueDepth:
            events = self.events[:self.queueDepth]
            self.events = self.events[self.queueDepth:]
        else:
            events = self.events
            self.events = []

        if not self.allow_nan:
            events = [ev for ev in events if ev.metric is not None]

        if events:
            await self.client.sendEvents(events)

    async def stop(self):
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        if self.client:
            await self.client.disconnect()


class RiemannUDP(Output):
    """Riemann UDP output (spray-and-pray mode)

    **Configuration arguments:**

    :param server: Riemann server IP address (default: 127.0.0.1)
    :type server: str.
    :param port: Riemann server port (default: 5555)
    :type port: int.
    """

    def __init__(self, *a):
        Output.__init__(self, *a)
        self.protocol = None

    async def createClient(self):
        """Create a UDP connection to Riemann"""
        server = self.config.get('server', '127.0.0.1')
        port = self.config.get('port', 5555)

        self.protocol = await riemann.create_riemann_udp(server, port)
        log.info('Riemann UDP ready for %s:%s', server, port)

    def eventsReceived(self, events):
        """Receives a list of events and transmits them to Riemann"""
        if self.protocol:
            self.protocol.sendEvents(events)
