"""
.. module:: influxdb3
   :synopsis: InfluxDB 3 output

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio
import logging

from duct.objects import Output
from duct.protocol.influxdb3 import InfluxDB3Client, to_line_protocol

log = logging.getLogger(__name__)


class InfluxDB3(Output):
    """InfluxDB 3 HTTP API output using line protocol.

    Events are written as measurements named after the event service.  Tags
    include the source hostname, state, and any event attributes.  The metric
    value is stored in the ``value`` field.

    **Configuration arguments:**

    :param url: InfluxDB 3 server URL (default: http://localhost:8086)
    :type url: str
    :param database: Target database / bucket name (default: duct)
    :type database: str
    :param token: Authentication token (default: None)
    :type token: str
    :param precision: Timestamp precision - nanosecond, microsecond, millisecond,
                      or second (default: nanosecond)
    :type precision: str
    :param maxsize: Maximum queue backlog size (default: 250000, 0 disables)
    :type maxsize: int
    :param maxrate: Maximum events flushed per interval (default: 1000, 0 disables)
    :type maxrate: int
    :param interval: Queue drain interval in seconds (default: 1.0)
    :type interval: float
    """

    def __init__(self, *a):
        Output.__init__(self, *a)
        self._tick_task = None

        self.inter = float(self.config.get('interval', 1.0))
        self.maxsize = int(self.config.get('maxsize', 250000))

        self.url = self.config.get('url', 'http://localhost:8086')
        self.database = self.config.get('database', 'duct')
        self.token = self.config.get('token')
        self.precision = self.config.get('precision', 'nanosecond')

        maxrate = int(self.config.get('maxrate', 1000))
        self.queueDepth = int(maxrate * self.inter) if maxrate > 0 else None

        self.client = None

    async def createClient(self):
        """Set up the InfluxDB 3 client and start the drain loop."""
        self.client = InfluxDB3Client(
            self.url, self.database, self.token, self.precision)
        self._tick_task = asyncio.create_task(self._drain_loop())

    async def _drain_loop(self):
        try:
            while True:
                await asyncio.sleep(self.inter)
                await self._tick()
        except asyncio.CancelledError:
            pass

    async def stop(self):
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass

    def _precision_multiplier(self):
        return {
            'nanosecond': 1_000_000_000,
            'microsecond': 1_000_000,
            'millisecond': 1_000,
            'second': 1,
        }.get(self.precision, 1_000_000_000)

    def transformEvent(self, ev):
        """Convert an event to an InfluxDB line protocol string."""
        tags = {'host': ev.hostname}
        if ev.state:
            tags['state'] = ev.state
        if ev.attributes:
            tags.update({k: v for k, v in ev.attributes.items()})
        for tag in ev.tags:
            if '=' in tag:
                k, v = tag.split('=', 1)
                tags[k.strip()] = v.strip()

        fields = {'value': float(ev.metric)}
        if ev.description:
            fields['description'] = ev.description

        timestamp = int(ev.time * self._precision_multiplier())
        return to_line_protocol(ev.service, tags, fields, timestamp)

    async def sendEvents(self, events):
        """Write a batch of events to InfluxDB 3."""
        lines = [self.transformEvent(ev) for ev in events]
        return await self.client.write(lines)

    async def _tick(self):
        if not self.events:
            return

        if self.queueDepth and len(self.events) > self.queueDepth:
            events = self.events[:self.queueDepth]
            self.events = self.events[self.queueDepth:]
        else:
            events = self.events
            self.events = []

        try:
            await self.sendEvents(events)
        except Exception as ex:
            log.warning('Could not write to InfluxDB 3: %s', ex)
            self.events.extend(events)
