"""
.. module:: opentsdb
   :synopsis: OpenTSDB output

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import asyncio
import logging

from duct.objects import Output
from duct.protocol.opentsdb import OpenTSDBClient

log = logging.getLogger(__name__)


class OpenTSDB(Output):
    """OpenTSDB HTTP API output

    **Configuration arguments:**

    :param url: URL (default: http://localhost:4242)
    :type url: str
    :param maxsize: Maximum queue backlog size (default: 250000, 0 disables)
    :type maxsize: int
    :param maxrate: Maximum rate of documents added to index (default: 100)
    :type maxrate: int
    :param interval: Queue check interval in seconds (default: 1.0)
    :type interval: int
    :param user: Optional basic auth username
    :type user: str
    :param password: Optional basic auth password
    :type password: str
    """

    def __init__(self, *a):
        Output.__init__(self, *a)
        self._tick_task = None

        self.inter = float(self.config.get('interval', 1.0))
        self.maxsize = int(self.config.get('maxsize', 250000))

        self.user = self.config.get('user')
        self.password = self.config.get('password')
        self.url = self.config.get('url', 'http://localhost:4242')

        maxrate = int(self.config.get('maxrate', 100))
        self.queueDepth = int(maxrate * self.inter) if maxrate > 0 else None

        self.client = None

    async def createClient(self):
        self.client = OpenTSDBClient(self.url, self.user, self.password)
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

    def transformEvent(self, ev):
        """Convert an event object into OpenTSDB format"""
        data = {
            'timestamp': int(ev.time * 1000),
            'metric': ev.service.replace(' ', '_'),
            'value': ev.metric,
            'tags': {
                'host': ev.hostname,
                'state': ev.state,
            }
        }
        if ev.tags:
            data['tags'] = ','.join(ev.tags)
        if ev.attributes:
            for key, val in ev.attributes.items():
                data['tags'][key] = val
        return data

    async def sendEvents(self, events):
        """Send a list of events to OpenTSDB"""
        return await self.client.put(
            [self.transformEvent(ev) for ev in events])

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
            result = await self.sendEvents(events)
            if result.get('errors'):
                log.warning('OpenTSDB error: %s', repr(result['errors']))
            if result.get('error'):
                log.warning('OpenTSDB error: %s',
                            result['error']['message'])
                if self.config.get('debug'):
                    for ln in result['error']['trace'].split('\n'):
                        log.debug(ln)
        except Exception as ex:
            log.warning('Could not connect to OpenTSDB: %s', ex)
            self.events.extend(events)
