"""
.. module:: elasticsearch
   :synopsis: Elasticsearch event output

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import asyncio
import datetime
import logging

from duct.protocol import elasticsearch
from duct.objects import Output

log = logging.getLogger(__name__)


class ElasticSearch(Output):
    """ElasticSearch HTTP API output

    **Configuration arguments:**

    :param url: Elasticsearch URL (default: http://localhost:9200)
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
    :param index: Index name format (default: duct-%Y.%m.%d)
    :type index: str
    """

    def __init__(self, *a):
        Output.__init__(self, *a)
        self._tick_task = None

        self.inter = float(self.config.get('interval', 1.0))
        self.maxsize = int(self.config.get('maxsize', 250000))

        self.user = self.config.get('user')
        self.password = self.config.get('password')
        self.url = self.config.get('url', 'http://localhost:9200')
        self.index = self.config.get('index', 'duct-%Y.%m.%d')

        maxrate = int(self.config.get('maxrate', 100))
        self.queueDepth = int(maxrate * self.inter) if maxrate > 0 else None

        self.client = None

    async def createClient(self):
        """Sets up HTTP connector and starts queue drain task"""
        self.client = elasticsearch.ElasticSearch(
            self.url, self.user, self.password, self.index)
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

    def transformEvent(self, event):
        """Transform an event into a format suitable for Elasticsearch"""
        data = dict(event)
        timestamp = datetime.datetime.utcfromtimestamp(event.time)
        data['metric'] = float(event.metric)
        data['@timestamp'] = timestamp.isoformat()
        data.pop('ttl', None)
        return data

    async def sendEvents(self, events):
        """Send events to Elasticsearch in bulk"""
        return await self.client.bulkIndex(
            [self.transformEvent(event) for event in events])

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
            if result.get('errors', False):
                log.warning('Elasticsearch bulk error: %s', repr(result))
        except Exception as ex:
            log.warning('Could not connect to Elasticsearch: %s', ex)
            self.events.extend(events)


# Backward compatibility
ElasticSearchLog = ElasticSearch
