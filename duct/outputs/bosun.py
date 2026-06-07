"""
.. module:: bosun
   :synopsis: Bosun output

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import json
import base64

from duct.outputs import opentsdb
from duct.utils import HTTPRequest


class Bosun(opentsdb.OpenTSDB):
    """Bosun HTTP API output

    :param url: URL (default: http://localhost:4242)
    :type url: str.
    :param maxsize: Maximum queue backlog size (default: 250000, 0 disables)
    :type maxsize: int.
    :param maxrate: Maximum rate of documents added to index (default: 100)
    :type maxrate: int.
    :param interval: Queue check interval in seconds (default: 1.0)
    :type interval: int.
    :param user: Optional basic auth username
    :type user: str.
    :param password: Optional basic auth password
    :type password: str.
    """

    def __init__(self, *a):
        opentsdb.OpenTSDB.__init__(self, *a)
        self.url = self.url.rstrip('/')
        self.metacache = {}

    async def createMetadata(self, metas):
        """Create metadata objects for new service keys"""
        headers = {}
        if self.user:
            token = base64.b64encode(
                f'{self.user}:{self.password}'.encode()
            ).decode()
            headers['Authorization'] = 'Basic ' + token

        return await HTTPRequest().getBody(
            self.url + '/api/metadata/put', 'POST',
            headers=headers, data=json.dumps(metas).encode()
        )

    async def sendEvents(self, events):
        tsdb_events = []
        metadata_batch = []
        for event in events:
            if not self.metacache.get(event.service):
                metadata_batch.append({
                    "Metric": event.service,
                    "Name": "rate",
                    "Value": "gauge"
                })
                self.metacache[event.service] = True
            tsdb_events.append(self.transformEvent(event))

        if metadata_batch:
            await self.createMetadata(metadata_batch)

        return await self.client.put(tsdb_events)
