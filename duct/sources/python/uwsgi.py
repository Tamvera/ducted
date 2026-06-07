"""
.. module:: uwsgi
   :platform: Any
   :synopsis: Reads UWSGI stats

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import asyncio
import json

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source


@implementer(IDuctSource)
class Emperor(Source):
    """Connects to UWSGI Emperor stats and creates useful metrics

    :param host: Hostname (default localhost)
    :type host: str.
    :param port: Port
    :type port: int.

    """

    async def get(self):
        host = self.config.get('host', 'localhost')
        port = int(self.config.get('port', 6001))

        reader, writer = await asyncio.open_connection(host, port)
        buf = b''
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buf += chunk
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        stats = json.loads(buf.decode())
        nodes = stats.get('vassals', [])

        events = []

        active = 0
        accepting = 0
        respawns = 0

        for node in nodes:
            if node['accepting'] > 0:
                active += 1
                accepting += node['accepting']
            if node['respawns'] > 0:
                respawns += 1

            events.extend([
                self.createEvent('ok', 'accepting', node['accepting'],
                                 prefix=node['id'] + '.accepting'),
                self.createEvent('ok', 'respawns', node['respawns'],
                                 prefix=node['id'] + '.respawns'),
            ])

        events.extend([
            self.createEvent('ok', 'active', active, prefix='total.active'),
            self.createEvent('ok', 'accepting', accepting,
                             prefix='total.accepting'),
            self.createEvent('ok', 'respawns', respawns,
                             prefix='total.respawns'),
        ])

        return events
