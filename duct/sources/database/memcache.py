"""
.. module:: memcache
   :platform: Unix
   :synopsis: A source module for memcache stats

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import asyncio

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.aggregators import Counter64


async def _get_memcache_stats(host, port, timeout=5.0):
    """Connect to memcached and retrieve stats as a dict."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout)
    try:
        writer.write(b'stats\r\n')
        await writer.drain()

        stats = {}
        while True:
            line = (await reader.readline()).decode()
            if line.startswith('END'):
                break
            if line.startswith('STAT '):
                parts = line.split()
                if len(parts) == 3:
                    stats[parts[1]] = parts[2]
        return stats
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@implementer(IDuctSource)
class Memcache(Source):
    """Reads memcache metrics

    **Configuration arguments:**

    :param host: Database host (default localhost)
    :type host: str.
    :param port: Database port (default 11211)
    :type port: int.

    **Metrics:**

    :(service name).(metrics): Metrics from memcached
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)
        self.host = self.config.get('host', '127.0.0.1')
        self.port = self.config.get('port', 11211)

    async def get(self):
        events = []
        try:
            raw = await _get_memcache_stats(self.host, self.port)
            events.append(self.createEvent('ok', 'Connection', 1,
                                           prefix='state'))
        except Exception:
            events.append(self.createEvent('critical', 'Connection refused', 0,
                                           prefix='state'))
            return events

        counters = [
            'reclaimed', 'evictions', 'total_items',
            'touch_hits', 'touch_misses',
            'delete_misses', 'delete_hits',
            'incr_hits', 'incr_misses',
            'cas_hits', 'cas_misses', 'cas_badval',
            'get_misses', 'get_hits',
            'decr_misses', 'decr_hits',
            'cmd_set', 'cmd_flush', 'cmd_touch', 'cmd_get',
            'bytes_written', 'bytes_read',
        ]

        vals = ['curr_connections', 'curr_items', 'hash_bytes', 'bytes']

        for key in counters:
            if key in raw:
                d = key.capitalize().replace('_', ' ')
                s = key.replace('_', '.')
                events.append(self.createEvent('ok',
                                               d, int(raw[key]),
                                               prefix=s,
                                               aggregation=Counter64))

        for key in vals:
            if key in raw:
                d = key.capitalize().replace('_', ' ')
                s = key.replace('_', '.')
                events.append(self.createEvent('ok', d, int(raw[key]),
                                               prefix=s))

        return events
