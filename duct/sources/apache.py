"""
.. module:: apache
   :platform: Unix
   :synopsis: A source module for apache stats

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.utils import HTTPRequest

@implementer(IDuctSource)
class Apache(Source):
    """Reads Apache mod_status output

    **Configuration arguments:**

    :param url: URL to fetch status from
    :type url: str.

    **Metrics:**

    :(service name).conns.active: Active connections at this time
    :(service name).conns.writing: Writing connections
    :(service name).conns.closing: Closing connections
    :(service name).conns.keep_alive: KeepAlive connections
    :(service name).accesses: Accepted connections
    :(service name).uptime: Server uptime
    :(service name).cpu_load: CPU load
    :(service name).total_kbytes: Total kbytes served
    :(service name).request_rate: Requests per second
    :(service name).bytes_rate: Bytes per second
    :(service name).bytes_req: Bytes per request
    :(service name).workers.busy: Busy workers
    :(service name).workers.idle: Idle workers
    """

    def _parse_stats(self, stats):
        stats = stats.strip('\n').split('\n')
        d = {}
        for row in stats:
            k, v = row.split(': ')
            d[k] = v

        return {
            'conns.active': int(d.get('ConnsTotal', 0)),
            'conns.writing': int(d.get('ConnsAsyncWriting', 0)),
            'conns.closing': int(d.get('ConnsAsyncClosing', 0)),
            'conns.keep_alive': int(d.get('ConnsAsyncKeepAlive', 0)),
            'accesses': int(d.get('Total Accesses', 0)),
            'uptime': int(d.get('Uptime', 0)),
            'cpu_load': float(d.get('CPULoad', 0)),
            'total_kbytes': int(d.get('Total kBytes', 0)),
            'request_rate': float(d.get('ReqPerSec', 0)),
            'bytes_rate': float(d.get('BytesPerSec', 0)),
            'bytes_req': float(d.get('BytesPerReq', 0)),
            'workers.busy': int(d.get('BusyWorkers', 0)),
            'workers.idle': int(d.get('IdleWorkers', 0))
        }

    async def _get_stats(self):
        url = self.config.get('url', self.config.get('stats_url'))
        return await HTTPRequest().getBody(url, headers={'User-Agent': 'Duct'})

    async def get(self):
        stats = await self._get_stats()

        metrics = self._parse_stats(stats)

        return [self.createEvent('ok', 'Apache %s' % k, v, prefix=k)
                for k, v in metrics.items()]
