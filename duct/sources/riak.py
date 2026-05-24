"""
.. module:: riak
   :platform: Any
   :synopsis: A source module for Riak metrics

.. moduleauthor:: Jeremy Thurgood <firxen@gmail.com>
"""

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.utils import HTTPRequest


@implementer(IDuctSource)
class RiakStats(Source):
    """Returns GET/PUT rates for a Riak node

    **Configuration arguments:**

    :param url: Riak stats URL
    :type url: str.
    :param useragent: User-Agent header to use
    :type useragent: str.

    **Metrics:**

    :(service name).latency: Time to complete request
    """

    async def _get_stats_from_node(self):
        url = self.config.get('url', 'http://%s:8098/stats' % self.hostname)
        ua = self.config.get('useragent', 'Duct Riak stats checker')

        return await HTTPRequest().getJson(url, headers={'User-Agent': ua})

    async def get(self):
        stats = await self._get_stats_from_node()
        get_rate = stats['node_gets'] / 60.0
        put_rate = stats['node_puts'] / 60.0

        return [
            self.createEvent(
                'ok', 'GETs per second for past minute', get_rate,
                prefix="gets_per_second"),
            self.createEvent(
                'ok', 'PUTs per second for past minute', put_rate,
                prefix="puts_per_second"),
        ]
