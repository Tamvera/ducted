"""
.. module:: network
   :platform: Unix
   :synopsis: A source module for network checks

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import asyncio
import logging
import time

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.protocol import icmp

from duct.utils import HTTPRequest, Timeout

log = logging.getLogger(__name__)

@implementer(IDuctSource)
class HTTP(Source):
    """Performs an HTTP request

    **Configuration arguments:**

    :param url: HTTP URL
    :type url: str.
    :param method: HTTP request method to use (default GET)
    :type method: str.
    :param match: A text string to match in the document when it is correct
    :type match: str.
    :param useragent: User-Agent header to use
    :type useragent: str.
    :param timeout: Timeout for connection (default 60s)
    :type timeout: int.

    **Metrics:**

    :(service name).latency: Time to complete request
    """

    async def get(self):

        method = self.config.get('method', 'GET')
        url = self.config.get('url', f'http://{self.hostname}/')
        match = self.config.get('match', None)
        ua = self.config.get('useragent', 'Duct HTTP checker')
        timeout = self.config.get('timeout', 60)

        t0 = time.time()

        try:
            body = await HTTPRequest(timeout).getBody(url, method,
                                                      {'User-Agent': ua})
        except Timeout:
            log.warning('[%s] Request timeout', url)
            t_delta = (time.time() - t0) * 1000
            return self.createEvent('critical',
                                    f'{url} - timeout', t_delta,
                                    prefix="latency")
        except Exception as e:
            log.warning('[%s] Request error %s', url, e)
            t_delta = (time.time() - t0) * 1000
            return self.createEvent('critical',
                                    f'{url} - {e}',
                                    t_delta,
                                    prefix="latency")

        t_delta = (time.time() - t0) * 1000

        if match:
            if match in body:
                state = 'ok'
            else:
                state = 'critical'
        else:
            state = 'ok'

        return self.createEvent(state, f'Latency to {url}',
                                t_delta, prefix="latency")

@implementer(IDuctSource)
class Ping(Source):
    """Performs an Ping checks against a destination

    **Configuration arguments:**

    :param destination: Host name or IP address to ping
    :type destination: str.

    **Metrics:**

    :(service name).latency: Ping latency
    :(service name).loss: Packet loss

    You can also override the `hostname` argument to make it match
    metrics from that host.
    """

    async def get(self):
        host = self.config.get('destination', self.hostname)

        try:
            loop = asyncio.get_event_loop()
            infos = await loop.getaddrinfo(host, None)
            ip = infos[0][4][0] if infos else None
        except Exception:
            ip = None

        if ip:
            try:
                loss, latency = await icmp.ping(ip, 5)
            except Exception:
                loss, latency = 100, None

            event = [self.createEvent('ok', f'{loss}% loss to {host}',
                                      loss, prefix="loss")]

            if latency:
                event.append(self.createEvent('ok', f'Latency to {host}',
                                              latency, prefix="latency"))
        else:
            event = [self.createEvent('critical', f'Unable to resolve {host}',
                                      100, prefix="loss")]

        return event
