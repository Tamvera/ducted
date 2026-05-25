"""
.. module:: haproxy
   :platform: Unix
   :synopsis: A source module for haproxy stats

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import csv
from base64 import b64encode

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.utils import HTTPRequest
from duct.aggregators import Counter


@implementer(IDuctSource)
class HAProxy(Source):
    """Reads Nginx stub_status

    **Configuration arguments:**

    :param url: URL to fetch stats from
    :type url: str.
    :param user: Username
    :type user: str.
    :param password: Password
    :type password: str.

    **Metrics:**

    :(service name).(backend|frontend|nodes).(stats): Various statistics
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)
        self.url = self.config.get('url', 'http://localhost/haproxy?stats;csv')
        self.user = self.config.get('user', 'haproxy')
        self.password = self.config.get('password', 'stats')

    def _ev(self, val, desc, pref, aggr=True):
        if val:
            val = int(val)
            if aggr:
                aggr = Counter
            else:
                aggr = None

            return self.createEvent('ok', f'{desc}: {val}', val,
                                    prefix=pref, aggregation=aggr)

    async def get(self):
        events = []

        authorization = b64encode(
            f'{self.user}:{self.password}'.encode()
        ).decode()

        try:
            stats = await HTTPRequest().getBody(
                self.url,
                headers={
                    'User-Agent': 'Duct',
                    'Authorization': 'Basic ' + authorization,
                }
            )
            stats = stats.lstrip('# ').split('\n')
            events.append(self.createEvent('ok', 'Connection ok', 1,
                                           prefix='state'))
        except Exception as e:
            return self.createEvent(
                'critical', f'Connection failed: {e}', 0,
                prefix='state')

        c = csv.DictReader(stats, delimiter=',')
        for row in c:
            if row['svname'] == 'BACKEND':
                p = f"backends.{row['pxname']}"
                events.append(self._ev(row['act'], 'Active servers',
                                       f'{p}.active'))
            elif row['svname'] == 'FRONTEND':
                p = f"frontends.{row['pxname']}"
            else:
                p = f"nodes.{row['pxname']}"
                events.append(self._ev(row['chkfail'], 'Check failures',
                                       f'{p}.checks_failed'))

            events.extend([
                self._ev(row['scur'], 'Sessions', f'{p}.sessions', False),
                self._ev(row['stot'], 'Session rate', f'{p}.session_rate'),
                self._ev(row['ereq'], 'Request errors', f'{p}.errors_req'),
                self._ev(row['econ'], 'Backend connection errors',
                         f'{p}.errors_con'),
                self._ev(row['eresp'], 'Response errors', f'{p}.errors_resp'),
                self._ev(row['wretr'], 'Retries', f'{p}.retries'),
                self._ev(row['wredis'], 'Switches', f'{p}.switches'),
                self._ev(int(row['bin'])*8, 'Bytes in', f'{p}.bytes_in'),
                self._ev(int(row['bout'])*8, 'Bytes out', f'{p}.bytes_out'),
                self._ev(row['hrsp_1xx'], '1xx codes', f'{p}.code_1xx'),
                self._ev(row['hrsp_2xx'], '2xx codes', f'{p}.code_2xx'),
                self._ev(row['hrsp_3xx'], '3xx codes', f'{p}.code_3xx'),
                self._ev(row['hrsp_4xx'], '4xx codes', f'{p}.code_4xx'),
                self._ev(row['hrsp_5xx'], '5xx codes', f'{p}.code_5xx'),
            ])

        return [e for e in events if e]
