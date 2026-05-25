"""
.. module:: postfix
   :platform: Unix
   :synopsis: A source module for postfix stats

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""

import logging
import os

from zope.interface import implementer

from duct.aggregators import Counter
from duct.interfaces import IDuctSource
from duct.objects import Source

log = logging.getLogger(__name__)


@implementer(IDuctSource)
class Postfix(Source):
    """Postfix checks

    **Configuration arguments:**

    :param spool: Postfix spool directory (default: /var/spool/postfix)
    :type spool: str.

    **Metrics:**

    :(service_name).active:
    :(service_name).deferred:
    :(service_name).maildrop:
    :(service_name).incoming:
    :(service_name).corrupt:
    :(service_name).hold:
    :(service_name).bounce:
    """
    ssh = True

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.spool = self.config.get('spool', '/var/spool/postfix')
        self.paths = ['active', 'deferred', 'maildrop', 'incoming', 'corrupt',
                      'hold', 'bounce']

    async def get(self):
        events = []
        for queue in self.paths:
            abspath = os.path.join(self.spool, queue)

            out, err, code = await self.fork(
                '/bin/sh',
                args=('-c',
                      f'"/bin/find {abspath} -type f | /usr/bin/wc -l"',)
            )

            if code == 0:
                val = int(out.strip('\n'))
                events.extend([
                    self.createEvent('ok', f'{queue} queue length', val,
                                     prefix=f'{queue}.value'),
                    self.createEvent('ok', 'Queue rate', val,
                                     prefix=f'{queue}.rate',
                                     aggregation=Counter)
                ])
            else:
                log.warning('Error running %s', repr(err))
        return events
