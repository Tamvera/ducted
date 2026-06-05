"""
.. module:: unbound
   :platform: Unix
   :synopsis: A source module for unbound stats

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

log = logging.getLogger(__name__)


@implementer(IDuctSource)
class Stats(Source):
    """Returns stats from unbound-control

    **Configuration arguments:**

    :param executable: Path to unbound-control executable
                       (default: /usr/sbin/unbound-control)
    :type executable: str.

    """
    ssh = True

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.uc = self.config.get('executable', '/usr/sbin/unbound-control')

    async def _get_uc_stats(self):
        out, err, code = await self.fork(self.uc, args=('stats',))

        if code == 0:
            return out.strip('\n').split('\n')
        else:
            log.warning('Error running unbound-control: %s', repr(err))
            return []

    async def get(self):
        events = []

        stats = await self._get_uc_stats()

        for row in stats:
            key, val = row.split('=')
            try:
                val = float(val)
            except ValueError:
                continue

            events.append(self.createEvent('ok', key, val, prefix=key))

        return events
