"""
.. module:: redis
   :platform: Unix
   :synopsis: A source module for redis stats

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
from zope.interface import implementer

import logging

from duct.interfaces import IDuctSource

log = logging.getLogger(__name__)
from duct.objects import Source
from duct.aggregators import Counter


@implementer(IDuctSource)
class Queues(Source):
    """Query llen from redis-cli

    **Configuration arguments:**

    :param queue: Queue name (defaults to 'celery', just because)
    :type queue: str.
    :param db: DB number
    :type db: int.
    :param clipath: Path to redis-cli (default: /usr/bin/redis-cli)
    :type clipath: str.

    **Metrics:**

    :(service_name): Queue length
    :(service_name): Queue rate
    """
    ssh = True

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.queue = self.config.get('queue', 'celery')
        self.db = int(self.config.get('db', 0))

        self.clipath = self.config.get('clipath', '/usr/bin/redis-cli')

    async def get(self):
        out, err, code = await self.fork(self.clipath, args=('-n',
                                                              str(self.db),
                                                              'llen',
                                                              self.queue,))

        if code == 0:
            val = int(out.strip('\n').split()[-1])
            return [
                self.createEvent('ok', '%s queue length' % self.queue, val),
                self.createEvent('ok', 'Queue rate', val, prefix='rate',
                                 aggregation=Counter)
            ]
        else:
            msg = 'Error running %s: %s' % (self.clipath, repr(err))
            log.warning(msg)
            return self.createEvent('critical', msg, None)
