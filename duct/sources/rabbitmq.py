"""
.. module:: rabbitmq
   :platform: Unix
   :synopsis: A source module for rabbitmq stats

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import logging
import time

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

log = logging.getLogger(__name__)


@implementer(IDuctSource)
class Queues(Source):
    """Returns Queue information for a particular vhost

    :param vhost: Vhost name
    :type vhost: str.

    **Metrics:**

    :(service_name).(queue).ready: Ready messages for queue
    :(service_name).(queue).unack: Unacknowledged messages for queue
    :(service_name).(queue).ready_rate: Ready rate of change per second
    :(service_name).(queue).unack_rate: Unacknowledge rate of change per second

    """
    ssh = True

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.last_t = None

        self.ready = {}
        self.unack = {}

        self.last_ready = 0
        self.last_unack = 0

    async def get(self):
        vhost = self.config.get('vhost', '/')

        mqctl = self.config.get('rabbitmqctl', '/usr/sbin/rabbitmqctl')

        out, err, code = await self.fork(mqctl, args=(
            'list_queues', '-p', vhost, 'name', 'messages_ready',
            'messages_unacknowledged'
        ))

        if code == 0:
            t = time.time()

            total_ready = 0
            total_unack = 0

            rows = out.strip('\n').split('\n')

            events = []

            for row in rows:
                if "..." in row:
                    continue
                name, ready, unack = row.split()
                ready = int(ready)
                unack = int(unack)

                total_ready += ready
                total_unack += unack

                events.extend([
                    self.createEvent('ok',
                                     f'{name} unacknowledged messages: {unack}',
                                     unack, prefix=f'{name}.unack'),
                    self.createEvent('ok', f'{name} ready messages: {ready}',
                                     ready, prefix=f'{name}.ready')
                ])

                if name in self.ready:
                    last_ready = self.ready[name]
                    last_unack = self.unack[name]

                    rrate = (ready - last_ready)/float(t - self.last_t)
                    urate = (unack - last_unack)/float(t - self.last_t)

                    events.extend([
                        self.createEvent(
                            'ok',
                            f'{name} unacknowledged rate: {urate:0.2f}',
                            urate, prefix=f'{name}.unack_rate'
                        ),
                        self.createEvent(
                            'ok',
                            f'{name} ready rate: {rrate:0.2f}',
                            rrate, prefix=f'{name}.ready_rate'
                        )
                    ])

                self.ready[name] = ready
                self.unack[name] = unack

            if self.last_t:
                # Get total rates
                rrate = (total_ready - self.last_ready)/float(t - self.last_t)
                urate = (total_unack - self.last_unack)/float(t - self.last_t)

                events.extend([
                    self.createEvent(
                        'ok',
                        f'Total unacknowledged rate: {urate:0.2f}',
                        urate, prefix='total.unack_rate'
                    ),
                    self.createEvent(
                        'ok',
                        f'Total ready rate: {rrate:0.2f}',
                        rrate, prefix='total.ready_rate'
                    ),
                    self.createEvent(
                        'ok',
                        f'Total unacknowledged messages: {total_unack}',
                        total_unack, prefix='total.unack'
                    ),
                    self.createEvent(
                        'ok',
                        f'Total ready messages: {total_ready}',
                        total_ready, prefix='total.ready'
                    )
                ])

            self.last_ready = total_ready
            self.last_unack = total_unack

            self.last_t = t

            return events
        else:
            log.warning('Error running rabbitmqctl: %s', repr(err))
