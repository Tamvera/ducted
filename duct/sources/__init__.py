"""
.. module:: sources
   :synopsis: Base sources for Ducted

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import time

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source


@implementer(IDuctSource)
class Duct(Source):
    """Reports Duct information about numbers of checks
    and queue sizes.

    **Metrics:**

    :(service name).event qrate: Events added to the queue per second
    :(service name).dequeue rate: Events removed from the queue per second
    :(service name).event qsize: Number of events held in the queue
    :(service name).sources: Number of sources running
    """

    def __init__(self, *a):
        Source.__init__(self, *a)

        self.events = self.duct.eventCounter
        self.rtime = time.time()

    def get(self):
        sources = len(self.duct.sources)

        t_delta = time.time() - self.rtime

        erate = (self.duct.eventCounter - self.events)/t_delta

        self.events = self.duct.eventCounter

        self.rtime = time.time()

        return [
            self.createEvent('ok', 'Event rate', erate, prefix="event rate"),
            self.createEvent('ok', 'Sources', sources, prefix="sources"),
        ]
