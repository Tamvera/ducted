"""
.. module:: logger
   :synopsis: Output which sends events to the standard logging output

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import logging

from duct.objects import Output

log = logging.getLogger(__name__)


class Logger(Output):
    """Logger output

    **Configuration arguments:**

    :param logfile: Logfile (default: write to standard log)
    :type logfile: str
    """

    def __init__(self, *a, **kw):
        Output.__init__(self, *a, **kw)
        if self.config.get('logfile'):
            self.logfile = open(self.config.get('logfile'), 'at')
        else:
            self.logfile = None

    async def stop(self):
        if self.logfile:
            self.logfile.close()

    def eventsReceived(self, events):
        for ev in events:
            if self.logfile:
                self.logfile.write(repr(ev) + '\n')
            else:
                log.info(repr(ev))
