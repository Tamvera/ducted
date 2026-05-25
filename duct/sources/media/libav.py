"""
.. module:: libav
   :synopsis: libav based checks

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import time

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.utils import fork


@implementer(IDuctSource)
class DarwinRTSP(Source):
    """Makes avprobe requests of a Darwin RTSP sample stream
    (sample_100kbit.mp4)

    **Configuration arguments:**

    :param destination: Host name or IP address to check
    :type method: str.

    **Metrics:**
    :(service name): Time to complete request

    You can also override the `hostname` argument to make it match
    metrics from that host.
    """

    async def get(self):
        host = self.config.get('destination', self.hostname)

        t0 = time.time()
        try:
            _out, err, code = await fork(
                '/usr/bin/avprobe',
                args=(f'rtsp://{host}/sample_100kbit.mp4', ),
                timeout=30.0
            )
        except:
            code = 1
            err = None

        t_delta = (time.time() - t0) * 1000

        if code == 0:
            e = self.createEvent('ok', f'RTSP Request time to {host}',
                                 t_delta)
        else:
            if err:
                try:
                    error = err.strip('\n').split('\n')[-2]
                except Exception:
                    error = err.replace('\n', '-')
            else:
                error = "Execution error"

            e = self.createEvent('critical',
                                 f'Unable to stream {host}:{error}',
                                 t_delta)

        return e
