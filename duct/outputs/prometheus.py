"""
.. module:: prometheus
   :synopsis: Prometheus output

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import logging

from aiohttp import web

from duct.objects import Output

log = logging.getLogger(__name__)


class Prometheus(Output):
    """Prometheus scrape-endpoint output

    **Configuration arguments:**

    :param port: Listening port (default: 9100)
    :type port: int.
    :param metric_path: Metrics path (default: metrics)
    :type metric_path: str.
    :param prefix: Prometheus metric prefix (default: duct_)
    :type prefix: str.
    """

    def __init__(self, *a):
        Output.__init__(self, *a)

        self.port = int(self.config.get('port', 9100))
        self.metric_path = self.config.get('metric_path', 'metrics')
        self.prefix = self.config.get('prefix', 'duct_')

        self.metric_table = {}
        self._runner = None

    async def _handle_metrics(self, request):
        content = ''.join(
            '%s %s\n' % (k, v) for k, v in self.metric_table.items()
        )
        return web.Response(text=content, content_type='text/plain')

    async def _handle_root(self, request):
        body = (
            '<html><head><title>Duct</title></head>'
            '<body><h1>Duct</h1>'
            '<p><a href="/%s">Metrics</a></p>'
            '</body></html>' % self.metric_path
        )
        return web.Response(text=body, content_type='text/html')

    async def createClient(self):
        app = web.Application()
        app.router.add_get('/' + self.metric_path, self._handle_metrics)
        app.router.add_get('/', self._handle_root)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, '0.0.0.0', self.port)
        await site.start()
        log.info('Prometheus metrics available on :%s/%s',
                 self.port, self.metric_path)

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    def eventsReceived(self, events):
        for event in events:
            metric_name = self.prefix + event.service.replace('.', '_')
            if event.attributes:
                labels = ','.join(
                    '%s="%s"' % (k, v)
                    for k, v in event.attributes.items()
                )
                metric_name += '{%s}' % labels
            self.metric_table[metric_name] = event.metric
