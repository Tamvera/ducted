"""
.. module:: logger
   :synopsis: Output which sends events to the standard logging output

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging
import asyncio

from duct.objects import Output, Event

log = logging.getLogger(__name__)


class Graphite(Output):
    """Graphite output.
    By default events are named {prefix}.{hostname}.{source service} unless
    prefix_hostname is set to False

    :param server: Graphite server hostname (default: localhost)
    :type server: str.
    :param port: Graphite server port (default: 2003)
    :type port: int.
    :param prefix: Metric path prefix (default: None)
    :type prefix: str.
    :param prefix_hostname: Whether to append the event source hostname field to
                            the path (default: True)
    :type prefix_hostname: bool.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.server = self.config.get('server', 'localhost')
        self.port = self.config.get('port', 2003)

        self.prefix = self.config.get('prefix', None)

        self.prefix_hostname = self.config.get('prefix_hostname', True)

    async def push_batch_to_graphite(self, metrics):
        """Sends a batch of metric lines to Graphite via a single TCP connection."""
        try:
            _reader, writer = await asyncio.open_connection(self.server, self.port)
            for metric_path, value, timestamp in metrics:
                message = f"{metric_path} {value} {timestamp}\n"
                writer.write(message.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            log.error("Failed to send batch to Graphite: %s", str(e))

    async def eventsReceived(self, events:list[Event]):
        """Called when events are routed to this output"""
        metrics = []
        for ev in events:
            path = []

            if self.prefix:
                path.append(self.prefix)

            if self.prefix_hostname:
                path.append(ev.hostname)

            path.append(ev.service)

            metrics.append(('.'.join(path), ev.metric, int(ev.time)))

        await self.push_batch_to_graphite(metrics)
