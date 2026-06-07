"""
.. module:: munin
   :platform: Any
   :synopsis: Provides MuninNode source which can get events from the
              munin-node protocol.

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.aggregators import Counter, Counter32


class MuninClient:
    """asyncio-based munin-node client"""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def connect(self):
        """Open connection to munin-node."""
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port)
        # Read the banner line
        await self.reader.readline()

    async def send_command(self, command, multiline=False):
        """Send a command to munin-node and return the response."""
        self.writer.write((command + '\n').encode())
        await self.writer.drain()

        if multiline:
            lines = []
            while True:
                line = (await self.reader.readline()).decode().rstrip('\n')
                if line.startswith('#'):
                    continue
                if line == '.':
                    break
                lines.append(line)
            return lines
        else:
            while True:
                line = (await self.reader.readline()).decode().rstrip('\n')
                if not line.startswith('#'):
                    return line

    async def disconnect(self):
        """Close connection to munin-node."""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass


@implementer(IDuctSource)
class MuninNode(Source):
    """Connects to munin-node and retrieves all metrics

    :param host: munin-node hostname (probably localhost)
    :type host: str.
    :param port: munin-node port (probably 4949)
    :type port: int.

    **Metrics:**

    :(service name).(plugin name).(keys...): A dot separated tree of
                                             munin plugin keys
    """

    async def get(self):
        host = self.config.get('host', 'localhost')
        port = int(self.config.get('port', 4949))

        client = MuninClient(host, port)
        await client.connect()

        # Announce capabilities
        await client.send_command('cap multigraph')

        listout = await client.send_command('list')
        plug_list = listout.split()
        events = []

        for plug in plug_list:
            config = await client.send_command(f'config {plug}', True)
            plugin_config = {}
            for r in config:
                name, val = r.split(' ', 1)
                if '.' in name:
                    metric, key = name.split('.')

                    if key in ['type', 'label', 'min', 'info']:
                        plugin_config[f'{plug}.{metric}.{key}'] = val

                else:
                    if name == 'graph_category':
                        plugin_config[f'{plug}.category'] = val

            category = plugin_config.get(f'{plug}.category', 'system')

            metrics = await client.send_command(f'fetch {plug}', True)
            for m in metrics:
                name, val = m.split(' ', 1)
                if name != 'multigraph':
                    metric, key = name.split('.')
                    base = f'{plug}.{metric}'
                    m_type = plugin_config.get(f'{base}.type', 'GAUGE')

                    try:
                        val = float(val)
                    except ValueError:
                        continue

                    info = plugin_config.get(f'{base}.info', base)
                    prefix = f'{category}.{base}'

                    if m_type == 'COUNTER':
                        events.append(self.createEvent('ok', info, val,
                                                       prefix=prefix,
                                                       aggregation=Counter32))
                    elif m_type == 'DERIVE':
                        events.append(self.createEvent('ok', info, val,
                                                       prefix=prefix,
                                                       aggregation=Counter))
                    else:
                        events.append(self.createEvent('ok', info, val,
                                                       prefix=prefix))

        await client.disconnect()

        return events
