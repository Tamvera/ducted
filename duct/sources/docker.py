"""
.. module:: docker
   :platform: Any
   :synopsis: A source module for Docker container metrics

.. moduleauthor:: Colin Alston <colin.alston@gmail.com>
"""
from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.utils import HTTPRequest, PersistentCache
from duct.aggregators import Counter64


@implementer(IDuctSource)
class ContainerStats(Source):
    """Returns stats for Docker containers on this host

    **Configuration arguments:**

    :param url: Docker stats URL
    :type url: str.

    **Metrics:**

    :(service name).(container name).mem_limit: Maximum memory for container
    :(service name).(container name).mem_used: Memory used by container
    :(service name).(container name).cpu: Percentage of system CPU in use
    :(service name).(container name).io_read: IO reads per second
    :(service name).(container name).io_write: IO writes per second
    :(service name).(container name).io_sync: IO synchronous op/s
    :(service name).(container name).io_async: IO asynchronous op/s
    :(service name).(container name).io_total: Total IOPS

    Note. If a MARATHON_APP_ID environment variable exists on the container
    then `container name` will be used instead of that.

    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.url = self.config.get('url', 'unix:/var/run/docker.sock')

        self.cache = PersistentCache(location='/tmp/dockerstats.cache')

    async def _get_stats_from_node(self):
        if self.url.startswith('unix:'):
            sock = self.url
            pref = ''
        else:
            sock = None
            pref = self.url

        containers = await HTTPRequest().getJson(
            f'{pref}/containers/json', socket=sock)

        allStats = {}

        for container in containers:
            name = container.get('Names', [None])[0].lstrip('/')

            stats = await HTTPRequest().getJson(
                f'{pref}/containers/{name}/stats?stream=false',
                socket=sock
            )

            detail = await HTTPRequest().getJson(
                f'{pref}/containers/{name}/json', socket=sock)

            env = detail['Config']['Env']

            if env:
                for var in env:
                    if var.startswith('MARATHON_APP_ID='):
                        name = var.split('=', 1)[-1].lstrip('/')

            allStats[name] = {
                'mem_limit': stats['memory_stats']['limit'],
                'mem_used': stats['memory_stats']['usage']
            }
            io_stats = stats['blkio_stats']['io_service_bytes_recursive']

            for item in io_stats:
                allStats[name]['io_' + item['op'].lower()] = item['value']

            sysCpu = stats['cpu_stats']['system_cpu_usage']
            dockCpu = stats['cpu_stats']['cpu_usage']['total_usage']

            if self.cache.contains(name):
                _lastTime, lastStats = self.cache.get(name)

                sysDelta = sysCpu - lastStats[0]
                dockDelta = dockCpu - lastStats[1]

                if sysDelta > 0:
                    allStats[name]['cpu'] = int((dockDelta / sysDelta) * 100)

            self.cache.set(name, [sysCpu, dockCpu])

        return allStats

    async def get(self):
        stats = await self._get_stats_from_node()

        events = []
        for name, container in stats.items():
            for pref, val in container.items():
                if pref.startswith('io_'):
                    events.append(self.createEvent(
                        'ok', '', val,
                        prefix=f'{name}.{pref}',
                        aggregation=Counter64
                    ))
                else:
                    events.append(self.createEvent(
                        'ok', '', val, prefix=f'{name}.{pref}'))

        return events
