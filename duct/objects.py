"""
.. module:: objects
   :synopsis: Base classes for sources, outputs, and event objects

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""

import hashlib
import time
import socket
import traceback
import asyncio
import logging

from duct.utils import fork
from duct.protocol import ssh

log = logging.getLogger(__name__)


class Event(object):
    """Duct Event object

    All sources pass these to the queue, which form a proxy object
    to create protobuf Event objects

    :param state: Some sort of string < 255 chars describing the state
    :param service: The service name for this event
    :param description: A description for the event
    :param metric: int or float metric for this event
    :param ttl: TTL (time-to-live) for this event
    :param tags: List of tag strings
    :param hostname: Hostname for the event (defaults to system fqdn)
    :param aggregation: Aggregation function
    :param attributes: A dictionary of key/value attributes for this event
    :param evtime: Event timestamp override
    """
    def __init__(
            self,
            state,
            service,
            description,
            metric,
            ttl,
            tags=None,
            hostname=None,
            aggregation=None,
            evtime=None,
            attributes=None,
            evtype='metric'):
        self.state = state
        self.service = service
        self.description = description
        self.metric = metric
        self.ttl = ttl
        self.tags = tags if tags is not None else []
        self.attributes = attributes
        self.aggregation = aggregation
        self.evtype = evtype

        self.time = evtime if evtime else time.time()
        self.hostname = hostname if hostname else socket.gethostbyaddr(
            socket.gethostname())[0]

    def eid(self):
        """Return a unique identifier for this event"""
        return self.hostname + '.' + self.service

    def __repr__(self):
        ser = ['%s=%s' % (key, repr(val)) for key, val in dict(self).items()]
        return "<Event %s>" % (', '.join(ser))

    def __iter__(self):
        obj = {
            'hostname': self.hostname,
            'state': self.state,
            'service': self.service,
            'metric': self.metric,
            'ttl': self.ttl,
            'tags': self.tags,
            'time': self.time,
            'type': self.evtype,
            'description': self.description,
        }
        if self.attributes:
            obj['attributes'] = self.attributes
        for key, val in obj.items():
            yield key, val

    def copyWithMetric(self, metric):
        """Create a copy of this event with a different metric value"""
        return Event(self.state, self.service, self.description, metric,
                     self.ttl, self.tags, self.hostname, self.aggregation)


class Output(object):
    """Output parent class

    :param config: Dictionary config for this queue
    :param duct: A DuctService object
    """
    def __init__(self, config, duct):
        self.config = config
        self.duct = duct
        self.events = []
        self.maxsize = 0

    async def createClient(self):
        """Coroutine that sets up the output connection"""
        pass

    def eventsReceived(self, events):
        """Receives a list of events and queues them"""
        if self.maxsize > 0:
            if len(self.events) < self.maxsize:
                self.events.extend(events)
        else:
            self.events.extend(events)

    async def stop(self):
        """Called when the service shuts down"""
        pass


class Source(object):
    """Source parent class

    :param config: Dictionary config for this queue
    :param queueBack: A callback method to receive a list of Event objects
    :param duct: A DuctService object
    """

    sync = False
    ssh = False

    def __init__(self, config, queueBack, duct):
        self.config = config
        self.duct = duct

        self._loop_task = None
        self.attributes = None

        self.service = config['service']
        self.inter = float(config.get('interval', duct.inter))
        self.ttl = float(config.get('ttl', duct.ttl))

        if 'tags' in config:
            self.tags = [tag.strip() for tag in config['tags'].split(',')]
        else:
            self.tags = []

        attributes = config.get("attributes")
        if isinstance(attributes, dict):
            self.attributes = attributes

        self.hostname = config.get('hostname')
        if self.hostname is None:
            self.hostname = socket.gethostbyaddr(socket.gethostname())[0]

        self.use_ssh = config.get('use_ssh', False)

        if self.use_ssh:
            self._init_ssh()

        self.queueBack = self._queueBack(queueBack)

        self.running = False

    def _init_ssh(self):
        """Configure SSH client options"""
        self.ssh_host = self.config.get('ssh_host', self.hostname)

        self.known_hosts = self.config.get(
            'ssh_knownhosts_file',
            self.duct.config.get('ssh_knownhosts_file', None)
        )

        self.ssh_keyfile = self.config.get(
            'ssh_keyfile', self.duct.config.get('ssh_keyfile', None))

        self.ssh_key = self.config.get(
            'ssh_key', self.duct.config.get('ssh_key', None))

        self.ssh_keypass = self.config.get(
            'ssh_keypass', self.duct.config.get('ssh_keypass', None))

        self.ssh_user = self.config.get(
            'ssh_username', self.duct.config.get('ssh_username', None))

        self.ssh_password = self.config.get(
            'ssh_password', self.duct.config.get('ssh_password', None))

        self.ssh_port = self.config.get(
            'ssh_port', self.duct.config.get('ssh_port', 22))

        if not (self.ssh_key or self.ssh_keyfile or self.ssh_password):
            raise Exception("To use SSH you must specify *one* of ssh_key,"
                            " ssh_keyfile or ssh_password for this source"
                            " check or globally")

        if not self.ssh_user:
            raise Exception("ssh_username must be set")

        self.ssh_keydb = []

        cHash = hashlib.sha1(
            ':'.join((
                self.ssh_host, self.ssh_user, str(self.ssh_port),
                str(self.ssh_password), str(self.ssh_key),
                str(self.ssh_keyfile)
            )).encode()).hexdigest()

        if cHash in self.duct.hostConnectorCache:
            self.ssh_client = self.duct.hostConnectorCache.get(cHash)
            self.ssh_connector = False
        else:
            self.ssh_connector = True
            self.ssh_client = ssh.SSHClient(self.ssh_host, self.ssh_user,
                                            self.ssh_port,
                                            password=self.ssh_password,
                                            knownhosts=self.known_hosts)

            if self.ssh_keyfile:
                self.ssh_client.addKeyFile(self.ssh_keyfile, self.ssh_keypass)

            if self.ssh_key:
                self.ssh_client.addKeyString(self.ssh_key, self.ssh_keypass)

            self.duct.hostConnectorCache[cHash] = self.ssh_client

    def _queueBack(self, caller):
        return lambda events: caller(self, events)

    async def start(self):
        """Called when source is started"""
        pass

    async def startTimer(self):
        """Starts the polling loop for this source"""
        await self.start()

        if self.use_ssh and self.ssh_connector:
            await self.ssh_client.connect()

        self._loop_task = asyncio.create_task(self._run_loop())

    async def _run_loop(self):
        """Async polling loop — fires tick() immediately then every inter seconds"""
        try:
            while True:
                await self.tick()
                await asyncio.sleep(self.inter)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        """Called when source is stopped"""
        pass

    async def stopTimer(self):
        """Stops the polling loop for this source"""
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._loop_task = None
        await self.stop()

    async def fork(self, *a, **kw):
        """Execute a subprocess, via SSH if use_ssh is set"""
        if self.use_ssh:
            return await self.ssh_client.fork(*a, **kw)
        else:
            return await fork(*a, **kw)

    async def _get(self):
        if self.use_ssh and not self.ssh:
            event = await self.sshGet()
        else:
            event = await self._call_get()

        if self.config.get('debug', False):
            log.debug("[%s] Tick: %s", self.config['service'], event)

        return event

    async def _call_get(self):
        """Call get(), handling both sync and async implementations"""
        result = self.get()
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def tick(self):
        """Called for every timer tick. Calls get() and passes results to queueBack."""
        if self.sync and self.running:
            return

        self.running = True

        try:
            event = await self._get()
            if event:
                self.queueBack(event)

        except Exception as ex:
            if self.duct.config.get('debug'):
                tb_lines = traceback.format_exc().splitlines()
                header = "[%s] Unhandled error: %%s" % self.service
                log.error(header, tb_lines[0])
                for l in tb_lines[1:]:
                    log.error(l)
            else:
                log.error("[%s] Unhandled error: %s", self.service, ex)

        self.running = False

    def createEvent(self, state, description, metric, prefix=None,
                    hostname=None, aggregation=None, evtime=None):
        """Creates an Event object from the Source configuration"""
        service_name = (self.service + "." + prefix) if prefix else self.service

        return Event(state, service_name, description, metric, self.ttl,
                     hostname=hostname or self.hostname,
                     aggregation=aggregation,
                     evtime=evtime, tags=self.tags, attributes=self.attributes)

    def createLog(self, evtype, data, evtime=None, hostname=None):
        """Creates a log-type Event object"""
        return Event(None, evtype, data, 0, self.ttl,
                     hostname=hostname or self.hostname, evtime=evtime,
                     tags=self.tags, evtype='log')

    def get(self):
        """Get method called every `self.inter` seconds.
        Should return a list of Event objects, a coroutine, or None.
        """
        raise NotImplementedError()

    async def sshGet(self):
        """Get method used when use_ssh is enabled"""
        raise NotImplementedError(
            "This source does not implement SSH remote checks")
