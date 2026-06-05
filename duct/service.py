"""
.. module:: service
   :synopsis: Core service classes

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import time
import sys
import os
import importlib
import re
import copy
import asyncio
import logging

log = logging.getLogger(__name__)


class DuctService(object):
    """Duct service - manages sources, outputs, event routing, and watchdog."""

    def __init__(self, config):
        self.running = False
        self.sources = []
        self.lastEvents = {}
        self.outputs = {}

        self.evCache = {}
        self.critical = {}
        self.warn = {}

        self.hostConnectorCache = {}

        self.eventCounter = 0

        self._watchdog_task = None

        self.config = config

        if os.path.exists('/var/lib/duct'):
            sys.path.append('/var/lib/duct')

        self.debug = self.config['debug']
        self.ttl = self.config['ttl']
        self.stagger = self.config['stagger']

        # Backward compatibility
        self.server = self.config['server']
        self.port = self.config['port']
        self.proto = self.config['proto']
        self.inter = self.config['interval']

        if self.debug:
            print("config:", repr(config))

        self.setupSources(self.config)

    async def setupOutputs(self, config):
        """Set up output processors"""
        if self.server:
            if self.proto == 'tcp':
                defaultOutput = {
                    'output': 'duct.outputs.riemann.RiemannTCP',
                    'server': self.server,
                    'port': self.port
                }
            else:
                defaultOutput = {
                    'output': 'duct.outputs.riemann.RiemannUDP',
                    'server': self.server,
                    'port': self.port
                }
            outputs = config.get('outputs') or [defaultOutput]
        else:
            outputs = config.get('outputs', [])

        for output in outputs:
            if 'debug' not in output:
                output['debug'] = self.debug

            cl = output['output'].split('.')[-1]
            path = '.'.join(output['output'].split('.')[:-1])

            outputObj = getattr(importlib.import_module(path), cl)(output, self)

            name = output.get('name', None)

            if name in self.outputs:
                self.outputs[name].append(outputObj)
            else:
                self.outputs[name] = [outputObj]

            asyncio.create_task(outputObj.createClient())

    def createSource(self, source):
        """Construct the source object as defined in the configuration"""
        if source.get('path'):
            path = source['path']
            if path not in sys.path:
                sys.path.append(path)

        cl = source['source'].split('.')[-1]
        path = '.'.join(source['source'].split('.')[:-1])

        sourceObj = getattr(importlib.import_module(path), cl)

        if 'debug' not in source:
            source['debug'] = self.debug

        if 'ttl' not in source:
            source['ttl'] = self.ttl

        if 'interval' not in source:
            source['interval'] = self.inter

        return sourceObj(source, self.sendEvent, self)

    def setupTriggers(self, source, sobj):
        """Set up trigger actions for a source"""
        if source.get('critical'):
            self.critical[sobj] = [(re.compile(key), val)
                                   for key, val in source['critical'].items()]

        if source.get('warning'):
            self.warn[sobj] = [(re.compile(key), val)
                               for key, val in source['warning'].items()]

    def setupSources(self, config):
        """Set up source objects from the given config"""
        sources = config.get('sources', [])

        for source in sources:
            src = self.createSource(source)
            self.setupTriggers(source, src)
            self.sources.append(src)

    def _aggregateQueue(self, events):
        """Handle aggregation for each event"""
        queue = []
        for ev in events:
            if ev.aggregation:
                eid = ev.eid()
                thisM = ev.metric

                if eid in self.evCache:
                    lastM, lastTime = self.evCache[eid]
                    tDelta = ev.time - lastTime
                    metric = ev.aggregation(lastM, ev.metric, tDelta)
                    if metric:
                        ev.metric = metric
                        queue.append(ev)

                self.evCache[eid] = (thisM, ev.time)
            else:
                queue.append(ev)

        return queue

    def setStates(self, source, queue):
        """Apply warning/critical states to events based on source triggers"""
        for ev in queue:
            if ev.state == 'ok':
                for key, val in self.warn.get(source, []):
                    if key.match(ev.service):
                        state = eval(  # pylint: disable=eval-used
                            f"service {val}", {'service': ev.metric})
                        if state:
                            ev.state = 'warning'

                for key, val in self.critical.get(source, []):
                    if key.match(ev.service):
                        state = eval(  # pylint: disable=eval-used
                            f"service {val}", {'service': ev.metric})
                        if state:
                            ev.state = 'critical'

    def routeEvent(self, source, events):
        """Route events to the configured output(s) for this source"""
        routes = source.config.get('route', None)

        if not isinstance(routes, list):
            routes = [routes]

        for route in routes:
            if self.debug:
                log.debug("Sending events %s to %s", events, route)

            if route not in self.outputs:
                log.warning('Could not route %s -> %s.',
                            source.config['service'], route)
            else:
                for output in self.outputs[route]:
                    asyncio.get_event_loop().call_soon(
                        output.eventsReceived, events)

    def sendEvent(self, source, events):
        """Callback that all event sources call when they have new events"""
        if isinstance(events, list):
            self.eventCounter += len(events)
        else:
            self.eventCounter += 1
            events = [events]

        queue = self._aggregateQueue(events)

        if queue:
            if (source in self.critical) or (source in self.warn):
                self.setStates(source, queue)

            self.routeEvent(source, queue)

        self.lastEvents[source] = time.time()

    async def _startSource(self, source):
        await source.startTimer()

    async def startService(self):
        """Start all outputs and sources"""
        await self.setupOutputs(self.config)

        if self.debug:
            log.debug("Starting service")

        stagger = 0
        loop = asyncio.get_event_loop()
        for source in self.sources:
            if self.debug:
                log.debug("Starting source %s", source.config['service'])
            start_delay = float(source.config.get('start_delay', stagger))
            loop.call_later(start_delay,
                            lambda s=source: asyncio.create_task(
                                self._startSource(s)))
            stagger += self.stagger

        loop.call_later(stagger, self._launch_watchdog)
        self.running = True

    def _launch_watchdog(self):
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def _watchdog_loop(self):
        """Periodically restart stale sources that have watchdog enabled"""
        try:
            while True:
                await asyncio.sleep(10)
                await self.sourceWatchdog()
        except asyncio.CancelledError:
            pass

    async def sourceWatchdog(self):
        """Recreate sources which haven't generated events in 10×interval"""
        for i, source in enumerate(self.sources):
            if not source.config.get('watchdog', False):
                continue
            last = self.lastEvents.get(source, None)
            if last:
                sn = repr(source)
                try:
                    if last < (time.time() - (source.inter * 10)):
                        log.warning(
                            "Trying to restart stale source %s: %ss",
                            sn, int(time.time() - last))

                        source = self.sources.pop(i)
                        try:
                            await source.stopTimer()
                        except Exception as ex:
                            log.warning("Could not stop timer for %s: %s",
                                        sn, ex)

                        config = copy.deepcopy(source.config)

                        del self.lastEvents[source]
                        del source

                        source = self.createSource(config)
                        asyncio.create_task(self._startSource(source))

                except Exception as ex:
                    log.warning("Could not reset source %s: %s", sn, ex)

    async def stopService(self):
        """Stop all sources, outputs, and the watchdog"""
        self.running = False

        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass

        for source in self.sources:
            try:
                await source.stopTimer()
            except Exception as ex:
                log.warning("Error stopping source: %s", ex)

        for _, outputs in self.outputs.items():
            for output in outputs:
                try:
                    await output.stop()
                except Exception as ex:
                    log.warning("Error stopping output: %s", ex)
