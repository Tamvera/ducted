"""
.. module:: sflow
   :platform: Unix
   :synopsis: A source module which provides an sflow collector

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import asyncio
import time
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct import utils

from duct.protocol.sflow import server
from duct.protocol.sflow.protocol import flows

log = logging.getLogger(__name__)


class sFlowReceiver(server.DatagramReceiver):
    """sFlow datagram protocol"""

    def __init__(self, source):
        super().__init__()
        self.source = source
        self.lookup = source.config.get('dnslookup', True)
        self.counterCache = {}
        self.convoQueue = {}
        self.resolver = utils.Resolver()

    def process_convo_queue(self, queue, host, idx, deltaIn, tDelta):
        """Process the conversation queue"""
        addr = {'dst': {}, 'src': {}}
        port = {'dst': {}, 'src': {}}
        btotal = 0

        for convo in queue:
            src, sport, dst, dport, cbytes = convo

            addr['src'].setdefault(src, 0)
            addr['dst'].setdefault(dst, 0)
            btotal += cbytes
            addr['src'][src] += cbytes
            addr['dst'][dst] += cbytes

            port['src'].setdefault(sport, 0)
            port['dst'].setdefault(dport, 0)
            port['src'][sport] += cbytes
            port['dst'][dport] += cbytes

        for direction, v in addr.items():
            for ip, cbytes in v.items():
                m = ((cbytes / float(btotal)) * deltaIn) / tDelta
                self.source.queueBack(self.source.createEvent(
                    'ok',
                    f'sFlow if:{idx} addr:{ip} inOctets/sec {m:0.2f}',
                    m,
                    prefix=f'{idx}.ip.{ip}.{direction}',
                    hostname=host,
                ))

        for direction, v in port.items():
            for p, cbytes in v.items():
                m = ((cbytes / float(btotal)) * deltaIn) / tDelta
                if p:
                    self.source.queueBack(self.source.createEvent(
                        'ok',
                        f'sFlow if:{idx} port:{p} inOctets/sec {m:0.2f}',
                        m,
                        prefix=f'{idx}.port.{p}.{direction}',
                        hostname=host,
                    ))

    def receive_flow(self, flow, sample, host):
        def queue_flow(host):
            if isinstance(sample, flows.IPv4Header):
                if sample.ip.proto in ('TCP', 'UDP'):
                    sport, dport = (sample.ip_sport, sample.ip_dport)
                else:
                    sport, dport = (None, None)

                src = sample.ip.src.asString()
                dst = sample.ip.dst.asString()
                cbytes = sample.ip.total_length

                self.convoQueue.setdefault(host, {})
                self.convoQueue[host].setdefault(flow.if_inIndex, [])
                self.convoQueue[host][flow.if_inIndex].append(
                    (src, sport, dst, dport, cbytes))

        if self.lookup:
            asyncio.ensure_future(self._reverse_and_call(host, queue_flow))
        else:
            queue_flow(host)

    def receive_counter(self, counter, host):
        def _hostcb(host):
            idx = counter.if_index

            self.convoQueue.setdefault(host, {})
            self.counterCache.setdefault(host, {})

            if idx in self.counterCache[host]:
                lastIn, lastOut, lastT = self.counterCache[host][idx]
                tDelta = time.time() - lastT

                self.counterCache[host][idx] = (
                    counter.if_inOctets, counter.if_outOctets, time.time())

                deltaOut = counter.if_outOctets - lastOut
                deltaIn = counter.if_inOctets - lastIn

                inRate = deltaIn / tDelta
                outRate = deltaOut / tDelta

                if idx in self.convoQueue[host]:
                    queue = self.convoQueue[host][idx]
                    self.convoQueue[host][idx] = []
                    self.process_convo_queue(queue, host, idx, deltaIn, tDelta)

                self.source.queueBack([
                    self.source.createEvent(
                        'ok',
                        f'sFlow index {idx} inOctets/sec {inRate:0.2f}',
                        inRate,
                        prefix=f'{idx}.inOctets', hostname=host,
                    ),
                    self.source.createEvent(
                        'ok',
                        f'sFlow index {idx} outOctets/sec {outRate:0.2f}',
                        outRate,
                        prefix=f'{idx}.outOctets', hostname=host,
                    ),
                ])
            else:
                self.counterCache[host][idx] = (
                    counter.if_inOctets, counter.if_outOctets, time.time())

        if self.lookup:
            asyncio.ensure_future(self._reverse_and_call(host, _hostcb))
        else:
            _hostcb(host)

    async def _reverse_and_call(self, host, callback):
        try:
            resolved = await self.resolver.reverse(host)
        except Exception:
            resolved = host
        callback(resolved)


@implementer(IDuctSource)
class sFlow(Source):
    """Provides an sFlow UDP server Source.

    **Configuration arguments:**

    :param port: UDP port to listen on (default: 6343)
    :type port: int.
    :param dnslookup: Enable reverse DNS lookup for device IPs (default: True)
    :type dnslookup: bool.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._transport = None

    async def get(self):
        """sFlow does not poll; data arrives via UDP."""

    async def startTimer(self):
        port = self.config.get('port', 6343)
        loop = asyncio.get_event_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: sFlowReceiver(self),
            local_addr=('0.0.0.0', port),
        )
        log.info('sFlow UDP server listening on port %s', port)

    async def stopTimer(self):
        if self._transport:
            self._transport.close()
            self._transport = None
