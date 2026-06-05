"""
.. module:: server
   :synopsis: SFlow UDP server

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio

from duct.protocol.sflow import protocol
from duct.protocol.sflow.protocol import flows, counters


class DatagramReceiver(asyncio.DatagramProtocol):
    """asyncio DatagramProtocol for receiving sFlow packets"""

    def __init__(self):
        super().__init__()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        host, _port = addr
        sflow = protocol.Sflow(data, host)

        for sample in sflow.samples:
            if isinstance(sample, protocol.FlowSample):
                asyncio.get_event_loop().call_soon(
                    self.process_flow_sample, sflow, sample)

            if isinstance(sample, protocol.CounterSample):
                asyncio.get_event_loop().call_soon(
                    self.process_counter_sample, sflow, sample)

    def error_received(self, exc):
        pass

    def process_flow_sample(self, sflow, flow):
        """Process an incoming flow sample"""
        for v in flow.flows.values():
            if isinstance(v, flows.HeaderSample) and v.frame:
                asyncio.get_event_loop().call_soon(
                    self.receive_flow, flow, v.frame, sflow.host)

    def process_counter_sample(self, sflow, counter):
        """Process an incoming counter sample"""
        for v in counter.counters.values():
            if isinstance(v, counters.InterfaceCounters):
                asyncio.get_event_loop().call_soon(
                    self.receive_counter, v, sflow.host)
            elif isinstance(v, counters.HostCounters):
                asyncio.get_event_loop().call_soon(
                    self.receive_host_counter, v)

    def receive_flow(self, flow, sample, host):
        """Called when a flow is received"""

    def receive_counter(self, counter, host):
        """Called when a counter is received"""

    def receive_host_counter(self, counter, host):
        """Called when a host counter is received"""
