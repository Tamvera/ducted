"""
.. module:: protocol
   :synopsis: SFlow protocol

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
from duct.protocol.sflow.protocol.xdr import Unpacker
from duct.protocol.sflow.protocol import flows, counters


class Sflow(object):
    """SFlow protocol stream decoder
    """
    def __init__(self, payload, host):
        self.host = host
        assert isinstance(payload, bytes)
        u = Unpacker(payload)

        self.version = u.unpack_uint()

        self.samplers = {
            1: FlowSample,
            2: CounterSample
        }

        self.samples = []
        self.sample_count = 0

        if self.version == 5:
            self.sflow_v5(u)

    def sflow_v5(self, u):
        """SFlow version 5 decoder
        """
        self.addrtype = u.unpack_uint()

        if self.addrtype == 1:
            self.address = u.unpack_fstring(4)

        if self.addrtype == 2:
            self.address = u.unpack_fstring(16)

        self.sub_agent_id = u.unpack_uint()
        self.sequence_number = u.unpack_uint()
        self.uptime = u.unpack_uint()

        self.sample_count = u.unpack_uint()

        self.decode_samples(u)

        # Sort samples by sequence number
        self.samples.sort(key=lambda x: x.sequence)

    def decode_samples(self, u):
        """Decode samples received
        """
        for _i in range(self.sample_count):
            sample_type = u.unpack_uint()
            self.samples.append(self.samplers[sample_type](u))

class FlowSample(object):
    """Flow sample object
    """
    def __init__(self, u):
        self.size = u.unpack_uint()

        self.sequence = u.unpack_uint()
        self.source_id = u.unpack_uint()
        self.sample_rate = u.unpack_uint()
        self.sample_pool = u.unpack_uint()
        self.dropped_packets = u.unpack_uint()

        self.if_inIndex = u.unpack_uint()
        self.if_outIndex = u.unpack_uint()

        self.record_count = u.unpack_uint()

        self.flows = {}

        for _i in range(self.record_count):
            flow_format = u.unpack_uint()
            flow_head = u.unpack_opaque()
            flow_u = Unpacker(flow_head)

            d = flows.getDecoder(flow_format)
            if d:
                self.flows[flow_format] = d(flow_u)

class CounterSample(object):
    """Counter sample object
    """
    def __init__(self, u):

        self.size = u.unpack_uint()
        self.sequence = u.unpack_uint()

        self.source_id = u.unpack_uint()

        self.record_count = u.unpack_uint()

        self.counters = {}

        for _i in range(self.record_count):
            counter_format = u.unpack_uint()
            counter = u.unpack_opaque()

            d = counters.getDecoder(counter_format)

            if d:
                self.counters[counter_format] = d(Unpacker(counter))
            else:
                print("Unknown format:", counter_format)
