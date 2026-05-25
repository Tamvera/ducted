import pytest

from duct.protocol.sflow import protocol

from duct.tests import globs


class TestSflow:
    def test_decode(self):
        proto = protocol.Sflow(globs.SFLOW_PACKET, '172.30.0.5')

        assert proto.version == 5
        assert len(proto.samples) == 5
