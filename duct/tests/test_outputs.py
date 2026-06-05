import json
import ssl
import unittest.mock as mock
import pytest
import pytest_asyncio

import duct.outputs.nats as nats_mod
from duct.outputs import elasticsearch, opentsdb, nats
from duct.objects import Event
from duct.service import DuctService

from duct.protocol.senml import senml_to_event, senml_cbor_to_event, json_to_event

from .helpers import TestConfig, FakeNATS


@pytest.fixture
def service():
    return DuctService(TestConfig({}))


@pytest.fixture
def event():
    return Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0,
                 attributes={"chicken": "little"},
                 hostname='testhost')


class TestOutputs:
    @pytest.mark.asyncio
    async def test_elasticsearch_output(self, service, event):
        last_request = {}

        async def _fake_request(path, data=None, method='GET'):
            last_request['args'] = (path, data, method)
            return {"errors": []}

        out = elasticsearch.ElasticSearch({}, service)
        await out.createClient()
        out.client._request = _fake_request

        out.eventsReceived([event])
        await out._tick()

        meta, metric = last_request['args'][1].strip('\n').split('\n')
        request_data = json.loads(metric)

        assert request_data['service'] == 'sky'

    @pytest.mark.asyncio
    async def test_opentsdb_output(self, service, event):
        last_request = {}

        async def _fake_request(path, data=None, method='GET'):
            last_request['args'] = (path, data, method)
            return {"errors": []}

        out = opentsdb.OpenTSDB({}, service)
        await out.createClient()
        out.client._request = _fake_request

        out.eventsReceived([event])
        await out._tick()

        request_data = json.loads(last_request['args'][1])[0]

        assert request_data['metric'] == 'sky'


class TestNATSOutput:
    @pytest.mark.asyncio
    async def test_nats_output_senml(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({}, service)
        await out.createClient()
        out.eventsReceived([event])
        await out._tick()

        topic, payload = out.nc.messages[0]
        assert topic.endswith(".sky")

        decoded = senml_to_event(payload)
        assert decoded.service == "sky"
        assert decoded.hostname == "testhost"

    @pytest.mark.asyncio
    async def test_nats_output_senml_full_roundtrip(self, service):
        ev = Event('warning', 'sky', 'Chicken alert', 2.0, 30.0,
                   tags=['prod', 'eu'], attributes={"chicken": "little"},
                   hostname='testhost')
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({}, service)
        await out.createClient()
        out.eventsReceived([ev])
        await out._tick()

        _, payload = out.nc.messages[0]
        decoded = senml_to_event(payload)

        assert decoded.state == "warning"
        assert decoded.description == "Chicken alert"
        assert decoded.metric == pytest.approx(2.0)
        assert decoded.tags == ["prod", "eu"]
        assert decoded.attributes["chicken"] == "little"

    @pytest.mark.asyncio
    async def test_nats_output_cbor(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({"format": "senml-cbor"}, service)
        await out.createClient()
        out.eventsReceived([event])
        await out._tick()

        topic, payload = out.nc.messages[0]
        assert topic.endswith(".sky")

        decoded = senml_cbor_to_event(payload)
        assert decoded.service == "sky"
        assert decoded.metric == pytest.approx(event.metric)
        assert decoded.state == event.state

    @pytest.mark.asyncio
    async def test_nats_output_json(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({"format": "json"}, service)
        await out.createClient()
        out.eventsReceived([event])
        await out._tick()

        topic, payload = out.nc.messages[0]
        assert topic.endswith(".sky")

        decoded = json_to_event(payload)
        assert decoded.service == "sky"
        assert decoded.metric == pytest.approx(event.metric)
        assert decoded.hostname == "testhost"

    @pytest.mark.asyncio
    async def test_nats_output_prefix(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({"prefix": "metrics"}, service)
        await out.createClient()
        out.eventsReceived([event])
        await out._tick()

        topic, _ = out.nc.messages[0]
        assert topic.startswith("metrics.")
        assert topic == "metrics.testhost.sky"

    @pytest.mark.asyncio
    async def test_nats_output_no_prefix(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({}, service)
        await out.createClient()
        out.eventsReceived([event])
        await out._tick()

        topic, _ = out.nc.messages[0]
        assert topic == "testhost.sky"

    @pytest.mark.asyncio
    async def test_nats_output_jetstream(self, service, event):
        nats_mod.nats = FakeNATS()
        out = nats_mod.Nats({"jetstream": True}, service)
        await out.createClient()
        assert out.js is not None

        out.eventsReceived([event])
        await out._tick()

        assert len(out.nc.messages) == 1
        topic, payload = out.nc.messages[0]
        assert topic.endswith(".sky")
        assert senml_to_event(payload).service == "sky"

    @pytest.mark.asyncio
    async def test_nats_output_publish_error_does_not_crash(self, service, event):
        """A publish failure is logged and the drain loop continues."""
        fake = FakeNATS()

        async def _failing_publish(topic, msg):
            raise RuntimeError("simulated publish failure")

        fake.publish = _failing_publish
        nats_mod.nats = fake

        out = nats_mod.Nats({}, service)
        out.nc = fake
        out.js = None
        out.use_jetstream = False
        out.eventsReceived([event])
        # Should not raise
        await out.sendEvents(out.events)

    def test_nats_output_tls_context_none_when_unconfigured(self, service):
        out = nats_mod.Nats({}, service)
        assert out._build_tls_context() is None

    def test_nats_output_tls_context_built(self, service):
        out = nats_mod.Nats({
            "tls_ca_file": "/ca.pem",
            "tls_cert_file": "/cert.pem",
            "tls_key_file": "/key.pem",
        }, service)
        mock_ctx = mock.MagicMock()
        with mock.patch("ssl.create_default_context", return_value=mock_ctx):
            ctx = out._build_tls_context()
        assert ctx is mock_ctx
        mock_ctx.load_verify_locations.assert_called_once_with(cafile="/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(certfile="/cert.pem", keyfile="/key.pem")

    @pytest.mark.asyncio
    async def test_nats_output_credentials_passed(self, service):
        captured = {}

        class CapturingFakeNATS(FakeNATS):
            async def connect(self, servers=[], **kw):
                captured.update(kw)
                return self

        nats_mod.nats = CapturingFakeNATS()
        out = nats_mod.Nats({"credentials_file": "/path/to/creds.creds"}, service)
        await out.createClient()
        assert captured.get("user_credentials") == "/path/to/creds.creds"

    @pytest.mark.asyncio
    async def test_nats_output_nkey_passed(self, service):
        captured = {}

        class CapturingFakeNATS(FakeNATS):
            async def connect(self, servers=[], **kw):
                captured.update(kw)
                return self

        nats_mod.nats = CapturingFakeNATS()
        out = nats_mod.Nats({"nkey_seed_file": "/path/to/seed.nk"}, service)
        await out.createClient()
        assert captured.get("nkeys_seed") == "/path/to/seed.nk"
