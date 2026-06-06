import asyncio
import struct
import pytest
import pytest_asyncio

from duct.ihateprotobuf import proto_pb2
from duct.objects import Event, Source, Output
from duct.service import DuctService
from duct.aggregators import Counter32, Counter64, Counter

from .helpers import TestConfig

# ---------------------------------------------------------------------------
# Fake Riemann server using asyncio streams
# ---------------------------------------------------------------------------

class FakeRiemannServer:
    """asyncio-based fake Riemann TCP server that speaks the framed protobuf
    protocol."""

    def __init__(self):
        self.received_messages = []
        self._waiters = []
        self._server = None

    async def start(self, host='127.0.0.1', port=0):
        self._server = await asyncio.start_server(
            self._handle_client, host, port)
        # Return the actual bound port
        return self._server.sockets[0].getsockname()[1]

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(self, reader, writer):
        try:
            while True:
                try:
                    length_bytes = await reader.readexactly(4)
                except asyncio.IncompleteReadError:
                    break
                length = struct.unpack('!I', length_bytes)[0]
                data = await reader.readexactly(length)
                msg = proto_pb2.Msg.FromString(data)
                self._receive_message(msg)
                # Send ok response
                resp = proto_pb2.Msg(ok=True).SerializeToString()
                writer.write(struct.pack('!I', len(resp)) + resp)
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    def _receive_message(self, msg):
        self.received_messages.append(msg)
        self._process_waiters()

    def _process_waiters(self):
        new_waiters = []
        firing = []
        while self._waiters:
            fut, n = self._waiters.pop(0)
            if len(self.received_messages) >= n:
                firing.append(fut)
            else:
                new_waiters.append((fut, n))
        self._waiters = new_waiters
        for fut in firing:
            if not fut.done():
                fut.set_result(list(self.received_messages))

    async def wait_for_messages(self, n):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._waiters.append((fut, n))
        self._process_waiters()
        return await fut


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------

class FakeSource(Source):
    pass


class FakeOutput(Output):
    def __init__(self, *a):
        Output.__init__(self, *a)
        self.events = None

    def eventsReceived(self, events):
        self.events = events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture
async def riemann_server():
    srv = FakeRiemannServer()
    port = await srv.start()
    yield srv, port
    await srv.stop()


@pytest.mark.asyncio
async def test_service_sends_event(riemann_server):
    srv, port = riemann_server
    service = DuctService(TestConfig({
        "outputs":[
            {"output":"duct.outputs.riemann.RiemannTCP", "server": "localhost", "port": port}
        ]
    }))

    await service.startService()
    try:
        [] = await srv.wait_for_messages(0)

        source = FakeSource(
            {'service': 'test', 'interval': 1.0, 'ttl': 60.0},
            service.sendEvent, service)
        service.sources.append(source)

        event = Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0,
                      hostname='localhost')
        service.sendEvent(source, event)

        await asyncio.sleep(0.1)
        [msg] = await srv.wait_for_messages(1)
        [ev] = msg.events
        assert ev.description == 'Sky has not fallen'
    finally:
        await service.stopService()


def _aggregator_test(service, m1, m2, aggregator, delta):
    ev1 = Event('ok', 'num', 'Number', m1, delta,
                hostname='localhost', aggregation=aggregator)
    ev2 = Event('ok', 'num', 'Number', m2, delta,
                hostname='localhost', aggregation=aggregator)
    ev1.time = 1
    ev2.time = delta + 1

    service._aggregateQueue([ev1])
    result = service._aggregateQueue([ev2])
    return result[0].metric


class TestAggregators:
    def setup_method(self):
        self.service = DuctService(TestConfig({}))

    def test_aggregate_counter32(self):
        assert _aggregator_test(self.service, 1, 2, Counter32, 4) == 0.25

    def test_aggregate_counter64(self):
        assert _aggregator_test(self.service, 1, 2, Counter64, 4) == 0.25

    def test_aggregate_counter(self):
        assert _aggregator_test(self.service, 1, 2, Counter, 4) == 0.25

    def test_aggregate_counter32_rollover(self):
        assert _aggregator_test(self.service, 4294967290, 5, Counter32, 4) == 2.5

    def test_aggregate_counter64_rollover(self):
        assert _aggregator_test(
            self.service, 18446744073709551610, 5, Counter64, 4) == 2.5

    def test_state_match(self):
        service = DuctService(TestConfig({
            'interval': 1.0, 'ttl': 60.0,
            'sources': [{
                'source': 'duct.sources.linux.basic.Network',
                'interval': 2.0,
                'critical': {'network.\\w+.tx_bytes': '> 500'},
                'warning': {'network.\\w+.tx_bytes': '> 100'},
                'service': 'network'}]
        }))

        ev1 = Event('ok', 'network.foo.tx_bytes', 'net1', 50, 1,
                    hostname='localhost')
        ev2 = Event('ok', 'network.foo.tx_bytes', 'net1', 1000, 1,
                    hostname='localhost')
        ev3 = Event('ok', 'network.foo.tx_bytes', 'net1', 200, 1,
                    hostname='localhost')

        service.setStates(service.sources[0], [ev1, ev2, ev3])

        assert ev1.state == 'ok'
        assert ev2.state == 'critical'
        assert ev3.state == 'warning'

    @pytest.mark.asyncio
    async def test_source_routing(self):
        service = DuctService(TestConfig({
            'interval': 1.0, 'ttl': 60.0,
            'sources': [{
                'source': 'duct.sources.linux.basic.LoadAverage',
                'interval': 2.0,
                'route': 'out1',
                'service': 'load'}]
        }))

        output1 = FakeOutput({}, service)
        output2 = FakeOutput({}, service)

        [source] = service.sources

        service.outputs = {
            'out1': [output1],
            'out2': [output2]
        }

        event = Event('ok', 'load', 'load', 1, 1, hostname='localhost')

        service.sendEvent(source, event)

        await asyncio.sleep(0.2)

        assert len(output1.events) == 1
        assert output2.events is None

        output1.events = None
        source.config['route'] = ['out1', 'out2']

        service.sendEvent(source, event)

        await asyncio.sleep(0.2)

        assert len(output1.events) == 1
        assert len(output2.events) == 1
