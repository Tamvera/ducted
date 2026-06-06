import asyncio
import json
import os
import socket
import unittest.mock as mock

import aiohttp.web
import pytest
import pytest_asyncio

import duct.sources.nats as nats_src_mod
from duct.sources.linux import basic, process, sensors
from duct.sources import riak, nginx, network, apache, munin, haproxy, nats
from duct.sources.database import elasticsearch, postgresql, memcache
from duct.sources.sensors.environment import ds18b20
from duct.service import DuctService
from duct.tests import globs
from duct.protocol.senml import event_to_senml, event_to_senml_cbor, event_to_json
from duct.objects import Event

from .helpers import TestConfig, FakeNATS

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def duct_service():
    return DuctService(TestConfig({}))


def _qb(source, events):
    pass


def skip_if_no_hostname():
    try:
        socket.gethostbyaddr(socket.gethostname())
    except socket.herror:
        pytest.skip('Unable to get local hostname.')


# ---------------------------------------------------------------------------
# Linux sources
# ---------------------------------------------------------------------------

class TestLinuxSources:
    async def test_basic_cpu(self, duct_service):
        skip_if_no_hostname()
        s = basic.CPU({'service': 'cpu'}, _qb, duct_service)
        try:
            await s.get()
            await s.get()
        except Exception:
            pytest.skip('Might not exist in docker')

    async def test_basic_cpu_multi_core(self, duct_service):
        s = basic.CPU({'service': 'cpu', 'hostname': 'localhost'},
                      _qb, duct_service)

        stats = [
            "cpu  2255 34 2290 25563 6290 127 456 0 0 0",
            "cpu0 181705 1227 44920 4777152 5864 0 8054 0 0 0",
            "cpu1 186678 1194 43662 1196906 1169 0 860 0 0 0"
        ]
        s._read_proc_stat = lambda: stats
        assert await s.get() is None

        stats = [
            "cpu  2255 34 2290 25563 6290 127 456 0 0 0",
            "cpu0 181728 1227 44936 4781296 5865 0 8055 0 0 0",
            "cpu1 186712 1194 43670 1201159 1173 0 860 0 0 0"
        ]
        s._read_proc_stat = lambda: stats
        events = await s.get()
        cpu_event = events[-1]
        iowait_event = events[4]
        assert cpu_event.service == 'cpu.core1'
        assert round(cpu_event.metric, 4) == 0.0098
        assert iowait_event.service == 'cpu.core0.iowait'
        assert round(iowait_event.metric, 4) == 0.0002

    async def test_basic_cpu_calculation(self, duct_service):
        s = basic.CPU({'service': 'cpu', 'hostname': 'localhost'},
                      _qb, duct_service)

        stats = ["cpu  2255 34 2290 25563 6290 127 456 0 0 0"]
        s._read_proc_stat = lambda: stats
        assert await s.get() is None

        stats = ["cpu  4510 68 4580 51126 12580 254 912 0 0 0"]
        s._read_proc_stat = lambda: stats
        events = await s.get()
        cpu_event = events[-1]
        iowait_event = events[4]
        assert cpu_event.service == 'cpu'
        assert round(cpu_event.metric, 4) == 0.1395
        assert iowait_event.service == 'cpu.iowait'
        assert round(iowait_event.metric, 4) == 0.1699

    @pytest.mark.asyncio
    async def test_basic_cpu_ssh(self, duct_service):
        s = basic.CPU({
            'service': 'cpu',
            'use_ssh': True,
            'ssh_knownhosts_file': None,
            'ssh_password': 'None',
            'ssh_username': 'test',
            'hostname': 'localhost',
        }, _qb, duct_service)

        stats = "cpu  2255 34 2290 25563 6290 127 456 0 0 0\n"

        async def _fake_fork(*x):
            return (stats, '', 0)

        s.fork = _fake_fork
        m = await s.sshGet()
        assert m is None

        stats = "cpu  4510 68 4580 51126 12580 254 912 0 0 0\n"
        s.fork = _fake_fork
        events = await s.sshGet()
        cpu_event = events[-1]
        iowait_event = events[4]
        assert cpu_event.service == 'cpu'
        assert round(cpu_event.metric, 4) == 0.1395
        assert iowait_event.service == 'cpu.iowait'
        assert round(iowait_event.metric, 4) == 0.1699

    async def test_basic_cpu_calculation_no_guest_stats(self, duct_service):
        s = basic.CPU({'service': 'cpu', 'hostname': 'localhost'},
                      _qb, duct_service)

        stats = ["cpu  2255 34 2290 25563 6290 127 456 0"]
        s._read_proc_stat = lambda: stats
        assert await s.get() is None

        stats = ["cpu  4510 68 4580 51126 12580 254 912 0"]
        s._read_proc_stat = lambda: stats
        events = await s.get()
        cpu_event = events[-1]
        iowait_event = events[4]
        assert cpu_event.service == 'cpu'
        assert round(cpu_event.metric, 4) == 0.1395
        assert iowait_event.service == 'cpu.iowait'
        assert round(iowait_event.metric, 4) == 0.1699

    async def test_disk_io(self, duct_service):
        s = basic.DiskIO({'service': 'disk', 'hostname': 'localhost'},
                         _qb, duct_service)

        stats = [
            '   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0',
            '   7       0 loop0 0 0 0 0 0 0 0 0 0 0 0',
            ' 202       2 xvda2 2 0 4 64 0 0 0 0 0 64 64',
            ' 202      32 xvdc 576 10 3739 748 144 0 4497 18080 0 8616 18828',
            ' 202      33 xvdc1 423 0 2435 264 144 0 4497 18080 0 8132 18344',
        ]
        s._getstats = lambda: stats
        events = await s.get()
        assert events[0].metric == 32
        assert events[1].metric == 2

    async def test_basic_memory(self, duct_service):
        skip_if_no_hostname()
        s = basic.Memory({'service': 'mem'}, _qb, duct_service)
        await s.get()

    def test_basic_memory_avail(self, duct_service):
        s = basic.Memory({'interval': 1.0, 'service': 'mem'}, _qb, duct_service)

        out = """MemTotal:        8048992 kB
MemFree:         2774664 kB
MemAvailable:    5631108 kB
Buffers:          145408 kB
Cached:          3183724 kB
SwapCached:            0 kB\n"""
        event = s._parse_stats(out.split('\n'))
        used, total = event.description.split()[-1].split('/')
        assert int(total) - int(used) == 5631108

        out = """MemTotal:        8048992 kB
MemFree:         2774664 kB
Buffers:          145408 kB
Cached:          3183724 kB
SwapCached:            0 kB\n"""
        event = s._parse_stats(out.split('\n'))
        used, total = event.description.split()[-1].split('/')
        assert int(total) - int(used) == 6103796

    async def test_basic_load(self, duct_service):
        skip_if_no_hostname()
        s = basic.LoadAverage({'service': 'mem'}, _qb, duct_service)
        await s.get()

    @pytest.mark.asyncio
    async def test_process_count(self, duct_service):
        skip_if_no_hostname()
        s = process.ProcessCount({'service': 'proc'}, _qb, duct_service)
        await s.get()

    @pytest.mark.asyncio
    async def test_basic_disk_space(self, duct_service):
        skip_if_no_hostname()
        s = basic.DiskFree({'service': 'df'}, _qb, duct_service)
        await s.get()

    @pytest.mark.asyncio
    async def test_process_stats(self, duct_service):
        skip_if_no_hostname()
        s = process.ProcessStats({'service': 'ps'}, _qb, duct_service)
        await s.get()

    async def test_network_stats(self, duct_service):
        skip_if_no_hostname()
        s = basic.Network({'service': 'net'}, _qb, duct_service)

        s._readStats = lambda: [
            '  eth0: 254519754 1437339    0    0    0     0          0      '
            '   0 202067280 1154168    0    0    0     0       0          0',
            '    lo: 63830682  900933    0    0    0     0          0       '
            '  0 63830682  900933    0    0    0     0       0          0'
        ]

        ev = await s.get()
        assert ev[0].metric == 254519754
        assert ev[1].metric == 1437339
        assert ev[2].metric == 0
        assert ev[3].metric == 202067280
        assert ev[4].metric == 1154168
        assert ev[5].metric == 0

    async def test_sensors(self, duct_service):
        s = sensors.Sensors({'service': 'sensors'}, _qb, duct_service)
        s._find_sensors = lambda: {
            'acpitz': {},
            'coretemp': {'physical_id_0': 58.0, 'core_0': 58.0, 'core_1': 54.0},
            'dell_smm': {'other': 34.0, 'processor_fan': 0, 'ambient': 48.0,
                         'cpu': 54.0, 'sodimm': 38.0}
        }
        events = await s.get()
        e = [ev for ev in events if ev.service == 'sensors.coretemp.physical_id_0']
        assert e[0].metric == 58.0


# ---------------------------------------------------------------------------
# Database/other sources
# ---------------------------------------------------------------------------

class TestOtherSources:
    @pytest.mark.asyncio
    async def test_postgresql(self, duct_service):
        events = []

        def _qb_ev(source, event):
            events.append(event)

        s = postgresql.PostgreSQL({'service': 'postgres'}, _qb_ev, duct_service)

        rows = [
            ('template1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            ('template0', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            ('postgres', 1, 1256230, 1, 1058, 33931091, 405747556, 6290701, 0, 0, 0),
            ('testdb', 0, 1304478, 0, 1495, 53416317, 686856059, 9578732, 3094, 460, 151)
        ]

        class FakeConn:
            async def fetch(self, query):
                return rows

            async def close(self):
                pass

        async def _fake_connect(**kw):
            return FakeConn()

        import unittest.mock as mock
        with mock.patch('asyncpg.connect', _fake_connect):
            event = await s.get()

        assert events[1].service == 'postgres.postgres.commits'
        assert events[1].metric == 1256230

    @pytest.mark.asyncio
    async def test_elastic(self, duct_service):
        s = elasticsearch.ElasticSearch({'service': 'es'}, _qb, duct_service)

        async def _request(path, data=None, method='GET'):
            if path == '/_cluster/stats':
                return globs.ES_CLUSTER_STATS
            if path == '/_nodes/stats':
                return globs.ES_NODES_STATS

        s.client._request = _request

        events = await s.get()

        assert events[0].service == 'es.cluster.status'
        assert events[0].metric == 1

    @pytest.mark.asyncio
    async def test_munin(self, duct_service):
        s = munin.MuninNode({'service': 'munin'}, _qb, duct_service)

        # Patch the MuninClient.connect to use a fake in-memory client
        class FakeMuninClient:
            async def connect(self):
                pass

            async def send_command(self, command, multiline=False):
                if command.startswith('cap '):
                    return "cap multigraph dirtyconfig"
                if command == 'list':
                    return globs.MUNIN_LIST
                if command.startswith('config '):
                    toconfigure = command.split()[-1]
                    configs = {
                        'apache_accesses': globs.MUNIN_APACHE_ACCESSES,
                        'apache_processes': globs.MUNIN_APACHE_PROCS,
                    }
                    r = configs.get(toconfigure, "")
                    return r.strip('\n').split('\n') if multiline else r
                if command.startswith('fetch '):
                    tofetch = command.split()[-1]
                    results = {
                        'apache_accesses': 'accesses80.value $',
                        'apache_processes': 'busy80.value 1\nidle80.value 49\nfree80.value 100',
                    }
                    r = results.get(tofetch, "")
                    return r.strip('\n').split('\n') if multiline else r
                return "" if not multiline else []

            async def disconnect(self):
                pass

        import duct.sources.munin as munin_module
        orig = munin_module.MuninClient

        def _fake_client(host, port):
            return FakeMuninClient()

        munin_module.MuninClient = _fake_client
        try:
            events = await s.get()
        finally:
            munin_module.MuninClient = orig

        assert events[0].metric == 1
        assert events[1].metric == 49.0

    @pytest.mark.asyncio
    async def test_memcache(self, duct_service):
        s = memcache.Memcache({'service': 'memcache'}, _qb, duct_service)

        async def _fake_stats(host, port, timeout=5.0):
            return {k: str(v) for k, v in globs.MEMCACHE_STATS.items()}

        import duct.sources.database.memcache as mc_mod
        orig = mc_mod._get_memcache_stats
        mc_mod._get_memcache_stats = _fake_stats
        try:
            events = await s.get()
        finally:
            mc_mod._get_memcache_stats = orig

        total_items_ev = [e for e in events if 'total.items' in e.service]
        assert total_items_ev[0].metric == 44

    @pytest.mark.asyncio
    async def test_haproxy(self, duct_service):
        s = haproxy.HAProxy({'service': 'haproxy'}, _qb, duct_service)

        async def _get_stats():
            return globs.HAPROXY_CSV

        s._get_stats = _get_stats
        events = await s.get()


# ---------------------------------------------------------------------------
# Apache / Nginx sources
# ---------------------------------------------------------------------------

class TestWebSources:
    @pytest.mark.asyncio
    async def test_apache_stats(self, duct_service):
        src = apache.Apache({
            'service': 'apache',
            'hostname': 'localhost',
            'url': 'http://localhost/server-status?auto'
        }, _qb, duct_service)

        async def _get_stats():
            return globs.APACHE_STATS

        src._get_stats = _get_stats

        events = await src.get()

        results = {
            'apache.uptime': 4564,
            'apache.accesses': 46,
            'apache.cpu_load': 0.036,
            'apache.bytes_req': 868.125,
            'apache.bytes_rate': 8.75,
            'apache.total_kbytes': 39,
            'apache.conns.active': 9,
            'apache.request_rate': 0.5,
            'apache.workers.idle': 48,
            'apache.workers.busy': 2,
            'apache.conns.writing': 2,
            'apache.conns.closing': 4,
            'apache.conns.keep_alive': 3,
        }

        for ev in events:
            assert ev.metric == results.get(ev.service)

    def test_nginx_parse(self, duct_service):
        src = nginx.Nginx({
            'service': 'nginx',
            'hostname': 'localhost',
            'stats_url': 'http://localhost/nginx_stats'
        }, _qb, duct_service)

        ngstats = """Active connections: 3
server accepts handled requests
 20649 20649 686969
Reading: 0 Writing: 1 Waiting: 2\n"""

        metrics = src._parse_nginx_stats(ngstats)
        assert metrics['handled'][0] == 20649

    async def test_nginx_log_nohistory(self, duct_service, tmp_path):
        events = []

        def qb(src, ev):
            events.append(ev)

        log_file = str(tmp_path / 'foo.log')
        f = open(log_file, 'wt')
        f.write('192.168.0.1 - - [16/Jan/2015:16:31:29 +0200] "GET /foo HTTP/1.1" 200 210 "-" "My Browser"\n')
        f.write('192.168.0.1 - - [16/Jan/2015:16:51:29 +0200] "GET /foo HTTP/1.1" 200 410 "-" "My Browser"\n')
        f.flush()

        src = nginx.NginxLogMetrics({
            'service': 'nginx',
            'hostname': 'localhost',
            'log_format': 'combined',
            'file': log_file
        }, qb, duct_service)

        src.log.tmp = str(tmp_path / 'foo.log2.lf')
        await src.get()
        assert len(events) == 0

        f.write('192.168.0.1 - - [16/Jan/2015:17:31:29 +0200] "GET /foo HTTP/1.1" 200 210 "-" "My Browser"\n')
        f.write('192.168.0.1 - - [16/Jan/2015:17:51:29 +0200] "GET /foo HTTP/1.1" 200 410 "-" "My Browser"\n')
        f.flush()

        await src.get()
        assert len(events) > 0

    async def test_nginx_log(self, duct_service, tmp_path):
        events = []

        def qb(src, ev):
            events.append(ev)

        log_file = str(tmp_path / 'foo.log')
        f = open(log_file, 'wt')
        f.write('192.168.0.1 - - [16/Jan/2015:16:31:29 +0200] "GET /foo HTTP/1.1" 200 210 "-" "My Browser"\n')
        f.write('192.168.0.1 - - [16/Jan/2015:16:51:29 +0200] "GET /foo HTTP/1.1" 200 410 "-" "My Browser"\n')
        f.flush()

        src = nginx.NginxLogMetrics({
            'service': 'nginx',
            'hostname': 'localhost',
            'log_format': 'combined',
            'history': True,
            'file': log_file
        }, qb, duct_service)

        src.log.tmp = str(tmp_path / 'foo.log.lf')
        await src.get()

        ev1 = events[0]
        ev2 = events[1]

        for i in ev1:
            if i.service == 'nginx.client.192.168.0.1.bytes':
                assert i.metric == 210

        for i in ev2:
            if i.service == 'nginx.client.192.168.0.1.bytes':
                assert i.metric == 410

        events.clear()

        f.write('192.168.0.1 - - [16/Jan/2015:17:10:31 +0200] "GET /foo HTTP/1.1" 200 410 "-" "My Browser"\n')
        f.write('192.168.0.1 - - [16/Jan/2015:17:10:34 +0200] "GET /bar HTTP/1.1" 200 410 "-" "My Browser"\n')
        f.close()

        await src.get()

        for i in events[0]:
            if i.service == 'nginx.client.192.168.0.1.requests':
                assert i.metric == 2
            if i.service == 'nginx.user-agent.My Browser.bytes':
                assert i.metric == 820
            if i.service == 'nginx.request./foo.bytes':
                assert i.metric == 410


# ---------------------------------------------------------------------------
# Riak sources (uses a real HTTP server)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def riak_server():
    """Start an aiohttp HTTP server that returns fake Riak stats."""
    app = aiohttp.web.Application()
    stats_data = {}

    async def handler(request):
        return aiohttp.web.Response(
            text=json.dumps(stats_data),
            content_type='application/json')

    app.router.add_get('/', handler)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    port = runner.addresses[0][1]
    yield stats_data, port
    await runner.cleanup()


class TestRiakSources:
    @pytest.mark.asyncio
    async def test_riak_stats_zeros(self, duct_service, riak_server):
        stats_data, port = riak_server
        stats_data.update({'node_gets': 0, 'node_puts': 0})

        s = riak.RiakStats({
            'service': 'riak',
            'hostname': 'localhost',
            'url': 'http://127.0.0.1:%s/' % port,
        }, _qb, duct_service)

        [gets, puts] = await s.get()
        assert gets.service == "riak.gets_per_second"
        assert gets.metric == 0.0
        assert puts.service == "riak.puts_per_second"
        assert puts.metric == 0.0

    @pytest.mark.asyncio
    async def test_riak_stats(self, duct_service, riak_server):
        stats_data, port = riak_server
        stats_data.update({'node_gets': 150, 'node_puts': 45})

        s = riak.RiakStats({
            'service': 'riak',
            'hostname': 'localhost',
            'url': 'http://127.0.0.1:%s/' % port,
        }, _qb, duct_service)

        [gets, puts] = await s.get()
        assert gets.service == "riak.gets_per_second"
        assert gets.metric == 2.5
        assert puts.service == "riak.puts_per_second"
        assert puts.metric == 0.75

# ---------------------------------------------------------------------------
# NATS sources
# ---------------------------------------------------------------------------

def _make_source_event(**overrides):
    defaults = dict(
        state="ok", service="cpu.load", description="load avg",
        metric=0.42, ttl=60.0, tags=["prod"],
        hostname="sender.host", attributes={"core": "0"},
        evtime=1_700_000_000.0,
    )
    defaults.update(overrides)
    return Event(**defaults)


class TestNATSSource:
    @pytest.mark.asyncio
    async def test_nats_source_senml(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats({"service": "test", "topics": ["sensors.>"]}, _qb, duct_service)
        await source.startTimer()

        ev = _make_source_event(service="cpu.load", hostname="sender.host")
        await source.nc.publish("sensors.cpu.load", event_to_senml(ev))

        assert len(event_queue) == 1
        assert event_queue[0].service == "cpu.load"
        assert event_queue[0].hostname == "sender.host"

    @pytest.mark.asyncio
    async def test_nats_source_senml_full_roundtrip(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats({"service": "test", "topics": ["metrics.>"]}, _qb, duct_service)
        await source.startTimer()

        ev = _make_source_event(state="warning", description="spiking",
                                tags=["a", "b"], attributes={"zone": "eu"})
        await source.nc.publish("metrics.cpu.load", event_to_senml(ev))

        assert len(event_queue) == 1
        decoded = event_queue[0]
        assert decoded.state == "warning"
        assert decoded.description == "spiking"
        assert decoded.tags == ["a", "b"]
        assert decoded.attributes["zone"] == "eu"
        assert decoded.metric == pytest.approx(ev.metric)

    @pytest.mark.asyncio
    async def test_nats_source_cbor(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats(
            {"service": "test", "topics": ["sensors.>"], "format": "senml-cbor"},
            _qb, duct_service,
        )
        await source.startTimer()

        ev = _make_source_event(service="disk.read", metric=1024.0)
        await source.nc.publish("sensors.disk.read", event_to_senml_cbor(ev))

        assert len(event_queue) == 1
        assert event_queue[0].service == "disk.read"
        assert event_queue[0].metric == pytest.approx(1024.0)

    @pytest.mark.asyncio
    async def test_nats_source_json(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats(
            {"service": "test", "topics": ["sensors.>"], "format": "json"},
            _qb, duct_service,
        )
        await source.startTimer()

        ev = _make_source_event(service="net.rx", metric=999.0, hostname="h")
        await source.nc.publish("sensors.net.rx", event_to_json(ev))

        assert len(event_queue) == 1
        assert event_queue[0].service == "net.rx"
        assert event_queue[0].hostname == "h"

    @pytest.mark.asyncio
    async def test_nats_source_malformed_message_does_not_crash(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats({"service": "test", "topics": ["sensors.>"]}, _qb, duct_service)
        await source.startTimer()

        await source.nc.publish("sensors.foo", b"this is not valid json or senml")

        assert len(event_queue) == 0

    @pytest.mark.asyncio
    async def test_nats_source_multiple_messages(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats({"service": "test", "topics": ["sensors.>"]}, _qb, duct_service)
        await source.startTimer()

        for i in range(5):
            ev = _make_source_event(service=f"metric.{i}", metric=float(i))
            await source.nc.publish(f"sensors.host.metric.{i}", event_to_senml(ev))

        assert len(event_queue) == 5
        services = {e.service for e in event_queue}
        assert services == {f"metric.{i}" for i in range(5)}

    @pytest.mark.asyncio
    async def test_nats_source_jetstream(self, duct_service):
        nats_src_mod.nats = FakeNATS()

        event_queue = []
        def _qb(source, events):
            event_queue.extend(events)

        source = nats_src_mod.Nats(
            {"service": "test", "topics": ["stream.>"], "jetstream": True, "durable": "myapp"},
            _qb, duct_service,
        )
        await source.startTimer()

        # For JetStream, the FakeJetStream shares the nc subscription map.
        # Publish via the fake JS to trigger the callback.
        ev = _make_source_event(service="temp", metric=23.5)
        await source.nc.js.publish("stream.temp", event_to_senml(ev))

        assert len(event_queue) == 1
        assert event_queue[0].service == "temp"

    def test_nats_source_tls_context_none_when_unconfigured(self, duct_service):
        source = nats_src_mod.Nats({"service": "test"}, lambda *a: None, duct_service)
        assert source._build_tls_context() is None

    def test_nats_source_tls_context_built(self, duct_service):
        source = nats_src_mod.Nats({
            "service": "test",
            "tls_ca_file": "/ca.pem",
            "tls_cert_file": "/cert.pem",
            "tls_key_file": "/key.pem",
        }, lambda *a: None, duct_service)
        mock_ctx = mock.MagicMock()
        with mock.patch("ssl.create_default_context", return_value=mock_ctx):
            ctx = source._build_tls_context()
        assert ctx is mock_ctx
        mock_ctx.load_verify_locations.assert_called_once_with(cafile="/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(certfile="/cert.pem", keyfile="/key.pem")

    @pytest.mark.asyncio
    async def test_nats_source_credentials_passed(self, duct_service):
        captured = {}

        class CapturingFakeNATS(FakeNATS):
            async def connect(self, servers=[], **kw):
                captured.update(kw)
                return self

        nats_src_mod.nats = CapturingFakeNATS()
        source = nats_src_mod.Nats(
            {"service": "test", "credentials_file": "/creds.creds"},
            lambda *a: None, duct_service,
        )
        await source.startTimer()
        assert captured.get("user_credentials") == "/creds.creds"

# ---------------------------------------------------------------------------
# Sensors
# ---------------------------------------------------------------------------

class TestSensorSources:
    @pytest.mark.asyncio
    async def test_ds18b20(self, duct_service, tmp_path):

        def add_fake_sensor(sensor, data):
            try:
                os.mkdir(tmp_path/sensor)
            except FileExistsError:
                pass
            with open(tmp_path/sensor/'w1_slave', 'w+b') as f:
                f.write(data.encode('ascii'))

        test_sensor = "28-94ebc5356461"
        add_fake_sensor(test_sensor,
                        'c0 01 55 00 7f ff 0c 10 13 : crc=13 YES\nc0 01 55 00 7f ff 0c 10 13 t=28000\n')

        test_sensor2 = "28-67dfc5356461"
        add_fake_sensor(test_sensor2,
                        '93 01 55 00 7f ff 0c 10 cb : crc=cb YES\n93 01 55 00 7f ff 0c 10 cb t=25187\n')

        source = ds18b20.DS18B20({
            "service": "temp",
            "device_path": tmp_path
        }, _qb, duct_service)

        events = await source.get()
        for ev in events:
            if ev.service == f"temp.{test_sensor}":
                assert ev.metric == 28.0
            else:
                assert ev.metric == 25.187

        source = ds18b20.DS18B20({
            "service": "temp",
            "device_path": tmp_path,
            "device_map": {test_sensor2: "test2"},
            "ignore_unmapped": True
        }, _qb, duct_service)

        events = await source.get()

        assert events[0].service == 'temp.test2'