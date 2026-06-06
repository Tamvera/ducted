import asyncio
import json
import os
import time

import pytest

from duct.protocol import riemann, elasticsearch, opentsdb
from duct.objects import Event
from duct.utils import fork, Timeout
from duct.configuration import ConfigFile
from duct.tests import globs


class TestConfig:
    def test_config_parser(self, tmp_path):
        if not os.path.exists(tmp_path/'testdir'):
            os.mkdir(tmp_path/'testdir')
        with open(tmp_path/'testdir/test.yaml', 'wt') as f:
            f.write(globs.CONFIG_INCLUDE)

        with open(tmp_path/'testconf.yaml', 'wt') as f:
            f.write(globs.CONFIG_TEST.replace('testdir', str(tmp_path/'testdir')))

        c = ConfigFile(str(tmp_path/'testconf.yaml'))

        merged = c.get('mergehash')
        assert merged['test3']['foo'] == 'bar'
        assert merged['test']['bar'] == 'baz'
        assert merged['test'].get('foo') is None

        sources = c.get('sources')

        host3 = [i for i in sources if i.get('hostname') == 'test3']
        assert len(host3) == 4

        host3_ssh = [i for i in sources if
                     i.get('hostname') == 'test3' and i.get('use_ssh') is True]
        assert len(host3_ssh) == 2


class TestRiemannProtobuf:
    def test_riemann_protobuf(self):
        proto = riemann.RiemannProtobufMixin()
        event = Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0)
        message = proto.encodeMessage([event])
        assert isinstance(message, bytes)

    def test_riemann_protobuf_with_attributes(self):
        proto = riemann.RiemannProtobufMixin()
        event = Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0,
                      attributes={"chicken": "little"})
        e = proto.encodeEvent(event)
        attrs = e.attributes
        assert len(attrs) == 1
        assert attrs[0].key == "chicken"
        assert attrs[0].value == "little"


class TestElasticsearchProto:
    @pytest.mark.asyncio
    async def test_elasticsearch_proto(self):
        proto = elasticsearch.ElasticSearch()
        last_request = {}

        async def _wrap_request(path, data=None, method='GET'):
            last_request['args'] = (path, data, method)
            return {"errors": []}

        proto._request = _wrap_request

        index = proto._get_index()

        t = time.strptime(index, "duct-%Y.%m.%d")
        assert time.gmtime().tm_year == t.tm_year
        assert time.gmtime().tm_mday == t.tm_mday
        assert time.gmtime().tm_mon == t.tm_mon

        event = Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0,
                      attributes={"chicken": "little"})

        ans = await proto.bulkIndex([dict(event)])

        assert ans['errors'] == []

        meta, metric = last_request['args'][1].strip('\n').split('\n')
        request_meta = json.loads(meta)
        request_data = json.loads(metric)

        assert last_request['args'][0] == '/_bulk'
        assert request_meta['index']['_index'] == index
        assert request_data['service'] == 'sky'


class TestFork:
    @pytest.mark.asyncio
    async def test_utils_fork(self):
        o, e, c = await fork('echo', args=('hi',))
        assert o == "hi\n"
        assert c == 0

    @pytest.mark.asyncio
    async def test_utils_fork_timeout(self):
        with pytest.raises(Timeout):
            await fork('sleep', args=('2',), timeout=0.1)
