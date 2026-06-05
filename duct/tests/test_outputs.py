import json
import pytest
import pytest_asyncio

from duct.outputs import elasticsearch, opentsdb
from duct.objects import Event
from duct.service import DuctService

from .helpers import TestConfig


@pytest.fixture
def service():
    return DuctService(TestConfig({}))


@pytest.fixture
def event():
    return Event('ok', 'sky', 'Sky has not fallen', 1.0, 60.0,
                 attributes={"chicken": "little"})


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
