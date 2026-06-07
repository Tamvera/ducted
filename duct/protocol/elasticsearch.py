"""
.. module:: elasticsearch
   :synopsis: Elasticsearch protocol module
   :no-index:

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import time
import uuid
import json
from base64 import b64encode

from duct import utils

class ElasticSearch(object):
    """Elasticsearch HTTP API client"""

    def __init__(self, url='http://localhost:9200', user=None, password=None,
                 index='duct-%Y.%m.%d'):
        self.url = url.rstrip('/')
        self.index = index
        self.user = user
        self.password = password

    def _get_index(self):
        return time.strftime(self.index)

    async def _request(self, path, data=None, method='GET'):
        headers = {}
        if self.user:
            authorization = b64encode(
                f'{self.user}:{self.password}'.encode()
            ).decode()
            headers['Authorization'] = 'Basic ' + authorization

        body = data.encode() if isinstance(data, str) else data
        return await utils.HTTPRequest().getJson(
            self.url + path, method, headers=headers, data=body)

    def _gen_id(self):
        return b64encode(uuid.uuid4().bytes).decode().rstrip('=')

    async def stats(self):
        """Return statistics for the cluster
        """
        return await self._request('/_cluster/stats')

    async def node_stats(self):
        """Return statistics for this node
        """
        return await self._request('/_nodes/stats')

    async def insertIndex(self, index_type, data):
        """Insert an index
        """
        return await self._request(
            f'/{self._get_index()}/{index_type}/{self._gen_id()}',
            json.dumps(data), 'PUT')

    async def bulkIndex(self, rows):
        """Insert many indicies
        """
        serdata = ""

        for row in rows:
            if '_id' in row:
                iid = row['id']
                row.pop('id')
            else:
                iid = self._gen_id()

            meta = {
                "index": {
                    "_index": self._get_index(),
                    "_type": row.get('type', 'event'),
                    "_id": iid,
                }
            }

            serdata += json.dumps(meta) + '\n'
            serdata += json.dumps(row) + '\n'

        return await self._request('/_bulk', serdata, 'PUT')
