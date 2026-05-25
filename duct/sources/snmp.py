"""
.. module:: snmp
   :platform: Unix
   :synopsis: A source module for polling SNMP. Requires PySNMP>=6.0

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import logging

from zope.interface import implementer

from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine, CommunityData, UdpTransportTarget, ContextData,
    ObjectType, ObjectIdentity, walk_cmd,
)
from pysnmp.proto import rfc1902

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct import aggregators

log = logging.getLogger(__name__)


class SNMPConnection(object):
    """A wrapper class for PySNMP asyncio functions.

    :param host: SNMP agent host
    :param port: SNMP port
    :param community: SNMP read community
    """

    def __init__(self, host, port, community):
        self.host = host
        self.port = port
        self.community = community
        self.engine = SnmpEngine()

    async def walk(self, oid):
        """Walk OIDs under `oid`. Returns a list of (oid, value) tuples."""
        results = []
        target = await UdpTransportTarget.create(
            (self.host, self.port), timeout=5, retries=2
        )
        async for (err_indication, err_status, err_index,
                   var_bind_table) in walk_cmd(
            self.engine,
            CommunityData(self.community, mpModel=0),
            target,
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
            lexicographicMode=False,
        ):
            if err_indication:
                log.warning('SNMP error: %s', err_indication)
                break
            if err_status:
                idx = err_index and var_bind_table[-1][int(err_index) - 1][0]
                log.warning('SNMP error status: %s at %s',
                            err_status.prettyPrint(), idx or '?')
                break
            for var_bind in var_bind_table:
                results.append((var_bind[0], var_bind[1]))

        return results


@implementer(IDuctSource)
class SNMP(Source):
    """Connects to an SNMP agent and retrieves OIDs.

    **Configuration arguments:**

    :param ip: SNMP agent host (default: 127.0.0.1)
    :type ip: str.
    :param port: SNMP port (default: 161)
    :type port: int.
    :param community: SNMP read community
    :type community: str.
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        host = self.config.get('ip', '127.0.0.1')
        port = int(self.config.get('port', 161))
        community = self.config.get('community', 'public')
        self.snmp = SNMPConnection(host, port, community)

    async def getCounter(self, soid):
        """Walk SNMP OID and return list of (oid, value) tuples."""
        return await self.snmp.walk(soid)

    async def getIfMetrics(self):
        """Retrieve interface metrics via SNMP and return Event list."""
        ifaces = await self.snmp.walk('1.3.6.1.2.1.2.2.1.2')

        table = [
            ('1.3.6.1.2.1.2.2.1.10', 'ifInOctets'),
            ('1.3.6.1.2.1.2.2.1.11', 'ifInUcastPkts'),
            ('1.3.6.1.2.1.2.2.1.12', 'ifInNUcastPkts'),
            ('1.3.6.1.2.1.2.2.1.14', 'ifInErrors'),
            ('1.3.6.1.2.1.2.2.1.13', 'ifInDiscards'),
            ('1.3.6.1.2.1.2.2.1.16', 'ifOutOctets'),
            ('1.3.6.1.2.1.2.2.1.17', 'ifOutUcastPkts'),
            ('1.3.6.1.2.1.2.2.1.18', 'ifOutNUcastPkts'),
            ('1.3.6.1.2.1.2.2.1.20', 'ifOutErrors'),
            ('1.3.6.1.2.1.2.2.1.19', 'ifOutDiscards'),
        ]

        data = {}
        for oid, key in table:
            d = await self.getCounter(oid)
            for i, (_, val) in enumerate(d):
                if val and i < len(ifaces):
                    iface = str(ifaces[i][1])
                    if iface not in data:
                        data[iface] = {}
                    data[iface][key] = val

        events = []
        for iface, metrics in data.items():
            for key, val in metrics.items():
                aggr = None
                if isinstance(val, rfc1902.Counter32):
                    aggr = aggregators.Counter32
                if isinstance(val, rfc1902.Counter64):
                    aggr = aggregators.Counter64

                events.append(self.createEvent(
                    'ok',
                    f'SNMP interface {iface} {key}={int(val):0.2f}',
                    int(val),
                    prefix=f'{iface}.{key}',
                    aggregation=aggr,
                ))

        return events

    async def get(self):
        return await self.getIfMetrics()


class SNMPCisco837(SNMP):
    """Connects to a Cisco 837 and makes metrics.

    **Configuration arguments:**

    :param ip: SNMP agent host (default: 127.0.0.1)
    :param port: SNMP port (default: 161)
    :param community: SNMP read community
    """

    async def get(self):
        events = await self.getIfMetrics()

        sync_us = (await self.snmp.walk('1.3.6.1.2.1.10.94.1.1.5'))[0][1]
        sync_ds = (await self.snmp.walk('1.3.6.1.2.1.10.94.1.1.4'))[0][1]

        sync_us = int(sync_us)
        sync_ds = int(sync_ds)

        events.append(self.createEvent(
            'ok', f'SNMP ADSL sync downstream {sync_ds}',
            sync_ds, prefix='adsl.rxrate'))

        events.append(self.createEvent(
            'ok', f'SNMP ADSL sync upstream {sync_us}',
            sync_us, prefix='adsl.txrate'))

        link = await self.snmp.walk('1.3.6.1.2.1.10.94.1.1.3')
        link = {str(i): j for i, j in link}

        output = int(link['1.3.6.1.2.1.10.94.1.1.3.1.7.15']) / 10.0
        attn = int(link['1.3.6.1.2.1.10.94.1.1.3.1.5.15']) / 10.0
        margin = int(link['1.3.6.1.2.1.10.94.1.1.3.1.4.15']) / 10.0

        events.append(self.createEvent(
            'ok', f'SNMP ADSL output power {output:0.2f}dBm',
            output, prefix='adsl.outpwr'))

        events.append(self.createEvent(
            'ok', f'SNMP ADSL attenuation {attn:0.2f}dB',
            attn, prefix='adsl.attn'))

        events.append(self.createEvent(
            'ok', f'SNMP ADSL noise margin {margin:0.2f}dB',
            margin, prefix='adsl.margin'))

        return events
