"""
.. module:: raritan_px
   :platform: Unix
   :synopsis: A source module for Raritan PX PDU monitoring via SNMP/PDU2-MIB

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.sources.snmp import SNMP

log = logging.getLogger(__name__)

_SENSOR_MAP = {
    'rmsVoltage':    (int,   'voltage'),
    'rmsCurrent':    (float, 'current'),
    'activePower':   (int,   'power'),
    'apparentPower': (int,   'apparent_power'),
    'powerFactor':   (float, 'pf'),
    'activeEnergy':  (int,   'energy'),
}

_TABLES = [
    ('measurementsOutletSensorValue', 'outlet'),
    ('measurementsInletSensorValue',  'inlet'),
]


@implementer(IDuctSource)
class RaritanPX(SNMP):
    """Monitors a Raritan PX PDU via SNMP using the PDU2-MIB.

    Requires compiled PDU2-MIB Python modules in ``mib_path``.

    :param ip: PDU hostname or IP (default: 127.0.0.1)
    :type ip: str.
    :param port: SNMP port (default: 161)
    :type port: int.
    :param community: SNMP read community (default: public)
    :type community: str.
    :param mib_path: Directory containing compiled MIB modules (default: /var/lib/duct/mibs)
    :type mib_path: str.

    **Metrics emitted** (prefixed by service name):

    :outlet.N.voltage: Outlet N RMS voltage (V)
    :outlet.N.current: Outlet N RMS current (A)
    :outlet.N.power: Outlet N active power (W)
    :outlet.N.apparent_power: Outlet N apparent power (VA)
    :outlet.N.pf: Outlet N power factor
    :outlet.N.energy: Outlet N active energy (Wh)
    :inlet.N.*: Same metrics for inlet N
    """

    def __init__(self, *a, **kw):
        SNMP.__init__(self, *a, **kw)
        self.mib_path = self.config.get('mib_path', '/var/lib/duct/mibs')

    def _parse_sensor(self, prefix, oid, val):
        """Parse one sensor varBind into an event, or return None if unrecognised."""
        oid_str = oid.prettyPrint()

        if '::' in oid_str:
            # Symbolic form: "PDU2-MIB::measurementsOutletSensorValue.1.rmsVoltage"
            _, name_and_instance = oid_str.split('::', 1)
            parts = name_and_instance.split('.')
            # parts[0] = object name, parts[1:] = index components
            # last = sensor type, second-to-last = outlet/inlet index
            if len(parts) < 3:
                return None
            index = parts[-2]
            sensor_key = parts[-1]
        else:
            # Numeric OID fallback — last two components are index and sensor type
            parts = oid_str.split('.')
            if len(parts) < 2:
                return None
            index = parts[-2]
            sensor_key = parts[-1]

        if sensor_key not in _SENSOR_MAP:
            return None

        dtype, metric_name = _SENSOR_MAP[sensor_key]
        try:
            value = dtype(val)
        except (ValueError, TypeError):
            return None

        return self.createEvent(
            'ok',
            f'Raritan PX {prefix}-{index} {metric_name}={value}',
            value,
            prefix=f'{prefix}.{index}.{metric_name}',
        )

    async def get(self):
        events = []
        mib_dirs = [self.mib_path]

        for table, label in _TABLES:
            try:
                results = await self.snmp.walkMib('PDU2-MIB', table, mib_dirs=mib_dirs)
            except Exception as exc:
                log.warning('Raritan PX SNMP error querying %s: %s', table, exc)
                continue

            for oid, val in results:
                event = self._parse_sensor(label, oid, val)
                if event:
                    events.append(event)

        return events
