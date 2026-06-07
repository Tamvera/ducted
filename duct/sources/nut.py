"""
.. module:: nut
   :platform: Unix
   :synopsis: A source module for NUT (Network UPS Tools) stats

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

log = logging.getLogger(__name__)

# NUT vars to collect, mapped to (metric suffix, human description)
_UPS_METRICS = {
    'battery.charge':      ('battery.charge',      'Battery charge percent'),
    'battery.charge.low':  ('battery.charge.low',  'Battery low charge threshold'),
    'battery.runtime':     ('battery.runtime',     'Battery runtime remaining (s)'),
    'battery.voltage':     ('battery.voltage',     'Battery voltage'),
    'input.voltage':       ('input.voltage',       'Input voltage'),
    'input.frequency':     ('input.frequency',     'Input frequency'),
    'output.voltage':      ('output.voltage',      'Output voltage'),
    'output.frequency':    ('output.frequency',    'Output frequency'),
    'ups.load':            ('ups.load',            'UPS load percent'),
    'ups.temperature':     ('ups.temperature',     'UPS temperature'),
    'ups.realpower':       ('ups.realpower',       'Real power (W)'),
    'ups.power':           ('ups.power',           'Apparent power (VA)'),
}


async def _nut_list_vars(host, port, ups, timeout=10):
    """Open a TCP connection to a NUT server and return all VAR values for ups."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        writer.write(f'LIST VAR {ups}\n'.encode())
        await writer.drain()

        data = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            line = line.decode().strip()
            if not line:
                continue
            if line.startswith('ERR '):
                raise IOError(line)
            if line.startswith('VAR '):
                # VAR <ups> <key> "<value>"
                parts = line.split(' ', 3)
                if len(parts) == 4:
                    key = parts[2]
                    value = parts[3].strip('"')
                    data[key] = value
            elif line.startswith('END LIST VAR'):
                break
        return data
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


@implementer(IDuctSource)
class UPS(Source):
    """Query UPS statistics directly from a NUT server over TCP

    :param ups: UPS name as configured in NUT (default: 'ups')
    :type ups: str.
    :param host: NUT server host (default: 'localhost')
    :type host: str.
    :param port: NUT server port (default: 3493)
    :type port: int.
    :param timeout: Connection timeout in seconds (default: 10)
    :type timeout: int.

    **Metrics:**

    :(service_name).battery.charge: Battery charge percentage
    :(service_name).battery.runtime: Battery runtime remaining in seconds
    :(service_name).battery.voltage: Battery voltage
    :(service_name).input.voltage: Input mains voltage
    :(service_name).input.frequency: Input mains frequency
    :(service_name).output.voltage: Output voltage
    :(service_name).ups.load: UPS load percentage
    :(service_name).ups.temperature: UPS temperature (if supported)
    :(service_name).ups.realpower: Real power in watts (if supported)
    :(service_name).ups.status: UPS status string (OL, OB, LB, etc.)
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)
        self.ups = self.config.get('ups', 'ups')
        self.nut_host = self.config.get('host', 'localhost')
        self.nut_port = int(self.config.get('port', 3493))
        self.timeout = int(self.config.get('timeout', 10))

    def _status_state(self, status):
        """Map NUT ups.status flags to a duct event state."""
        if not status:
            return 'unknown'
        if 'LB' in status or 'OB' in status:
            return 'critical'
        if 'ALARM' in status or 'OVER' in status:
            return 'warning'
        return 'ok'

    async def get(self):
        try:
            data = await _nut_list_vars(
                self.nut_host, self.nut_port, self.ups, self.timeout
            )
        except asyncio.TimeoutError:
            msg = f'Timeout connecting to NUT at {self.nut_host}:{self.nut_port}'
            log.warning(msg)
            return self.createEvent('critical', msg, None)
        except Exception as exc:
            msg = f'NUT error ({self.nut_host}:{self.nut_port} ups={self.ups}): {exc}'
            log.warning(msg)
            return self.createEvent('critical', msg, None)

        status = data.get('ups.status', '')
        state = self._status_state(status)

        events = [
            self.createEvent(state, f'UPS status: {status or "unknown"}',
                             None, prefix='ups.status')
        ]

        for var, (suffix, description) in _UPS_METRICS.items():
            raw = data.get(var)
            if raw is None:
                continue
            try:
                value = float(raw)
            except ValueError:
                continue
            events.append(self.createEvent(state, description, value, prefix=suffix))

        return events
