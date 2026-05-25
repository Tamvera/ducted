"""
.. module:: sensors
   :platform: unix
   :synopsis: Provides checks for system sensors and SMART devices

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import os

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source


@implementer(IDuctSource)
class Sensors(Source):
    """Returns hwmon sensors info

    Note: There is no transformation done on values, they may be in
    thousands

    **Metrics:**

    :(service name).(adapter).(sensor): Sensor value
    """

    def _find_sensors(self):
        path = '/sys/class/hwmon'
        sensors = {}
        # Find adapters
        if os.path.exists(path):
            monitors = os.listdir(path)

            for hwmons in monitors:
                mon_path = os.path.join(path, hwmons)
                name_path = os.path.join(mon_path, 'name')
                if os.path.exists(name_path):
                    with open(name_path, 'rt', encoding='utf-8') as name_file:
                        name = name_file.read().strip()

                else:
                    name = None

                if name not in sensors:
                    sensors[name] = {}

                sensor_map = {}

                # Find sensors in this adapter
                for mon_file in os.listdir(mon_path):
                    if mon_file.startswith('temp') or mon_file.startswith(
                            'fan'):
                        tn = mon_file.split('_')[0]
                        sensor_path = os.path.join(mon_path, mon_file)

                        if tn not in sensor_map:
                            sensor_map[tn] = [None, 0]

                        if mon_file.endswith('_input'):
                            with open(sensor_path, 'rt',
                                      encoding='utf-8') as value_file:
                                value = int(value_file.read().strip())

                                if mon_file.startswith('temp'):
                                    value = value / 1000.0

                                sensor_map[tn][1] = value

                        if mon_file.endswith('_label'):
                            with open(sensor_path, 'rt',
                                      encoding='utf-8') as value_file:
                                sensor_name = value_file.read().strip()
                                sensor_map[tn][0] = sensor_name

                for sensor_name, value in sensor_map.values():
                    if sensor_name:
                        filtered_name = sensor_name.lower().replace(' ', '_')
                        sensors[name][filtered_name] = value
        return sensors

    async def get(self):
        sensors = self._find_sensors()

        events = []

        for adapter, v in sensors.items():
            for sensor, val in v.items():
                events.append(
                    self.createEvent('ok',
                                     f'Sensor {adapter}:{sensor} - {val}',
                                     val,
                                     prefix=f'{adapter}.{sensor}'))
        return events

@implementer(IDuctSource)
class LMSensors(Source):
    """Returns lm-sensors output

    This does the exact same thing as the Sensors class but uses lm-sensors.

    **Metrics:**

    :(service name).(adapter).(sensor): Sensor value
    """
    ssh = True

    async def _get_sensors(self):
        out, _err, code = await self.fork('/usr/bin/sensors')
        if code == 0:
            return out.strip('\n').split('\n')
        else:
            return []

    def _parse_sensors(self, sensors):
        adapters = {}
        adapter = None
        for i in sensors:
            l = i.strip()
            if not l:
                continue

            if ':' in l:
                n, v = l.split(':')
                vals = v.strip().split()

                if n == 'Adapter':
                    continue

                if '\xc2\xb0' in vals[0]:
                    val = vals[0].split('\xc2\xb0')[0]
                elif len(vals) > 1:
                    val = vals[0]
                else:
                    continue

                val = float(val)

                adapters[adapter][n] = val

            else:
                adapter = l
                adapters[adapter] = {}

        return adapters

    async def get(self):
        sensors = await self._get_sensors()
        adapters = self._parse_sensors(sensors)

        events = []

        for adapter, v in adapters.items():
            for sensor, val in v.items():
                events.append(
                    self.createEvent('ok',
                                     f'Sensor {adapter}:{sensor} - {val}',
                                     val,
                                     prefix=f'{adapter}.{sensor}'))

        return events

@implementer(IDuctSource)
class SMART(Source):
    """Returns SMART output for all disks

    **Metrics:**

    :(service name).(disk).(sensor): Sensor value
    """

    ssh = True

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.devices = []

    async def _get_disks(self):
        out, _err, code = await self.fork('/usr/sbin/smartctl',
                                          args=('--scan',))

        if code != 0:
            return []

        out = out.strip('\n').split('\n')
        devices = []
        for ln in out:
            if '/dev' in ln:
                devices.append(ln.split()[0])

        return devices

    async def _get_smart(self, device):
        out, _err, code = await self.fork('/usr/sbin/smartctl',
                                          args=('-A', device))

        if code == 0:
            return out.strip('\n').split('\n')
        else:
            return []

    def _parse_smart(self, smart):
        mark = False
        attributes = {}
        for l in smart:
            ln = l.strip('\n').strip()
            if not ln:
                continue

            if mark:
                (_id, attribute, _flag, _val, _worst, _thresh, _type, _u, _wf,
                 raw) = ln.split(None, 9)

                try:
                    raw = int(raw.split()[0])
                    attributes[attribute.replace('_', ' ')] = raw
                except:
                    pass

            if ln[:3] == 'ID#':
                mark = True

        return attributes

    async def get(self):
        if not self.devices:
            self.devices = await self._get_disks()

        events = []

        for disk in self.devices:
            smart = await self._get_smart(disk)
            stats = self._parse_smart(smart)

            for sensor, val in stats.items():
                events.append(
                    self.createEvent('ok',
                                     f'Attribute {disk}:{sensor} - {val}',
                                     val,
                                     prefix=f'{disk}.{sensor}')
                )

        return events
