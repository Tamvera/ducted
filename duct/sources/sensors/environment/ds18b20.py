"""
.. module:: ds18b20
   :platform: Any
   :synopsis: DS18B20 sensor with 1-wire interface presented by /sys/bus/w1

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import os
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.utils import wait


log = logging.getLogger(__name__)


@implementer(IDuctSource)
class DS18B20(Source):
    """DS18B20 1-wire temperature sensor.
    This module reads sensors connected with the `wire` kernel module typically
    used on a Raspberry Pi with the w1-gpio device tree overlay.

    **Configuration arguments:**

    :param device_path: Path for w1_therm and w1_gpio devices
    :type device_path: str
    :param device_map: A dictionary structure re-mapping sensor serials to custom names
    :type device_map: dict
    :param ignore_unmapped: If True any sensor not listed in the device_map is
                            ignored (default: False)
    :type ignore_unmapped: bool
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

        self.device_path = self.config.get('device_path', '/sys/bus/w1/devices/')

        self.device_map = self.config.get('device_map', {})

        self.ignore_unmapped = self.config.get('ignore_unmapped', False)

    def read_temp(self, sensor_id):
        """Reads the temperature from a 1-Wire sensor device."""
        try:
            with open(os.path.join(self.device_path, sensor_id, 'w1_slave'), 'r') as f:
                lines = f.readlines()
            if lines[0].strip()[-3:] != 'YES':
                return None  # CRC check failed
            temp_str = lines[1].split('t=')[-1]
            return float(temp_str) / 1000.0
        except Exception as e:
            log.warning(f"Error reading sensor {sensor_id}: {e}")
            return None

    async def get(self):
        """Polls all sensors and sends metrics in a batch every POLL_INTERVAL seconds."""

        sensors = [d for d in os.listdir(self.device_path) if d.startswith('28-')]
        metrics = []

        for sensor_id in sensors:
            if self.ignore_unmapped and (sensor_id not in self.device_map):
                log.debug(f"Ignoring unmapped sensor {sensor_id}.")
                continue

            label = self.device_map.get(sensor_id, sensor_id)
            temp = self.read_temp(sensor_id)

            if temp is not None:
                metrics.append(self.createEvent('ok', f'{sensor_id} Temperature C', temp, prefix=label))

        return metrics