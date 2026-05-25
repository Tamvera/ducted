"""
.. module:: environment
   :platform: Any
   :synopsis: Sources for interfacing with environmental sensors

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.utils import wait


# MPL115A2 Register Address
PADC_MSB = 0x00
PADC_LSB = 0x01
TADC_MSB = 0x02
TACD_LSB = 0x03
A0_MSB = 0x04
A0_LSB = 0x05
B1_MSB = 0x06
B1_LSB = 0x07
B2_MSB = 0x08
B2_LSB = 0x09
C12_MSB = 0x0A
C12_LSB = 0x0B
CONVERT = 0x12


@implementer(IDuctSource)
class MPL115(Source):
    """MPL115 temperature and pressure sensor

    This source was based on:
    https://github.com/FaBoPlatform/FaBoBarometer-MPL115-Python

    **Configuration arguments:**

    :param address: I2C address of the sensor (default 0x60)
    :type dx: hex.
    :param smbus: Bus number (default 1)
    :type smbus: int.
    :param altitude: Altitude for correction (default 0)
    :type altitude: float.
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        try:
            import smbus  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "MPL115 source requires python-smbus (smbus-cffi)") from exc

        self.address = self.config.get('address', 0x60)
        self.bus = smbus.SMBus(self.config.get('smbus', 1))
        self.altitude = self.config.get('altitude', 0.0)

        self.readCoefficients()

    async def get(self):
        data = await self.readData()

        return [
            self.createEvent('ok', 'HPA', round(data['hpa'], 2), prefix='hpa'),
            self.createEvent('ok', 'Temperature', round(data['temp'], 2),
                             prefix='temp')
        ]

    def readCoefficients(self):
        """Read coefficients from sensor"""
        def _convert(data1, data2):
            value = data1 | (data2 << 8)
            if value & (1 << 16 - 1):
                value -= (1 << 16)
            return value

        data = self.bus.read_i2c_block_data(self.address, A0_MSB, 8)

        self.a0 = _convert(data[1], data[0])
        self.b1 = _convert(data[3], data[2])
        self.b2 = _convert(data[5], data[4])
        self.c12 = _convert(data[7], data[6])

        self.a0 = float(self.a0) / (1 << 3)
        self.b1 = float(self.b1) / (1 << 13)
        self.b2 = float(self.b2) / (1 << 14)
        self.c12 = float(self.c12) / (1 << 24)

    async def readData(self):
        """Read data from the sensor
           returns dict containing 'hpa' and 'temp' values
        """
        self.bus.write_byte_data(self.address, CONVERT, 0x01)

        await wait(3)

        data = self.bus.read_i2c_block_data(self.address, PADC_MSB, 4)

        padc = ((data[0] << 8) | data[1]) >> 6
        tadc = ((data[2] << 8) | data[3]) >> 6

        pcomp = self.a0 + (self.b1 + self.c12 * tadc) * padc + self.b2 * tadc

        hpa = pcomp * ((1150.0 - 500.0) / 1023.0) + 500.0
        hpa /= pow(1.0 - (self.altitude / 44330.0), 5.255)

        temp = 25.0 - (tadc - 512.0) / 5.35

        return {'hpa': hpa, 'temp': temp}
