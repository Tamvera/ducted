"""
.. module:: i2c
   :synopsis: I2C async transaction wrapper

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio
import glob
from contextlib import asynccontextmanager
from functools import lru_cache

from smbus2 import SMBus, i2c_msg


@lru_cache
def list_buses() -> list[int]:
    "Lists available i2c busses"
    return sorted(
        int(p.replace('/dev/i2c-', ''))
        for p in glob.glob('/dev/i2c-*')
    )


_locks: dict[int, asyncio.Lock] = {}


def _bus_lock(bus: int) -> asyncio.Lock:
    if bus not in _locks:
        _locks[bus] = asyncio.Lock()
    return _locks[bus]


class I2CTransaction:
    """MPL115 temperature and pressure sensor

    **Arguments:**
    :param smbus: Bus number (default 1)
    :type smbus: int.
    :param device: I2C device address
    :type device: int.
    """

    def __init__(self, bus: int, device: int):
        self._smbus = SMBus(bus)
        self._device = device

    def close(self):
        "Close the transaction"
        self._smbus.close()

    async def _run(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, fn, *args)

    async def write(self, data: bytes):
        "Write raw data"
        msg = i2c_msg.write(self._device, data)
        await self._run(self._smbus.i2c_rdwr, msg)

    async def read(self, n_bytes: int) -> bytes:
        "Read n_bytes"
        msg = i2c_msg.read(self._device, n_bytes)
        await self._run(self._smbus.i2c_rdwr, msg)
        return bytes(msg)

    async def transfer(self, write_data: bytes, read_n: int) -> bytes:
        """Write then immediately read in a single combined transaction."""
        w = i2c_msg.write(self._device, write_data)
        r = i2c_msg.read(self._device, read_n)
        await self._run(self._smbus.i2c_rdwr, w, r)
        return bytes(r)

    async def read_byte(self) -> int:
        "Read one byte"
        return await self._run(self._smbus.read_byte, self._device)

    async def write_byte(self, value: int):
        "Write one byte"
        await self._run(self._smbus.write_byte, self._device, value)

    async def read_byte_data(self, register: int) -> int:
        "Read a register byte"
        return await self._run(self._smbus.read_byte_data, self._device, register)

    async def write_byte_data(self, register: int, value: int):
        "Write a register byte"
        await self._run(self._smbus.write_byte_data, self._device, register, value)

    async def read_block(self, register: int, length: int) -> list[int]:
        "Read a register block"
        return await self._run(self._smbus.read_i2c_block_data, self._device, register, length)

    async def write_block(self, register: int, data: list[int]):
        "Write a register block"
        await self._run(self._smbus.write_i2c_block_data, self._device, register, data)


@asynccontextmanager
async def transaction(bus: int, device: int):
    """Acquire exclusive access to a bus and yield an I2CTransaction bound to device.

    Callers on the same bus number are queued and served in order;
    callers on different buses run concurrently.
    """
    async with _bus_lock(bus):
        iic = I2CTransaction(bus, device)
        try:
            yield iic
        finally:
            iic.close()
