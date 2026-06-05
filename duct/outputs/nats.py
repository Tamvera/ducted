"""
.. module:: nats
   :synopsis: Output which sends events to NATS topics

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging
import asyncio

from duct.objects import Output, Event
from duct.protocol.senml import event_to_senml

import nats
from nats.aio.client import Client as NATS

log = logging.getLogger(__name__)


class Nats(Output):
    """NATS output

    **Configuration arguments:**

    :param servers: List of NATS URIs (default: ["nats://localhost:4222"])
    :type servers: list

    :param prefix: Prefix added to topics (default: "")
    :type prefix: str

    :param format: Serialisation format, one of json, senml-json, senml-cbor (default: senml-json)
    :type format: str

    :param interval: Queue check interval in seconds (default: 1.0)
    :type interval: int

    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._tick_task = None

        self.servers = self.config.get("servers", ["nats://localhost:4222"])
        self.prefix = self.config.get("prefix", "")

        self.format = self.config.get("format", "senml-json")

        self.inter = float(self.config.get('interval', 1.0))

        self.transformers = {
            "senml-json": event_to_senml
        }

        # TODO: Add TLS support

        self.nc = None

    async def stop(self):
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        
        log.info(f"Disconnecting NATS")
        await self.nc.drain()
        await self.nc.close()

    async def _tick(self):
        if not self.events:
            return
        events = self.events
        self.events = []
        await self.sendEvents(events)

    def _transform_event(self, event):
        ev =  self.transformers[self.format](event)
        return ev
    
    async def sendEvents(self, events: list[Event]):
        for ev in events:
            if self.prefix:
                topic = f"{self.prefix}.{ev.hostname}.{ev.service}"
            else:
                topic = f"{ev.hostname}.{ev.service}"
            await self.nc.publish(topic, self._transform_event(ev).encode("ascii"))

    async def _drain_loop(self):
        try:
            while True:
                await asyncio.sleep(self.inter)
                await self._tick()
        except asyncio.CancelledError:
            pass

    async def createClient(self) -> NATS:
        log.info(f"Connecting to NATS: {self.servers}")

        self.nc: NATS = await nats.connect(
            servers=self.servers,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            reconnect_time_wait=1,
        )
        log.info(f"Connected to NATS successfully")

        self._tick_task = asyncio.create_task(self._drain_loop())