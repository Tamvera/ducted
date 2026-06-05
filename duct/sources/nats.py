"""
.. module:: nats
   :synopsis: Source which subscribes to NATS topics for events

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging
import asyncio

from zope.interface import implementer

from duct.objects import Output, Event, Source
from duct.protocol.senml import senml_to_event

from duct.interfaces import IDuctSource

import nats
from nats.aio.client import Client as NATS

log = logging.getLogger(__name__)

@implementer(IDuctSource)
class Nats(Source):
    """NATS source

    **Configuration arguments:**

    :param servers: List of NATS URIs (default: ["nats://localhost:4222"])
    :type servers: list
    :param topics: List of topics to subscribe to (default: [">"])
    :type topics: list
    :param format: Serialisation format, one of json, senml-json, senml-cbor (default: senml-json)
    :type format: str

    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._loop_task = None

        self.servers = self.config.get("servers", ["nats://localhost:4222"])
        self.prefix = self.config.get("prefix", "")

        self.format = self.config.get("format", "senml-json")

        self.topics = self.config.get("topics", [">"])

        self.transformers = {
            "senml-json": senml_to_event
        }

        self.subscriptions = []

        # TODO: Add TLS support

        self.nc = None

    async def get(self):
        "Uses async queue"
        pass 

    async def startTimer(self):
        log.info(f"Connecting to NATS: {self.servers}")

        self.nc: NATS = await nats.connect(
            servers=self.servers,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            reconnect_time_wait=1,
        )

        for topic in self.topics:
            log.info(f"Subscribing to topic '{topic}'")
            sub = await self.nc.subscribe(topic, cb=self._get_event)
            self.subscriptions.append(sub)

        log.info(f"Connected to NATS successfully")

    async def stopTimer(self):
        if self.nc:
            log.info(f"Disconnecting NATS")
            await self.nc.drain()
            await self.nc.close()

    async def _get_event(self, message):
        ev = self.transformers[self.format](message.data.decode())
        self.queueBack([ev])