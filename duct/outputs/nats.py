"""
.. module:: nats
   :synopsis: Output which sends events to NATS topics

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging
import asyncio
import ssl

import nats
from nats.aio.client import Client as NATS

from duct.objects import Output, Event
from duct.protocol.senml import event_to_senml, event_to_senml_cbor, event_to_json


log = logging.getLogger(__name__)


class Nats(Output):
    """NATS output

    :param servers: List of NATS URIs (default: ["nats://localhost:4222"])
    :type servers: list

    :param prefix: Prefix added to topics (default: "")
    :type prefix: str

    :param format: Serialisation format - json, senml-json, senml-cbor (default: senml-json)
    :type format: str

    :param interval: Queue drain interval in seconds (default: 1.0)
    :type interval: float

    :param jetstream: Publish via JetStream instead of core NATS (default: false)
    :type jetstream: bool

    :param credentials_file: Path to NATS credentials (.creds) file
    :type credentials_file: str

    :param nkey_seed_file: Path to NKey seed file
    :type nkey_seed_file: str

    :param tls_ca_file: Path to CA certificate file for TLS
    :type tls_ca_file: str

    :param tls_cert_file: Path to client certificate file for mTLS
    :type tls_cert_file: str

    :param tls_key_file: Path to client private key file for mTLS
    :type tls_key_file: str

    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._tick_task = None

        self.servers = self.config.get("servers", ["nats://localhost:4222"])
        self.prefix = self.config.get("prefix", "")
        self.format = self.config.get("format", "senml-json")
        self.inter = float(self.config.get("interval", 1.0))
        self.use_jetstream = bool(self.config.get("jetstream", False))

        self.credentials_file = self.config.get("credentials_file")
        self.nkey_seed_file = self.config.get("nkey_seed_file")
        self.tls_ca_file = self.config.get("tls_ca_file")
        self.tls_cert_file = self.config.get("tls_cert_file")
        self.tls_key_file = self.config.get("tls_key_file")

        self.transformers = {
            "senml-json": event_to_senml,
            "senml-cbor": event_to_senml_cbor,
            "json": event_to_json,
        }

        self.nc = None
        self.js = None

    def _build_tls_context(self):
        if not (self.tls_ca_file or self.tls_cert_file):
            return None
        ctx = ssl.create_default_context()
        if self.tls_ca_file:
            ctx.load_verify_locations(cafile=self.tls_ca_file)
        if self.tls_cert_file and self.tls_key_file:
            ctx.load_cert_chain(certfile=self.tls_cert_file, keyfile=self.tls_key_file)
        return ctx

    async def _on_disconnect(self):
        log.warning("NATS disconnected")

    async def _on_reconnect(self):
        log.info("NATS reconnected")

    async def stop(self):
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass

        if self.nc:
            log.info("Disconnecting NATS")
            await self.nc.drain()
            await self.nc.close()

    async def _tick(self):
        if not self.events:
            return
        events = self.events
        self.events = []
        await self.sendEvents(events)

    def _transform_event(self, event: Event) -> bytes:
        return self.transformers[self.format](event)

    async def sendEvents(self, events: list[Event]):
        "Send batches of events to NATS or JetStream"
        for ev in events:
            if self.prefix:
                topic = f"{self.prefix}.{ev.hostname}.{ev.service}"
            else:
                topic = f"{ev.hostname}.{ev.service}"
            payload = self._transform_event(ev)
            try:
                if self.use_jetstream:
                    await self.js.publish(topic, payload)
                else:
                    await self.nc.publish(topic, payload)
            except Exception:
                log.exception("Failed to publish event to topic %s", topic)

    async def _drain_loop(self):
        try:
            while True:
                await asyncio.sleep(self.inter)
                await self._tick()
        except asyncio.CancelledError:
            pass

    async def createClient(self) -> NATS:
        log.info("Connecting to NATS: %s", self.servers)

        connect_kwargs = dict(
            servers=self.servers,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            reconnect_time_wait=1,
            disconnected_cb=self._on_disconnect,
            reconnected_cb=self._on_reconnect,
        )

        tls_ctx = self._build_tls_context()
        if tls_ctx:
            connect_kwargs["tls"] = tls_ctx

        if self.credentials_file:
            connect_kwargs["user_credentials"] = self.credentials_file
        elif self.nkey_seed_file:
            connect_kwargs["nkeys_seed"] = self.nkey_seed_file

        self.nc = await nats.connect(**connect_kwargs)

        if self.use_jetstream:
            self.js = self.nc.jetstream()
            log.info("Connected to NATS (JetStream mode)")
        else:
            log.info("Connected to NATS")

        self._tick_task = asyncio.create_task(self._drain_loop())
