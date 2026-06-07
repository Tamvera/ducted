"""
.. module:: nats
   :synopsis: Source which subscribes to NATS topics for events
   :no-index:

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging
import ssl

from zope.interface import implementer

import nats
from nats.aio.client import Client as NATS

from duct.objects import Source
from duct.protocol.senml import senml_to_event, senml_cbor_to_event, json_to_event

from duct.interfaces import IDuctSource


log = logging.getLogger(__name__)


@implementer(IDuctSource)
class Nats(Source):
    """NATS source

    :param servers: List of NATS URIs (default: ["nats://localhost:4222"])
    :type servers: list

    :param topics: List of topics to subscribe to (default: [">"])
    :type topics: list

    :param format: Serialisation format - json, senml-json, senml-cbor (default: senml-json)
    :type format: str

    :param jetstream: Subscribe via JetStream with a durable consumer (default: false)
    :type jetstream: bool

    :param durable: Durable consumer name for JetStream subscriptions (default: "ducted")
    :type durable: str

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

        self.servers = self.config.get("servers", ["nats://localhost:4222"])
        self.topics = self.config.get("topics", [">"])
        self.format = self.config.get("format", "senml-json")
        self.use_jetstream = bool(self.config.get("jetstream", False))
        self.durable = self.config.get("durable", "ducted")

        self.credentials_file = self.config.get("credentials_file")
        self.nkey_seed_file = self.config.get("nkey_seed_file")
        self.tls_ca_file = self.config.get("tls_ca_file")
        self.tls_cert_file = self.config.get("tls_cert_file")
        self.tls_key_file = self.config.get("tls_key_file")

        self.transformers = {
            "senml-json": senml_to_event,
            "senml-cbor": senml_cbor_to_event,
            "json": json_to_event,
        }

        self.subscriptions = []
        self.nc = None

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

    async def get(self):
        "Uses async queue"

    async def startTimer(self):
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

        self.nc: NATS = await nats.connect(**connect_kwargs)

        if self.use_jetstream:
            js = self.nc.jetstream()
            for topic in self.topics:
                log.info("Subscribing to JetStream topic '%s' (durable=%s)", topic, self.durable)
                sub = await js.subscribe(topic, durable=self.durable, cb=self._get_event)
                self.subscriptions.append(sub)
            log.info("Connected to NATS (JetStream mode)")
        else:
            for topic in self.topics:
                log.info("Subscribing to topic '%s'", topic)
                sub = await self.nc.subscribe(topic, cb=self._get_event)
                self.subscriptions.append(sub)
            log.info("Connected to NATS")

    async def stopTimer(self):
        if self.nc:
            log.info("Disconnecting NATS")
            await self.nc.drain()
            await self.nc.close()

    async def _get_event(self, message):
        try:
            ev = self.transformers[self.format](message.data)
            self.queueBack([ev])
        except Exception:
            log.exception(
                "Failed to decode NATS message on subject '%s'", message.subject
            )
