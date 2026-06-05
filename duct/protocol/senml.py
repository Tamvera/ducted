"""
.. module:: senml
   :synopsis: SenML Protocol support (RFC 8428)

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import json

import cbor2

from duct.objects import Event


def event_to_senml_pack(event: Event) -> list:
    """Convert a Duct Event to a SenML record pack (list of dicts)."""
    pack = [
        {
            "bn": event.hostname + "/",
            "bt": event.time,
            "n": event.service,
            "v": event.metric,
        }
    ]
    if event.state:
        pack.append({"n": event.service + "/state", "vs": event.state})
    if event.description:
        pack.append({"n": event.service + "/description", "vs": event.description})
    if event.ttl:
        pack.append({"n": event.service + "/ttl", "v": float(event.ttl)})
    if event.tags:
        pack.append({"n": event.service + "/tags", "vs": ",".join(event.tags)})
    if event.attributes:
        for k, v in event.attributes.items():
            if isinstance(v, (int, float)):
                pack.append({"n": f"{event.service}/attr/{k}", "v": float(v)})
            else:
                pack.append({"n": f"{event.service}/attr/{k}", "vs": str(v)})
    return pack


def senml_pack_to_event(pack: list, default_ttl: float = 60.0) -> Event:
    """Convert a SenML record pack (list of dicts) to a Duct Event."""
    bn = pack[0].get("bn", "")
    bt = pack[0].get("bt", None)
    hostname = bn.rstrip("/") if bn else ""

    service = ""
    metric = 0.0
    state = "ok"
    description = ""
    ttl = default_ttl
    tags = []
    attributes = {}

    for record in pack:
        n = record.get("n", "")
        # Relative name after stripping base name prefix
        relative = (bn + n)[len(bn):]
        parts = relative.split("/", 1)
        svc = parts[0]
        sub = parts[1] if len(parts) > 1 else None

        if sub is None:
            service = svc
            metric = float(record.get("v", record.get("vs", 0.0)))
        elif sub == "state":
            state = record.get("vs", "ok")
        elif sub == "description":
            description = record.get("vs", "")
        elif sub == "ttl":
            ttl = float(record.get("v", default_ttl))
        elif sub == "tags":
            tags = [t.strip() for t in record.get("vs", "").split(",") if t.strip()]
        elif sub.startswith("attr/"):
            attr_key = sub[5:]
            attributes[attr_key] = record.get("v", record.get("vs", ""))

    return Event(
        state, service, description, metric, ttl,
        tags=tags,
        hostname=hostname or None,
        evtime=bt,
        attributes=attributes if attributes else None,
    )


def event_to_senml(event: Event) -> bytes:
    """Serialise a Duct Event to SenML-JSON bytes."""
    return json.dumps(event_to_senml_pack(event)).encode("utf-8")


def senml_to_event(data: bytes | str, default_ttl: float = 60.0) -> Event:
    """Deserialise SenML-JSON bytes or string to a Duct Event."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return senml_pack_to_event(json.loads(data), default_ttl)


def event_to_senml_cbor(event: Event) -> bytes:
    """Serialise a Duct Event to SenML-CBOR bytes."""
    return cbor2.dumps(event_to_senml_pack(event))


def senml_cbor_to_event(data: bytes, default_ttl: float = 60.0) -> Event:
    """Deserialise SenML-CBOR bytes to a Duct Event."""
    return senml_pack_to_event(cbor2.loads(data), default_ttl)


def event_to_json(event: Event) -> bytes:
    """Serialise a Duct Event to plain JSON bytes using the native event dict."""
    return json.dumps(dict(event)).encode("utf-8")


def json_to_event(data: bytes | str, default_ttl: float = 60.0) -> Event:
    """Deserialise a plain JSON event dict to a Duct Event."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    d = json.loads(data)
    return Event(
        d.get("state", "ok"),
        d.get("service", ""),
        d.get("description", ""),
        d.get("metric", 0.0),
        d.get("ttl", default_ttl),
        tags=d.get("tags", []),
        hostname=d.get("hostname") or None,
        evtime=d.get("time"),
        attributes=d.get("attributes"),
        evtype=d.get("type", "metric"),
    )
