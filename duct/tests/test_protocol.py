"""Tests for duct.protocol.senml"""
import json
import pytest

from duct.objects import Event
from duct.protocol.senml import (
    event_to_senml_pack,
    senml_pack_to_event,
    event_to_senml,
    senml_to_event,
    event_to_senml_cbor,
    senml_cbor_to_event,
    event_to_json,
    json_to_event,
)


def _make_event(**overrides):
    defaults = dict(
        state="ok",
        service="cpu.load",
        description="CPU load average",
        metric=0.75,
        ttl=60.0,
        tags=["prod", "linux"],
        hostname="web01.example.com",
        attributes={"core": "0", "socket": 1},
        evtime=1_700_000_000.0,
    )
    defaults.update(overrides)
    return Event(**defaults)


# ---------------------------------------------------------------------------
# SenML pack structure
# ---------------------------------------------------------------------------

class TestSenMLPack:
    def test_pack_is_list(self):
        ev = _make_event()
        pack = event_to_senml_pack(ev)
        assert isinstance(pack, list)
        assert len(pack) >= 1

    def test_pack_base_fields(self):
        ev = _make_event(hostname="srv1", service="disk", metric=42.0, evtime=1_000.0)
        pack = event_to_senml_pack(ev)
        first = pack[0]
        assert first["bn"] == "srv1/"
        assert first["bt"] == 1_000.0
        assert first["n"] == "disk"
        assert first["v"] == 42.0

    def test_pack_secondary_records(self):
        ev = _make_event(state="critical", description="high load",
                         tags=["prod"], hostname="h", service="cpu")
        pack = event_to_senml_pack(ev)
        names = {r["n"] for r in pack}
        assert "cpu/state" in names
        assert "cpu/description" in names
        assert "cpu/ttl" in names
        assert "cpu/tags" in names

    def test_pack_state_value(self):
        ev = _make_event(state="warning", service="mem", hostname="h")
        pack = event_to_senml_pack(ev)
        state_rec = next(r for r in pack if r.get("n") == "mem/state")
        assert state_rec["vs"] == "warning"

    def test_pack_tags_joined(self):
        ev = _make_event(tags=["a", "b", "c"], service="s", hostname="h")
        pack = event_to_senml_pack(ev)
        tags_rec = next(r for r in pack if r.get("n") == "s/tags")
        assert tags_rec["vs"] == "a,b,c"

    def test_pack_numeric_attribute(self):
        ev = _make_event(attributes={"count": 5}, service="s", hostname="h")
        pack = event_to_senml_pack(ev)
        attr_rec = next(r for r in pack if r.get("n") == "s/attr/count")
        assert attr_rec.get("v") == 5.0
        assert "vs" not in attr_rec

    def test_pack_string_attribute(self):
        ev = _make_event(attributes={"zone": "eu-west"}, service="s", hostname="h")
        pack = event_to_senml_pack(ev)
        attr_rec = next(r for r in pack if r.get("n") == "s/attr/zone")
        assert attr_rec.get("vs") == "eu-west"
        assert "v" not in attr_rec

    def test_pack_no_tags_when_empty(self):
        ev = _make_event(tags=[], service="s", hostname="h")
        pack = event_to_senml_pack(ev)
        names = [r.get("n") for r in pack]
        assert "s/tags" not in names

    def test_pack_no_attributes_when_none(self):
        ev = _make_event(attributes=None, service="s", hostname="h")
        pack = event_to_senml_pack(ev)
        names = [r.get("n") for r in pack]
        assert not any("attr" in (n or "") for n in names)


# ---------------------------------------------------------------------------
# senml_pack_to_event round-trips
# ---------------------------------------------------------------------------

class TestSenMLRoundTrip:
    def _roundtrip(self, ev):
        return senml_pack_to_event(event_to_senml_pack(ev))

    def test_hostname_preserved(self):
        ev = _make_event(hostname="myhost.local")
        assert self._roundtrip(ev).hostname == "myhost.local"

    def test_service_preserved(self):
        ev = _make_event(service="disk.read")
        assert self._roundtrip(ev).service == "disk.read"

    def test_metric_preserved(self):
        ev = _make_event(metric=3.14)
        assert self._roundtrip(ev).metric == pytest.approx(3.14)

    def test_state_preserved(self):
        ev = _make_event(state="critical")
        assert self._roundtrip(ev).state == "critical"

    def test_description_preserved(self):
        ev = _make_event(description="something happened")
        assert self._roundtrip(ev).description == "something happened"

    def test_ttl_preserved(self):
        ev = _make_event(ttl=120.0)
        assert self._roundtrip(ev).ttl == pytest.approx(120.0)

    def test_tags_preserved(self):
        ev = _make_event(tags=["x", "y"])
        assert self._roundtrip(ev).tags == ["x", "y"]

    def test_attributes_preserved(self):
        ev = _make_event(attributes={"zone": "us-east", "num": 7})
        rt = self._roundtrip(ev)
        assert rt.attributes["zone"] == "us-east"
        assert float(rt.attributes["num"]) == 7.0

    def test_timestamp_preserved(self):
        ev = _make_event(evtime=1_700_000_000.0)
        assert self._roundtrip(ev).time == pytest.approx(1_700_000_000.0)

    def test_empty_tags_roundtrip(self):
        ev = _make_event(tags=[])
        assert self._roundtrip(ev).tags == []

    def test_no_attributes_roundtrip(self):
        ev = _make_event(attributes=None)
        assert self._roundtrip(ev).attributes is None


# ---------------------------------------------------------------------------
# Serialisation formats
# ---------------------------------------------------------------------------

class TestSenMLJSON:
    def test_returns_bytes(self):
        ev = _make_event()
        assert isinstance(event_to_senml(ev), bytes)

    def test_is_valid_json_array(self):
        ev = _make_event()
        data = json.loads(event_to_senml(ev))
        assert isinstance(data, list)

    def test_roundtrip_from_bytes(self):
        ev = _make_event(service="net.rx", metric=1234.0)
        assert senml_to_event(event_to_senml(ev)).service == "net.rx"

    def test_roundtrip_from_str(self):
        ev = _make_event(service="net.tx", metric=99.0)
        raw = event_to_senml(ev).decode("utf-8")
        assert senml_to_event(raw).service == "net.tx"


class TestSenMLCBOR:
    def test_returns_bytes(self):
        ev = _make_event()
        assert isinstance(event_to_senml_cbor(ev), bytes)

    def test_cbor_smaller_than_json(self):
        ev = _make_event()
        assert len(event_to_senml_cbor(ev)) < len(event_to_senml(ev))

    def test_roundtrip_service(self):
        ev = _make_event(service="temp.sensor")
        assert senml_cbor_to_event(event_to_senml_cbor(ev)).service == "temp.sensor"

    def test_roundtrip_full(self):
        ev = _make_event()
        rt = senml_cbor_to_event(event_to_senml_cbor(ev))
        assert rt.hostname == ev.hostname
        assert rt.metric == pytest.approx(ev.metric)
        assert rt.state == ev.state
        assert rt.tags == ev.tags


class TestPlainJSON:
    def test_returns_bytes(self):
        ev = _make_event()
        assert isinstance(event_to_json(ev), bytes)

    def test_roundtrip_basic(self):
        ev = _make_event(service="load.1", metric=0.5)
        rt = json_to_event(event_to_json(ev))
        assert rt.service == "load.1"
        assert rt.metric == pytest.approx(0.5)
        assert rt.hostname == ev.hostname

    def test_roundtrip_state_and_description(self):
        ev = _make_event(state="warning", description="high load")
        rt = json_to_event(event_to_json(ev))
        assert rt.state == "warning"
        assert rt.description == "high load"

    def test_roundtrip_tags(self):
        ev = _make_event(tags=["a", "b"])
        assert json_to_event(event_to_json(ev)).tags == ["a", "b"]

    def test_roundtrip_attributes(self):
        ev = _make_event(attributes={"k": "v"})
        assert json_to_event(event_to_json(ev)).attributes == {"k": "v"}

    def test_roundtrip_from_str(self):
        ev = _make_event(service="foo")
        raw = event_to_json(ev).decode("utf-8")
        assert json_to_event(raw).service == "foo"
