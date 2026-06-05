"""
Helper classes for tests
"""
from duct.configuration import ConfigFile
# pylint: disable=import-outside-toplevel,super-init-not-called,unused-argument,missing-function-docstring,attribute-defined-outside-init

class TestConfig(ConfigFile):
    """Test config which accepts a plain dict"""
    def __init__(self, config):
        self.raw_config = config
        self._parse_config()


def _nats_subject_matches(subject: str, pattern: str) -> bool:
    """Basic NATS subject matching supporting '>' and '*' wildcards."""
    if pattern == ">":
        return True
    if pattern.endswith(".>"):
        prefix = pattern[:-2]
        return subject == prefix or subject.startswith(prefix + ".")
    if "*" not in pattern:
        return subject == pattern
    # Token-by-token match for '*'
    pat_parts = pattern.split(".")
    sub_parts = subject.split(".")
    if len(pat_parts) != len(sub_parts):
        return False
    return all(p == "*" or p == s for p, s in zip(pat_parts, sub_parts))


class FakeJetStream:
    """Fake JetStream context that shares the parent client's message store."""
    def __init__(self, nc):
        self.nc = nc

    async def publish(self, topic, payload):
        "Publish a message to the fake queue"
        self.nc.messages.append((topic, payload))
        from nats.aio.msg import Msg
        for sub, cb in list(self.nc.subs.items()):
            if _nats_subject_matches(topic, sub):
                await cb(Msg(self.nc, subject=topic, data=payload))

    async def subscribe(self, topic, durable=None, cb=None):
        "Subscribe to a fake topic"
        self.nc.subs[topic] = cb
        return object()


class FakeNATS:
    """
    Dual-purpose fake NATS client. Returns itself on connect.
    Triggers subscribed callbacks on publish so source/output tests can be wired together.
    """
    def __init__(self, *a, **kw):
        self.messages = []
        self.subs = {}

    async def publish(self, topic, message):
        "Publiush a message to the fake queue"
        self.messages.append((topic, message))
        from nats.aio.msg import Msg
        for sub, cb in list(self.subs.items()):
            if _nats_subject_matches(topic, sub):
                await cb(Msg(self, subject=topic, data=message))

    async def subscribe(self, topic, cb=None):
        "Subscribe to a fake topic"
        self.subs[topic] = cb
        return object()

    def jetstream(self):
        "Fake jetstream endpoint"
        if not hasattr(self, "js"):
            self.js = FakeJetStream(self)
        return self.js

    async def drain(self):
        pass

    async def close(self):
        pass

    async def connect(self, servers=[], **kw):
        return self
