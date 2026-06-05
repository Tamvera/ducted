"""
Helper classes for tests
"""
from duct.configuration import ConfigFile, DuctConfig

class TestConfig(ConfigFile):
    """Test config which accepts a plain dict"""
    def __init__(self, config):
        self.raw_config = config
        self._parse_config()

class FakeNATS:
    """
    Dual purpose fake NATS client class, returns itself on connect 
    """
    def __init__(self, *a, **kw):
        self.messages = []
        self.subs = {}

    async def publish(self, topic, message):
        self.messages.append((topic, message))
        from nats.aio.msg import Msg

        for sub, qb in self.subs.items():
            if topic.startswith(sub) or sub == '>':
                # Not a particularly durable aproximation of matching...
                await qb(Msg(self, subject=topic, data=message))

    async def subscribe(self, topic, cb=None):
        self.subs[topic] = cb

    async def connect(self, servers=[], **kw):
        return self