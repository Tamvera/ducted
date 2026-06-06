import pytest

from duct import utils


class TestUtils:
    def test_persistent_cache(self, tmp_path):
        loc = str(tmp_path/'cache')
        pc = utils.PersistentCache(location=loc)

        pc.set('foo', 'bar')
        pc.set('bar', 'baz')

        pc2 = utils.PersistentCache(location=loc)

        assert pc2.get('foo')[1] == 'bar'

        pc.set('foo', 'baz')

        assert pc2.get('foo')[1] == 'baz'

        pc.delete('foo')

        assert not pc.contains('foo')
        assert pc.contains('bar')

        pc.expire(0)

        assert not pc.contains('bar')
