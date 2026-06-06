import datetime
import os
import pytest

from duct.logs import follower, parsers


class TestLogs:
    def test_logfollow(self, tmp_path):
        try:
            os.unlink(tmp_path/'test.log.lf')
            os.unlink(tmp_path/'test.log')
        except Exception:
            pass

        with open(tmp_path/'test.log', 'wt') as log:
            log.write('foo\nbar\n')
            log.flush()

            f = follower.LogFollower(str(tmp_path/'test.log'), tmp_path=tmp_path, history=True)

            r = f.get()

            log.write('test')
            log.flush()

            r2 = f.get()

            log.write('ing\n')
            log.flush()

            r3 = f.get()

            assert r[0] == 'foo'
            assert r[1] == 'bar'

            assert r2 == []
            assert r3[0] == 'testing'

        

        # Move inode
        os.rename(tmp_path/'test.log', tmp_path/'testold.log')

        with open(tmp_path/'test.log', 'wt') as log:
            log.write('foo2\nbar2\n')
            log.close()

            r = f.get()

            assert r[0] == 'foo2'
            assert r[1] == 'bar2'

            # Go backwards
            log = open(tmp_path/'test.log', 'wt')
            log.write('foo3\n')
            log.close()

            r = f.get()

            assert r[0] == 'foo3'

        os.unlink(tmp_path/'test.log')
        os.unlink(tmp_path/'testold.log')

    def test_apache_parser(self):
        log = parsers.ApacheLogParser('combined')

        line = '192.168.0.102 - - [16/Jan/2015:11:11:45 +0200] "GET / HTTP/1.1" 200 709 "-" "My browser"'

        want = {
            'status': 200,
            'request': 'GET / HTTP/1.1',
            'bytes': 709,
            'user-agent': 'My browser',
            'client': '192.168.0.102',
            'time': datetime.datetime(2015, 1, 16, 11, 11, 45),
            'referer': None,
            'logname': None,
            'user': None,
        }

        p = log.parse(line)

        for k, v in want.items():
            assert p[k] == v
