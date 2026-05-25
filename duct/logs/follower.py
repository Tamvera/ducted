"""
.. module:: follower
   :synopsis: Log tailing classes

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""
import os


class LogFollower(object):
    """Provides a class for following log files between runs

    :param logfile: Full path to logfile
    :type logfile: str
    :param parser: Optional parser method for log lines
    :type parser: str
    """

    def __init__(self, logfile, parser=None, tmp_path="/var/lib/duct/",
                 history=False):
        self.logfile = logfile
        self.tmp = os.path.join(
            tmp_path,
            f"{self.logfile.lstrip('/').replace('/', '-')}.lf"
        )

        self.history = history

        self.lastInode = None
        self.lastSize = -1

        self.readLast()

        self.parser = parser

    def cleanStore(self):
        """Clear up the log position store
        """
        os.unlink(self.tmp)

    def storeLast(self):
        """Persist the current position in the file
        """
        with open(self.tmp, 'wt', encoding='utf-8') as fi:
            fi.write(f'{self.lastSize}:{self.lastInode}')

    def readLast(self):
        """Read the latest changes in the file
        """
        if os.path.exists(self.tmp):
            with open(self.tmp, 'rt', encoding='utf-8') as fi:
                ls, li = fi.read().split(':')
            self.lastSize = int(ls)
            self.lastInode = int(li)
        else:
            if self.history:
                self.lastSize = 0
                self.lastInode = 0
            else:
                # Don't re-read the entire file
                stat = os.stat(self.logfile)
                self.lastSize = stat.st_size
                self.lastInode = stat.st_ino

    def get_fn(self, fn, max_lines=None):
        """Passes each parsed log line to `fn`
        This is a better idea than storing a giant log file in memory
        """
        stat = os.stat(self.logfile)

        if (stat.st_ino == self.lastInode) and (stat.st_size == self.lastSize):
            # Nothing new
            return []

        # Handle rollover and rotations vaguely
        if (stat.st_ino != self.lastInode) or (stat.st_size < self.lastSize):
            self.lastSize = 0

        fi = open(self.logfile, 'rt', encoding='utf-8')  # pylint: disable=consider-using-with
        fi.seek(self.lastSize)

        self.lastInode = stat.st_ino

        lines = 0

        for i in fi:
            lines += 1
            if max_lines and (lines > max_lines):
                self.storeLast()
                fi.close()
                return

            if '\n' in i:
                self.lastSize += len(i)
                if self.parser:
                    line = self.parser(i.strip('\n'))
                else:
                    line = i.strip('\n')

                fn(line)

        self.storeLast()

        fi.close()

    def get(self, max_lines=None):
        """Returns a big list of all log lines since the last run
        """
        rows = []

        self.get_fn(rows.append, max_lines=max_lines)

        return rows
