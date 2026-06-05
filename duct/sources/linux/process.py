"""
.. module:: processes
   :platform: unix
   :synopsis: Provides checks for running system processes

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import re

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source


@implementer(IDuctSource)
class ProcessCount(Source):
    """Returns the ps count on the system

    **Metrics:**

    :(service name): Number of processes
    """

    ssh = True

    async def get(self):
        out, _err, _code = await self.fork('/bin/ps', args=('-e',))

        count = len(out.strip('\n').split('\n')) - 1

        return self.createEvent('ok', f'Process count {count}', count)

@implementer(IDuctSource)
class ProcessStats(Source):
    """Returns memory used by each active parent process

    **Metrics:**

    :(service name).proc.(process name).cpu: Per process CPU usage
    :(service name).proc.(process name).memory: Per process memory use
    :(service name).proc.(process name).age: Per process age
    :(service name).user.(user name).cpu: Per user CPU usage
    :(service name).user.(user name).memory: Per user memory use
    """

    ssh = True

    async def get(self):
        out, _err, _code = await self.fork(
            '/bin/ps',
            args=('-eo', 'pid,user:50,etime,rss,pcpu,comm:50,cmd:255',)
        )

        lines = out.strip('\n').split('\n')

        cols = lines[0].split()

        procs = {}
        users = {}

        for l in lines[1:]:
            parts = l.split(None, len(cols) - 1)

            proc = {}
            for i, e in enumerate(parts):
                proc[cols[i]] = e.strip()

            parts = None

            elapsed = proc['ELAPSED']
            if '-' in elapsed:
                days = int(elapsed.split('-')[0])
                hours, minutes, seconds = [
                    int(i) for i in elapsed.split('-')[1].split(':')
                ]
                age = (days*24*60*60) + (hours*60*60) + (minutes*60)
                age += seconds

            elif elapsed.count(':') == 2:
                hours, minutes, seconds = [
                    int(i) for i in elapsed.split(':')
                ]
                age = (hours*60*60) + (minutes*60) + seconds

            else:
                minutes, seconds = [
                    int(i) for i in elapsed.split(':')
                ]
                age = (minutes*60) + seconds

            # Ignore kernel and tasks that just started, usually it's this ps
            if (proc['CMD'][0] != '[') and (age > 0):
                binary = re.sub(r'[^\w_]', '', proc['CMD'].split()[0])
                comm = re.sub(r'[^\w_]', '', proc['COMMAND'])
                user = re.sub(r'[^\w_]', '', proc['USER'].lower())

                mem = int(proc['RSS'])/1024.0
                cpu = float(proc['%CPU'])

                if user in users:
                    users[user]['cpu'] += cpu
                    users[user]['mem'] += mem
                else:
                    users[user] = {
                        'cpu': cpu, 'mem': mem
                    }

                if binary != comm:
                    key = f"{binary}.{comm}"
                else:
                    key = comm

                key = key.strip('_')

                if key in procs:
                    procs[key]['cpu'] += cpu
                    procs[key]['mem'] += mem
                    procs[key]['age'] += age
                else:
                    procs[key] = {
                        'cpu': cpu, 'mem': mem, 'age': age
                    }

        events = []

        for k, v in users.items():
            events.append(self.createEvent(
                'ok', f"User memory {k}: {v['mem']:0.2f}MB",
                v['mem'], prefix=f'user.{k}.mem'))
            events.append(self.createEvent(
                'ok', f"User CPU usage {k}: {int(v['cpu']*100)}%",
                v['cpu'], prefix=f'user.{k}.cpu'))

        for k, v in procs.items():
            events.append(self.createEvent(
                'ok', f"Process age {k}: {v['age']}s",
                v['age'], prefix=f'proc.{k}.age'))
            events.append(self.createEvent(
                'ok',
                f"Process memory {k}: {v['mem']:0.2f}MB", v['mem'],
                prefix=f'proc.{k}.mem'
            ))
            events.append(
                self.createEvent(
                    'ok',
                    f"Process CPU usage {k}: {int(v['cpu']*100)}%",
                    v['cpu'],
                    prefix=f'proc.{k}.cpu'
                )
            )

        return events
