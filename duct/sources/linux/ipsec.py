"""
.. module:: ipsec
   :platform: unix
   :synopsis: Some monitoring stuff for IPSEC tunnels

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source


@implementer(IDuctSource)
class StrongSwan(Source):
    """Returns the status of strongSwan IPSec tunnels

    **Metrics:**

    :(service name).(peer name): Tunnel status
    """
    ssh = True

    async def get(self):
        out, _err, _code = await self.fork('/usr/bin/sudo', args=(
            'ipsec', 'statusall'))

        connections = {}

        s = 0

        for l in out.strip('\n').split('\n'):
            if l == "Connections:":
                s = 1
                continue
            elif l == "Routed Connections:":
                s = 2
            elif "Security Associations" in l:
                s = 3
            elif l[0] == ' ' and ':' in l:
                if s == 1:
                    con, detail = l.strip().split(': ', 1)
                    detail = detail.strip()

                    if con not in connections:
                        connections[con] = {
                            'source': detail.split('...')[0],
                            'destination': detail.split('...')[1].split()[0],
                            'up': False
                        }
                elif s == 3:
                    con, detail = l.strip().split(': ', 1)
                    detail = detail.strip()
                    if '[' in con:
                        con = con.split('[')[0]
                    else:
                        con = con.split('{')[0]

                    if 'ESTABLISHED' in detail:
                        connections[con]['up'] = True

        events = []
        for k, v in connections.items():
            if v['up']:
                events.append(self.createEvent('ok', f'IPSec tunnel {k} up',
                                               1, prefix=k))
            else:
                events.append(self.createEvent('critical',
                                               f'IPSec tunnel {k} down',
                                               0, prefix=k))

        return events
