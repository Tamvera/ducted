"""
.. module:: basic
   :platform: unix
   :synopsis: Basic linux host checks

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source
from duct.aggregators import Counter64


@implementer(IDuctSource)
class LoadAverage(Source):
    """Reports system load average for the current host

    **Metrics:**

    :(service name): Load average
    """

    def _parse_loadaverage(self, data):
        la1 = data.split()[0]

        return self.createEvent('ok', 'Load average', float(la1))

    async def get(self):
        with open('/proc/loadavg', 'rt', encoding='utf-8') as f:
            return self._parse_loadaverage(f.read())

    async def sshGet(self):
        loadavg, err, code = await self.fork('/bin/cat /proc/loadavg')

        if code == 0:
            return self._parse_loadaverage(loadavg)
        else:
            raise RuntimeError(err)


@implementer(IDuctSource)
class DiskIO(Source):
    """Reports disk IO statistics per device

    :param devices: List of devices to check (optional)
    :type devices: list.

    **Metrics:**

    :(service name).(device name).reads: Number of completed reads
    :(service name).(device name).read_bytes: Bytes read per second
    :(service name).(device name).read_latency: Disk read latency
    :(service name).(device name).writes: Number of completed writes
    :(service name).(device name).write_bytes: Bytes written per second
    :(service name).(device name).write_latency: Disk write latency
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)

        self.devices = self.config.get('devices')

        self.tcache = {}
        self.trc = {}
        self.twc = {}

    def _parse_stats(self, stats):
        events = []
        for s in stats:
            parts = s.strip().split()
            n = parts[2]
            # Filter things we don't care about
            if (n[:4] != 'loop') and (n[:3] != 'ram'):
                dname = "/dev/" + n

                if self.devices and (dname not in self.devices):
                    continue

                nums = [int(i) for i in parts[3:]]

                reads, _readm, read_sec, read_t = nums[:4]
                writes, _writem, write_sec, write_t = nums[4:8]

                # Calculate the average latency of read/write ops
                if n in self.tcache:
                    (last_r, last_w, last_rt, last_wt) = self.tcache[n]

                    r_delta = float(reads - last_r)
                    w_delta = float(writes - last_w)

                    if r_delta > 0:
                        read_lat = (read_t - last_rt)/float(reads - last_r)
                        self.trc[n] = read_lat
                    else:
                        read_lat = self.trc.get(n, None)

                    if w_delta > 0:
                        write_lat = (write_t - last_wt)/float(writes - last_w)
                        self.twc[n] = write_lat
                    else:
                        write_lat = self.twc.get(n, None)

                else:
                    if reads > 0:
                        read_lat = read_t / float(reads)
                        self.trc[n] = read_lat
                    else:
                        read_lat = None

                    if writes > 0:
                        write_lat = write_t / float(writes)
                        self.twc[n] = write_lat
                    else:
                        write_lat = None

                self.tcache[n] = (reads, writes, read_t, write_t)

                if read_lat:
                    events.append(self.createEvent(
                        'ok',
                        'Read latency (ms)',
                        read_lat,
                        prefix=f'{dname}.read_latency'
                    ))

                if write_lat:
                    events.append(self.createEvent(
                        'ok',
                        'Write latency (ms)', write_lat,
                        prefix=f'{dname}.write_latency'
                    ))

                events.extend([
                    self.createEvent('ok', 'Reads', reads,
                                     prefix=f'{dname}.reads',
                                     aggregation=Counter64),
                    self.createEvent('ok', 'Read Bps', read_sec * 512,
                                     prefix=f'{dname}.read_bytes',
                                     aggregation=Counter64),
                    self.createEvent('ok', 'Writes', writes,
                                     prefix=f'{dname}.writes',
                                     aggregation=Counter64),
                    self.createEvent('ok', 'Write Bps', write_sec * 512,
                                     prefix=f'{dname}.write_bytes',
                                     aggregation=Counter64),
                ])

        return events

    async def sshGet(self):
        diskstats, err, code = await self.fork('/bin/cat /proc/diskstats')

        if code == 0:
            stats = diskstats.strip('\n').split('\n')
            return self._parse_stats(stats)
        else:
            raise RuntimeError(err)

    def _getstats(self):
        with open('/proc/diskstats', 'rt', encoding='utf-8') as f:
            stats = f.read()
        return stats.strip('\n').split('\n')

    async def get(self):
        stats = self._getstats()
        return self._parse_stats(stats)


@implementer(IDuctSource)
class CPU(Source):
    """Reports system CPU utilisation as a percentage/100

    **Metrics:**

    :(service name): Percentage CPU utilisation
    :(service name).(type): Percentage CPU utilisation by type
    :(service name).coreX: Percentage CPU utilisation per core
    :(service name).coreX.(type): Percentage CPU utilisation per core by type
    """

    cols = ['user', 'nice', 'system', 'idle', 'iowait', 'irq',
            'softirq', 'steal', 'guest', 'guest_nice']

    def __init__(self, *a):
        Source.__init__(self, *a)

        self.cpu = {}
        self.prev_total = {}
        self.prev_usage = {}

    def _read_proc_stat(self):
        cpus = []
        with open('/proc/stat', 'rt', encoding='utf-8') as procstat:
            for l in procstat:
                if l.startswith('cpu'):
                    cpus.append(l.strip('\n'))
        return cpus

    def _calculate_metrics(self, stat):
        cpuid = stat.split()[0]
        cpu = [int(i) for i in stat.split()[1:]]
        # We might not have all the virt-related numbers, so zero-pad.
        cpu = (cpu + [0, 0, 0])[:10]
        (user, nice, system, idle, iowait, irq,
         softirq, steal, _guest, _guestnice) = cpu

        usage = user + nice + system + irq + softirq + steal
        total = usage + iowait + idle

        if not self.cpu.get(cpuid):
            # No initial values, so set them and return no events.
            self.cpu[cpuid] = cpu
            self.prev_total[cpuid] = total
            self.prev_usage[cpuid] = usage
            return None

        total_diff = total - self.prev_total[cpuid]

        if total_diff != 0:
            metrics = [(None,
                        (usage - self.prev_usage[cpuid]) / float(total_diff))]

            for i, name in enumerate(self.cols):
                prev = self.cpu[cpuid][i]
                cpu_m = (cpu[i] - prev) / float(total_diff)

                metrics.append((name, cpu_m))

            self.cpu[cpuid] = cpu
            self.prev_total[cpuid] = total
            self.prev_usage[cpuid] = usage

            return metrics

        return None

    def _transpose_metrics(self, metrics, prefix):
        if metrics:
            events = [
                self.createEvent('ok',
                                 f'CPU {name} {int(cpu_m * 100)}%',
                                 cpu_m, prefix=prefix+name)
                for name, cpu_m in metrics[1:]
            ]

            events.append(self.createEvent(
                'ok', f'CPU {int(metrics[0][1] * 100)}%', metrics[0][1],
                prefix=prefix.rstrip('.')))

            return events
        return None

    async def sshGet(self):
        procstat, err, code = await self.fork('cat /proc/stat')
        if code == 0:
            metrics = []
            stat = procstat.strip('\n').split('\n')
            for cpu in stat:
                if not cpu.startswith('cpu'):
                    continue
                if cpu.split()[0] == 'cpu':
                    prefix = ""
                else:
                    prefix = 'core' + cpu.split()[0].strip('cpu') + '.'
                stats = self._calculate_metrics(cpu)
                if stats:
                    metrics.extend(self._transpose_metrics(stats, prefix))

            return metrics or None
        else:
            raise RuntimeError(err)

    async def get(self):
        stat = self._read_proc_stat()
        metrics = []
        for cpu in stat:
            if not cpu.startswith('cpu'):
                continue
            if cpu.split()[0] == 'cpu':
                prefix = ""
            else:
                prefix = 'core' + cpu.split()[0].strip('cpu') + '.'
            stats = self._calculate_metrics(cpu)
            if stats:
                metrics.extend(self._transpose_metrics(stats, prefix))

        return metrics or None


@implementer(IDuctSource)
class Memory(Source):
    """Reports system memory utilisation as a percentage/100

    **Metrics:**

    :(service name): Percentage memory utilisation
    """

    def _parse_stats(self, mem):
        dat = {}
        for l in mem:
            if ':' not in l:
                continue
            k, v = l.replace(':', '').split()[:2]
            dat[k] = int(v)

        if 'MemAvailable' in dat:
            free = dat['MemAvailable']
        else:
            free = dat['MemFree'] + dat['Buffers'] + dat['Cached']
        total = dat['MemTotal']
        used = total - free

        return self.createEvent('ok', f'Memory {used}/{total}',
                                used/float(total))

    async def get(self):
        with open('/proc/meminfo', 'rt', encoding='utf-8') as f:
            return self._parse_stats(f)

    async def sshGet(self):
        mem, err, code = await self.fork('/bin/cat /proc/meminfo')

        if code == 0:
            return self._parse_stats(mem.strip('\n').split('\n'))
        else:
            raise RuntimeError(err)


@implementer(IDuctSource)
class DiskFree(Source):
    """Returns the free space for all mounted filesystems

    :param disks: List of devices to check (optional)
    :type disks: list.

    **Metrics:**

    :(service name).(device).used: Used space (%)
    :(service name).(device).bytes: Used space (kbytes)
    :(service name).(device).free: Free space (kbytes)
    """

    ssh = True

    async def get(self):
        disks = self.config.get('disks')

        out, _, _ = await self.fork('/bin/df', args=('-lPx', 'tmpfs',))

        out = [i.split() for i in out.strip('\n').split('\n')[1:]]

        events = []

        for disk, _size, used, free, util, _mnt in out:
            if disks and (disk not in disks):
                continue

            if disk != "udev":
                util = int(util.strip('%'))
                used = int(used)
                free = int(free)
                events.extend([
                    self.createEvent('ok', f'Disk percent used {util}%',
                                     util, prefix=f'{disk}.used'),
                    self.createEvent('ok', f'Disk bytes used {used} kB',
                                     used, prefix=f'{disk}.bytes'),
                    self.createEvent('ok', f'Disk free {free} kB',
                                     free, prefix=f'{disk}.free')
                ])

        return events

@implementer(IDuctSource)
class Network(Source):
    """Returns all network interface statistics

    :param interfaces: List of interfaces to check (optional)
    :type interfaces: list.

    **Metrics:**

    :(service name).(device).tx_bytes: Bytes transmitted
    :(service name).(device).tx_packets: Packets transmitted
    :(service name).(device).tx_errors: Errors
    :(service name).(device).rx_bytes: Bytes received
    :(service name).(device).rx_packets: Packets received
    :(service name).(device).rx_errors: Errors
    """

    def _parse_stats(self, stats):
        ifaces = self.config.get('interfaces')
        ev = []

        for stat in stats:
            items = stat.split()
            iface = items[0].strip(':')

            if ifaces and (iface not in ifaces):
                continue

            tx_bytes = int(items[1])
            tx_packets = int(items[2])
            tx_err = int(items[3])
            rx_bytes = int(items[9])
            rx_packets = int(items[10])
            rx_err = int(items[11])

            ev.extend([
                self.createEvent('ok',
                                 f'Network {iface} TX bytes/sec',
                                 tx_bytes, prefix=f'{iface}.tx_bytes',
                                 aggregation=Counter64),
                self.createEvent('ok',
                                 f'Network {iface} TX packets/sec',
                                 tx_packets, prefix=f'{iface}.tx_packets',
                                 aggregation=Counter64),
                self.createEvent('ok',
                                 f'Network {iface} TX errors/sec',
                                 tx_err, prefix=f'{iface}.tx_errors',
                                 aggregation=Counter64),
                self.createEvent('ok',
                                 f'Network {iface} RX bytes/sec',
                                 rx_bytes, prefix=f'{iface}.rx_bytes',
                                 aggregation=Counter64),
                self.createEvent('ok',
                                 f'Network {iface} RX packets/sec',
                                 rx_packets, prefix=f'{iface}.rx_packets',
                                 aggregation=Counter64),
                self.createEvent('ok',
                                 f'Network {iface} RX errors/sec',
                                 rx_err, prefix=f'{iface}.rx_errors',
                                 aggregation=Counter64),
            ])

        return ev

    def _readStats(self):
        with open('/proc/net/dev', 'rt', encoding='utf-8') as f:
            proc_dev = f.read()
        return proc_dev.strip('\n').split('\n')[2:]

    async def sshGet(self):
        net, err, code = await self.fork('/bin/cat /proc/net/dev')

        if code == 0:
            return self._parse_stats(net.strip('\n').split('\n')[2:])
        else:
            raise RuntimeError(err)

    async def get(self):
        return self._parse_stats(self._readStats())
