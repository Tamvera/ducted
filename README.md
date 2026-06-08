# Duct

I want to talk to you about ducts. Do your ducts seem old-fashioned? Out of date?

[![CI](https://github.com/Tamvera/ducted/actions/workflows/ci.yml/badge.svg)](https://github.com/Tamvera/ducted/actions/workflows/ci.yml)
[![Latest Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://tamvera.github.io/ducted/)

**A pluggable Python telemetry pipeline.** Collect metrics from anywhere, route them to any backend, with nothing but Python and a YAML file.

Duct is an asyncio daemon that polls *sources* on configurable intervals and routes the resulting events to *outputs*. Both are plain Python classes: anything Python can reach - a socket, a file, an HTTP API, a subprocess, an SSH session, a hardware sensor - can be a source or an output. No DSL, no code generation.

## What makes it different

- **Sources and outputs can run network servers.** The Prometheus output hosts an HTTP scrape endpoint inside the daemon; the sFlow source runs a UDP collector; the Riemann source acts as a full Riemann TCP server. Duct can receive telemetry from other systems, not only emit it.
- **SSH remote checks, no remote agent.** Mark any source `use_ssh: true` and it runs transparently on a remote host over a pooled SSH connection.
- **Modern, backend-agnostic.** InfluxDB 3, Prometheus, NATS (JetStream + SenML/CBOR), Elasticsearch, Graphite, and more. Use one output or several simultaneously.
- **Fine-grained routing.** Route individual sources to specific outputs or sets of outputs.
- **Threshold state evaluation.** Declare `warning`/`critical` expressions per-metric; Duct overrides event state automatically.
- **Blueprint macros.** DRY config: define a toolbox of checks once, expand it across a list of hosts with a single block.

## Built-in sources

| Category | Sources |
|---|---|
| Linux system | CPU, memory, load average, disk I/O, disk free, network, process stats |
| Sensors & hardware | lm-sensors, SMART, DS18B20 temperature, MPL115 pressure/temperature, UPS (NUT) |
| Web & proxy | Apache, Nginx (status + log metrics), HAProxy |
| Databases | PostgreSQL, Elasticsearch, Redis, RabbitMQ, Memcache, Riak |
| Network | Ping, HTTP, sFlow UDP collector, SNMP |
| Infrastructure | Docker containers, Postfix, Unbound, StrongSwan/IPsec |
| Messaging | NATS subscriber (JetStream + SenML) |
| Ingestion | Riemann TCP listener, Munin node client, uWSGI Emperor |

## Installation

```bash
pip install ducted
```

Requires Python 3.11+.

## Quick start

Create `duct.yml`:

```yaml
interval: 1.0
ttl: 60.0

outputs:
  # Prometheus scrape endpoint at http://0.0.0.0:9100/metrics
  - output: duct.outputs.prometheus.Prometheus
    port: 9100

sources:
  - service: cpu
    source: duct.sources.linux.basic.CPU
    interval: 1.0
    warning:  { cpu: "> 0.5" }
    critical: { cpu: "> 0.8" }

  - service: memory
    source: duct.sources.linux.basic.Memory
    interval: 10.0

  - service: disk
    source: duct.sources.linux.basic.DiskFree
    interval: 60.0
```

```bash
ductd -c duct.yml
```

## Outputs

Fan out to multiple backends simultaneously by naming them:

```yaml
outputs:
  - output: duct.outputs.prometheus.Prometheus
    name: prom
    port: 9100

  - output: duct.outputs.influxdb3.InfluxDB3
    name: influx
    url: http://localhost:8086
    database: mymetrics
    token: my-token

  - output: duct.outputs.nats.Nats
    name: bus
    servers: ["nats://localhost:4222"]
    format: senml-cbor
    jetstream: true
```

## Receiving telemetry

Sources can listen as well as poll. Funnel events from other systems into the same pipeline and route them to any output:

```yaml
sources:
  # Act as a Riemann TCP server - ingest events from other agents
  - service: riemann-in
    source: duct.sources.riemann.RiemannTCP
    port: 5555
    route: influx

  # Collect sFlow from network switches (UDP server)
  - service: sflow
    source: duct.sources.sflow.sFlow
    port: 6343

  # Subscribe to a NATS subject
  - service: nats-in
    source: duct.sources.nats.Nats
    servers: ["nats://localhost:4222"]
    topics: ["metrics.>"]
```

## SSH remote checks

Run checks on remote hosts without installing an agent. SSH connections are pooled per host:

```yaml
ssh_username: monitor
ssh_keyfile: /etc/duct/id_ed25519

sources:
  - service: load
    source: duct.sources.linux.basic.LoadAverage
    use_ssh: true
    ssh_host: web01.example.com
    interval: 30.0
```

Use **blueprint macros** to expand a toolbox of checks across many hosts with a single config block - see the [documentation](https://tamvera.github.io/ducted/) for details.

## Writing a plugin

A source is a Python class. It can use any library, open any connection, or start any server:

```python
from zope.interface import implementer
from duct.interfaces import IDuctSource
from duct.objects import Source

@implementer(IDuctSource)
class MySource(Source):
    async def get(self):
        value = await fetch_something()
        return self.createEvent('ok', 'My metric', value, prefix='mymetric')
```

Point to it in config by dotted import path:

```yaml
sources:
  - service: mything
    source: mypackage.mysource.MySource
    interval: 10.0
```

Outputs follow the same pattern: subclass `Output`, implement `createClient`, and drain the event queue however you like - including running an embedded server.

## Documentation

Full documentation at <https://tamvera.github.io/ducted/>
