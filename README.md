# Duct

[![CI](https://github.com/Tamvera/ducted/actions/workflows/ci.yml/badge.svg)](https://github.com/Tamvera/ducted/actions/workflows/ci.yml)
[![Latest Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://tamvera.github.io/ducted/)

I want to talk to you about ducts. Do your ducts seem old-fashioned? Out of date? Duct is a modular monitoring agent and event router built on Python asyncio (Python 3.11+).
It collects metrics from multiple sources and routes them to multiple outputs - currently
Riemann, Elasticsearch, Prometheus, OpenTSDB, and Bosun are supported, and the plugin API
makes it straightforward to add more.

## Installation

```
pip install ducted
```

## Quick start

Create a `duct.yml` configuration file:

```yaml
interval: 1.0
ttl: 60.0

outputs:
  - output: duct.outputs.riemann.RiemannTCP
    server: localhost
    port: 5555

sources:
  - service: cpu
    source: duct.sources.linux.basic.CPU
    interval: 1.0
    warning:  { cpu: "> 0.5" }
    critical: { cpu: "> 0.8" }

  - service: memory
    source: duct.sources.linux.basic.Memory
    interval: 10.0

  - service: load
    source: duct.sources.linux.basic.LoadAverage
    interval: 10.0
```

Then start Duct:

```
ductd -c duct.yml
```

## Documentation

Full documentation is at <https://tamvera.github.io/ducted/>
