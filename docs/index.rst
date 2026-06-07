.. Duct documentation master file

Duct - a pluggable Python telemetry pipeline
============================================

Duct is an asyncio daemon that polls *sources* on configurable intervals and
routes the resulting events to *outputs*. Both are plain Python classes:
anything Python can reach - a socket, a file, an HTTP API, a subprocess, an
SSH session, a hardware sensor - can be a source or an output.

**What makes it different:**

- **Sources and outputs can run network servers.** The Prometheus output
  hosts an HTTP scrape endpoint; the sFlow source is a UDP collector; the
  Riemann source acts as a full Riemann TCP server. Duct can *receive*
  telemetry from other systems, not only emit it.
- **SSH remote checks, no remote agent.** Mark any source ``use_ssh: true``
  and it runs on a remote host over a pooled SSH connection.
- **Modern, backend-agnostic.** InfluxDB 3, Prometheus, NATS
  (JetStream + SenML/CBOR), Elasticsearch, Graphite, and more.
- **Fine-grained routing.** Route individual sources to specific outputs or
  sets of outputs.
- **Blueprint macros.** DRY config: define a toolbox of checks once and
  expand it across a list of hosts with a single block.

Contents
--------

.. toctree::
    :maxdepth: 3

    start
    sources
    outputs
    blueprints
    examples


API Documentation
-----------------

.. toctree::
    :maxdepth: 2

    api/protocol.rst
    api/logs.rst
    api/sources.rst
    api/outputs.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
