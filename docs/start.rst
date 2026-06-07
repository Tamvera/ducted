Getting started
***************

Installation
============

Requires Python 3.11 or newer::

    $ pip install ducted

Quick start
===========

Create a ``duct.yml`` configuration file and run ``ductd``::

    interval: 1.0
    ttl: 60.0

    outputs:
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

::

    ductd -c duct.yml

This starts Duct with a Prometheus scrape endpoint at
``http://0.0.0.0:9100/metrics``. Use ``-v`` / ``--verbose`` to enable
debug logging. The daemon can also be started as a module::

    python -m duct -c duct.yml


Outputs
=======

Outputs define where events go. Configure one or many - events are
fanned out to all of them by default. Give each a ``name`` to enable
:ref:`routing`.

Prometheus
----------

Runs an embedded HTTP server exposing a Prometheus scrape endpoint::

    outputs:
      - output: duct.outputs.prometheus.Prometheus
        port: 9100          # default
        metric_path: metrics  # default → /metrics
        prefix: duct_         # prepended to every metric name

InfluxDB 3
----------

Writes events via the InfluxDB 3 line-protocol HTTP API::

    outputs:
      - output: duct.outputs.influxdb3.InfluxDB3
        url: http://localhost:8086
        database: mymetrics
        token: my-token

NATS
----

Publishes events to NATS subjects. Supports core NATS and JetStream,
and three serialisation formats: ``senml-json`` (default),
``senml-cbor``, and ``json``::

    outputs:
      - output: duct.outputs.nats.Nats
        servers:
          - nats://localhost:4222
        format: senml-cbor
        jetstream: true

Elasticsearch
-------------

::

    outputs:
      - output: duct.outputs.elasticsearch.ElasticSearch
        url: http://127.0.0.1:9200/

Graphite
--------

::

    outputs:
      - output: duct.outputs.graphite.Graphite
        server: localhost
        port: 2003

Other supported outputs: ``duct.outputs.opentsdb.OpenTSDB``,
``duct.outputs.bosun.Bosun``, ``duct.outputs.riemann.RiemannTCP``,
``duct.outputs.riemann.RiemannUDP``.

.. _routing:

Routing sources
===============

Sources can be routed to specific named outputs rather than all outputs::

    outputs:
      - output: duct.outputs.prometheus.Prometheus
        name: prom
        port: 9100

      - output: duct.outputs.influxdb3.InfluxDB3
        name: influx
        url: http://localhost:8086
        database: mymetrics
        token: my-token

    sources:
      - service: cpu
        source: duct.sources.linux.basic.CPU
        interval: 1.0
        route: prom           # single output

      - service: memory
        source: duct.sources.linux.basic.Memory
        interval: 10.0
        route:                # or a list
          - prom
          - influx

Set ``default_route`` globally to route unspecified sources to a named
output. Set ``route: '*'`` on a source to send to all outputs regardless
of the default.


Using sources
=============

Sources are Python classes that produce events on a timer. Add them to the
``sources`` list with any source-specific config.

Common options available on every source:

+-------------+-----------+--------------------------------------------------+
| service     | required  | Metric name prefix (dot-separated sub-metrics    |
|             |           | are appended automatically)                      |
+-------------+-----------+--------------------------------------------------+
| interval    | required  | Poll interval in seconds (float)                 |
+-------------+-----------+--------------------------------------------------+
| ttl         | optional  | Metric time-to-live in seconds                   |
+-------------+-----------+--------------------------------------------------+
| hostname    | optional  | Override the default system FQDN                 |
+-------------+-----------+--------------------------------------------------+
| tags        | optional  | Comma-separated list of tags                     |
+-------------+-----------+--------------------------------------------------+
| route       | optional  | Output name or list of names to route to         |
+-------------+-----------+--------------------------------------------------+
| watchdog    | optional  | Restart the source if it stalls (set ``true``)   |
+-------------+-----------+--------------------------------------------------+

Example - CPU with thresholds::

    sources:
      - service: cpu
        source: duct.sources.linux.basic.CPU
        interval: 1.0
        warning:  { cpu: "> 0.5" }
        critical: { cpu: "> 0.8" }

Sources that return multiple metrics append a prefix. The Ping source
returns both ``latency`` and ``loss``::

    sources:
      - service: googledns
        source: duct.sources.network.Ping
        interval: 60.0
        destination: 8.8.8.8
        critical:
          googledns.latency: "> 100"
          googledns.loss:    "> 0"


State triggers
==============

``warning`` and ``critical`` expressions can use regular expressions as
keys to match sources that produce per-device metrics::

    sources:
      - service: network
        source: duct.sources.linux.basic.Network
        interval: 5.0
        critical:
          network.\w+.tx_packets: "> 1000"

The expression is evaluated against ``event.metric``. Matching events
have their state overridden.


Receiving telemetry
===================

Sources are not limited to polling - they can run embedded servers and
receive events pushed from other systems.

Riemann TCP server
------------------

Accept events from other Riemann-speaking agents::

    sources:
      - service: riemann-in
        source: duct.sources.riemann.RiemannTCP
        port: 5555

sFlow collector
---------------

Listen for sFlow datagrams from network switches::

    sources:
      - service: sflow
        source: duct.sources.sflow.sFlow
        port: 6343

NATS subscriber
---------------

Subscribe to NATS subjects and route the events onward::

    sources:
      - service: nats-in
        source: duct.sources.nats.Nats
        servers:
          - nats://localhost:4222
        topics:
          - "metrics.>"
        format: senml-json

Munin node
----------

Poll a local or remote Munin node and translate its plugins into
Duct events::

    sources:
      - service: munin
        source: duct.sources.munin.MuninNode
        host: localhost
        interval: 60.0


SSH remote checks
=================

Any source with ``ssh = True`` supports running transparently on a remote
host. SSH connections are pooled per host/user/key combination.

Set credentials globally and override per-source as needed::

    ssh_username: monitor
    ssh_keyfile: /etc/duct/id_ed25519

    sources:
      - service: load
        source: duct.sources.linux.basic.LoadAverage
        use_ssh: true
        ssh_host: web01.example.com
        interval: 30.0

``ssh_knownhosts_file``, ``ssh_username``, ``ssh_keyfile``,
``ssh_password``, and ``ssh_keypass`` can all be set globally or
per-source. An inline ``ssh_key`` YAML blob is also accepted.

.. note::
   Duct does not perform host key verification by default.

Use blueprint macros to apply a toolbox of checks to many hosts without
repeating config - see :doc:`blueprints`.


Writing a plugin
================

A source is a Python class. It can use any Python library, open any
connection, or start any server:

.. code-block:: python

    from zope.interface import implementer
    from duct.interfaces import IDuctSource
    from duct.objects import Source

    @implementer(IDuctSource)
    class MySource(Source):
        def __init__(self, *a, **kw):
            Source.__init__(self, *a, **kw)
            self.myopt = self.config.get('myopt', 'default')

        async def get(self):
            value = await fetch_something(self.myopt)
            return self.createEvent('ok', 'My metric', value, prefix='mymetric')

Point to it in config by dotted import path::

    sources:
      - service: mything
        source: mypackage.mysource.MySource
        interval: 10.0
        myopt: custom-value

An output follows the same pattern: subclass ``Output``, implement
``createClient`` for setup (start servers, open connections), and drain
the event queue - see :doc:`outputs` for a walkthrough.
