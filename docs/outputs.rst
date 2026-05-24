Outputs
*******

Introduction
============

Outputs are Python objects which subclass :class:`duct.objects.Output`. They
are constructed with a dictionary parsed from the YAML configuration block
which defines them, and as such can read any attributes from that either
optional or mandatory.

Since outputs are constructed at startup time they can retain any required
state. A copy of the queue is passed to all 
:method:`duct.objects.Output.eventsReceived` calls which happen at each 
queue `interval` config setting as the queue is emptied. This list of
:class:`duct.objects.Event` objects must not be altered by the output.

The `output` configuration option is passed a string representing an object
the same way as `sources` configurations are. For example this outputs events
to Riemann over TCP::

    outputs:
        - output: duct.outputs.riemann.RiemannTCP
          server: 127.0.0.1
          port: 5555

Using TLS with Riemann
======================

The RiemannTCP output also supports TLS, which can make use of Puppet certs for
convenience ::

    outputs:
        - output: duct.outputs.riemann.RiemannTCP
          server: 127.0.0.1
          port: 5554
          tls: true
          cert: /var/lib/puppet/ssl/certs/test.acme.com.pem
          key: /var/lib/puppet/ssl/private_keys/test.acme.com.pem

Writing your own outputs
========================

An output class should subclass :class:`duct.objects.Output`.

The output can implement a ``createClient`` coroutine which starts the output
(opens connections, etc.) at startup. The output must also have an
``eventsReceived`` method which receives a list of :class:`duct.objects.Event`
objects; it may be a plain method or an ``async def`` coroutine.

An example logging output::

    import logging

    from duct.objects import Output

    log = logging.getLogger(__name__)

    class Logger(Output):
        def eventsReceived(self, events):
            log.info("Events dequeued: %s", len(events))

If you save this as `test.py` the basic configuration you need is simply ::

    outputs:
        - output: duct.outputs.riemann.RiemannUDP
          server: localhost
          port: 5555

        - output: test.Logger

You should now see how many events are exiting in the Duct log ::

    2024-01-01 15:35:28 root INFO Events dequeued: 7
    2024-01-01 15:35:29 root INFO Events dequeued: 2
    2024-01-01 15:35:30 root INFO Events dequeued: 3

Events can be routed in different ways to outputs, see the Getting started
guide for more details
