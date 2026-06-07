"""
.. module:: riemann
   :platform: Unix
   :synopsis: A source module which provides a Riemann server

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import asyncio
import struct
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source, Event
from duct.ihateprotobuf import proto_pb2

log = logging.getLogger(__name__)


async def _handle_client(reader, writer, source):
    """Handle one TCP client connection on the Riemann server"""
    try:
        while True:
            length_bytes = await reader.readexactly(4)
            length = struct.unpack('!I', length_bytes)[0]
            data = await reader.readexactly(length)

            message = proto_pb2.Msg()
            message.ParseFromString(data)

            for event in message.events:
                source.queueBack(
                    Event(
                        event.state,
                        event.service,
                        event.description,
                        event.metric_f,
                        event.ttl,
                        hostname=event.host,
                        evtime=event.time,
                    )
                )

            response = proto_pb2.Msg(ok=True).SerializeToString()
            writer.write(struct.pack('!I', len(response)) + response)
            await writer.drain()

    except asyncio.IncompleteReadError:
        pass
    except ConnectionResetError:
        pass
    except Exception as e:
        log.warning('Riemann server client error: %s', e)
    finally:
        writer.close()


@implementer(IDuctSource)
class RiemannTCP(Source):
    """Provides a listening Riemann TCP server that accepts metrics
    and proxies them to the queue.

    :param port: Port to listen on (default: 5555)
    :type port: int.
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)
        self._server = None

    async def startTimer(self):
        """Creates a Riemann TCP server instead of a polling timer"""
        port = int(self.config.get('port', 5555))
        self._server = await asyncio.start_server(
            lambda r, w: _handle_client(r, w, self),
            '0.0.0.0', port,
        )
        log.info('Riemann TCP server listening on port %s', port)

    async def stopTimer(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def get(self):
        pass
