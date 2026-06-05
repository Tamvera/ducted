"""
.. module:: postgresql
   :platform: Unix
   :synopsis: A source module for postgres stats

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import logging

from zope.interface import implementer

from duct.interfaces import IDuctSource
from duct.objects import Source

from duct.aggregators import Counter64

log = logging.getLogger(__name__)


@implementer(IDuctSource)
class PostgreSQL(Source):
    """Reads PostgreSQL metrics

    **Configuration arguments:**

    :param host: Database host
    :type host: str.
    :param port: Database port
    :type port: int.
    :param user: Username
    :type user: str.
    :param password: Password
    :type password: str.

    **Metrics:**

    :(service name).(database name).(metrics): Metrics from pg_stat_database
    """

    def __init__(self, *a, **kw):
        Source.__init__(self, *a, **kw)
        self.user = self.config.get('user', 'postgres')
        self.password = self.config.get('password', '')
        self.port = self.config.get('port', 5432)
        self.host = self.config.get('host', '127.0.0.1')

    async def get(self):
        try:
            import asyncpg  # pylint: disable=import-outside-toplevel
        except ImportError:
            log.error(
                'duct.sources.database.postgresql.PostgreSQL requires asyncpg')
            return None

        try:
            conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database='postgres',
            )
        except Exception as e:
            return self.createEvent(
                'critical',
                f'Connection error: {str(e).replace(chr(10), " ")}',
                0,
                prefix='state'
            )

        cols = (
            ('xact_commit', 'commits'),
            ('xact_rollback', 'rollbacks'),
            ('blks_read', 'disk.read'),
            ('blks_hit', 'disk.cache'),
            ('tup_returned', 'returned'),
            ('tup_fetched', 'selects'),
            ('tup_inserted', 'inserts'),
            ('tup_updated', 'updates'),
            ('tup_deleted', 'deletes'),
        )

        keys, names = zip(*cols)

        try:
            rows = await conn.fetch(
                f"SELECT datname,numbackends,{','.join(keys)}"
                " FROM pg_stat_database"
            )

            for row in rows:
                db = row[0]
                threads = row[1]
                if db not in ('template0', 'template1'):
                    self.queueBack(self.createEvent(
                        'ok',
                        f'threads: {threads}',
                        threads,
                        prefix=f'{db}.threads'
                    ))

                    for i, col in enumerate(list(row)[2:]):
                        self.queueBack(self.createEvent(
                            'ok',
                            f'{names[i]}: {col}',
                            col,
                            prefix=f'{db}.{names[i]}',
                            aggregation=Counter64
                        ))

            return self.createEvent('ok', 'Connection ok', 1, prefix='state')

        except Exception as e:
            return self.createEvent(
                'critical',
                f'Query error: {str(e).replace(chr(10), " ")}',
                0,
                prefix='state'
            )
        finally:
            await conn.close()
