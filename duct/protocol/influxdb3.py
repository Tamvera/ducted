"""
.. module:: influxdb3
   :synopsis: InfluxDB 3 protocol module

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
from base64 import b64encode

from duct import utils


def _escape_tag(value):
    """Escape a tag key or value for line protocol"""
    return str(value).replace(',', r'\,').replace('=', r'\=').replace(' ', r'\ ')


def _escape_measurement(value):
    """Escape a measurement name for line protocol"""
    return str(value).replace(',', r'\,').replace(' ', r'\ ')


def _format_field(value):
    """Format a field value for line protocol"""
    if isinstance(value, bool):
        return 't' if value else 'f'
    if isinstance(value, int):
        return f'{value}i'
    if isinstance(value, float):
        return repr(value)
    return f'"{str(value).replace(chr(34), chr(92) + chr(34))}"'


def to_line_protocol(measurement, tags, fields, timestamp_ns):
    """Encode a single point in InfluxDB line protocol.

    :param measurement: Measurement name
    :param tags: Dict of tag key/value pairs
    :param fields: Dict of field key/value pairs
    :param timestamp_ns: Timestamp in nanoseconds
    :returns: Line protocol string (no trailing newline)
    """
    tag_set = ','.join(
        f'{_escape_tag(k)}={_escape_tag(v)}'
        for k, v in sorted(tags.items())
    )
    field_set = ','.join(
        f'{_escape_tag(k)}={_format_field(v)}'
        for k, v in fields.items()
    )
    meas = _escape_measurement(measurement)
    if tag_set:
        return f'{meas},{tag_set} {field_set} {timestamp_ns}'
    return f'{meas} {field_set} {timestamp_ns}'


class InfluxDB3Client:
    """InfluxDB 3 HTTP write client"""

    def __init__(self, url='http://localhost:8086', database='duct',
                 token=None, precision='nanosecond'):
        self.url = url.rstrip('/')
        self.database = database
        self.token = token
        self.precision = precision

    def _headers(self):
        headers = {'Content-Type': 'text/plain; charset=utf-8'}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        return headers

    async def write(self, lines):
        """Write line protocol data to InfluxDB 3.

        :param lines: List of line protocol strings
        """
        body = '\n'.join(lines)
        url = (f'{self.url}/api/v3/write_lp'
               f'?db={self.database}&precision={self.precision}')
        return await utils.HTTPRequest().getBody(
            url, 'POST', headers=self._headers(), data=body.encode())
