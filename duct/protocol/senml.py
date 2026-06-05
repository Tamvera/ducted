"""
.. module:: senml
   :synopsis: SenML Protocol support

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""

import json
from base64 import b64encode

from duct import utils
from duct.objects import Event

def event_to_senml(event: Event) -> str:
    """Converts a Duct event to SenML
    """
    return json.dumps({
        "n": f"{event.hostname}.{event.service}",
        "t": event.time,
        "v": event.metric
    })

def senml_to_event(data: str, ttl=60.0, hostname="") -> Event:
    """Converts a SenML message to an Event object
    """

    data_dict = json.loads(data)

    return Event("ok", data_dict.get("n", ""), "", data_dict.get("v", 0.0), ttl, hostname=hostname)
