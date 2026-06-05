"""
.. module:: aggregators
   :synopsis: Aggregation functions

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""


def Counter32(al, br, delta):
    """32bit counter aggregator with wrapping
    """
    if br < al:
        ch = 4294967295 - al
        return (ch + br) / float(delta)

    return (br - al) / float(delta)

def Counter64(al, br, delta):
    """64bit counter aggregator with wrapping
    """
    if br < al:
        ch = 18446744073709551615 - al
        return (ch + br) / float(delta)

    return (br - al) / float(delta)

def Counter(al, br, delta):
    """Counter derivative
    """
    if br < al:
        return None

    return (br - al) / float(delta)
