"""Duct - A monitoring agent

.. moduleauthor:: Colin Alston <colin@tamvera.com>

"""

from duct import service

def makeService(config):
    """
    Create DuctService object
    """
    return service.DuctService(config)
