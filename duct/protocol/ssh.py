"""
.. module:: ssh
   :synopsis: Provides a simplified SSH client interface via asyncssh

.. moduleauthor:: Colin Alston <colin@tamvera.com>
"""
import os
import asyncio
import logging

import asyncssh

log = logging.getLogger(__name__)


class SSHClient(object):
    """Create an SSH connection and tunnel commands over it.

    :param hostname: Hostname to connect to
    :param username: Username to authenticate with
    :param port: Port to connect to (default: 22)
    :param password: Optional password for authentication
    :param knownhosts: Known hosts file path
    """

    def __init__(self, hostname, username, port, password=None,
                 knownhosts=None):
        self.hostname = hostname
        self.username = username
        self.port = int(port)
        self.password = password
        self.connection = None

        self.knownhosts = None
        if knownhosts:
            if os.path.exists(knownhosts):
                self.knownhosts = knownhosts

        self._client_keys = []

    def addKeyFile(self, kfile, password=None):
        """Import a private key from a file"""
        if not os.path.exists(kfile):
            raise FileNotFoundError(f"Key file not found: {kfile}")
        key = asyncssh.read_private_key(kfile, passphrase=password)
        self._client_keys.append(key)

    def addKeyString(self, kstring, password=None):
        """Import a private key from a string"""
        key = asyncssh.import_private_key(kstring, passphrase=password)
        self._client_keys.append(key)

    async def connect(self):
        """Open connection to SSH server"""
        log.info("Opening SSH connection to %s@%s:%s",
                 self.username, self.hostname, self.port)

        connect_kwargs = dict(
            host=self.hostname,
            port=self.port,
            username=self.username,
            known_hosts=self.knownhosts,
        )

        if self._client_keys:
            connect_kwargs['client_keys'] = self._client_keys

        if self.password:
            connect_kwargs['password'] = self.password

        self.connection = await asyncssh.connect(**connect_kwargs)
        log.info("Established SSH connection to %s", self.hostname)

    async def fork(self, command, args=(), env=None, path=None, _timeout=3600):
        """Execute a remote command. Returns (stdout, stderr, exit_code)."""
        if not self.connection:
            return (None, "SSH not ready", 255)

        full_cmd = command
        if path:
            full_cmd = f'PATH={path} {full_cmd}'
        if env:
            prefix = ' '.join(f'{k}={v}' for k, v in env.items())
            full_cmd = prefix + ' ' + full_cmd
        if args:
            full_cmd += ' ' + ' '.join(args)

        try:
            result = await asyncio.wait_for(
                self.connection.run(full_cmd),
                timeout=_timeout
            )
            return result.stdout, result.stderr, result.exit_status or 0
        except asyncio.TimeoutError:
            return (None, "SSH command timed out", 1)
        except Exception as e:
            log.warning("SSH fork error: %s", e)
            return (None, str(e), 1)
