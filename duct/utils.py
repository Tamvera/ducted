"""
.. module:: utils
   :synopsis: Utility wrappers for HTTP calls and process forks

.. moduleauthor:: Colin Alston <colin@imcol.in>
"""

import json
import time
import os
import asyncio
import logging

import aiohttp

log = logging.getLogger(__name__)


async def wait(msecs):
    """Async delay in milliseconds"""
    await asyncio.sleep(msecs / 1000.0)


class Timeout(Exception):
    """Raised when an operation exceeds its timeout."""


def reverseNameFromIPAddress(address):
    """Returns PTR record name for IP address"""
    return '.'.join(reversed(address.split('.'))) + '.in-addr.arpa'


class Resolver(object):
    """Helper class for DNS resolution with caching"""

    def __init__(self):
        self.recs = {}

    async def reverse(self, ip):
        """Perform a reverse lookup on `ip`"""
        if ip in self.recs:
            host, ttl, ti = self.recs[ip]
            if (time.time() - ti) < ttl:
                return host

        try:
            loop = asyncio.get_event_loop()
            results = await loop.getaddrinfo(
                reverseNameFromIPAddress(ip), None,
                type=asyncio.socket.SOCK_DGRAM
            )
            if results:
                host = results[0][4][0]
                self.recs[ip] = (host, 300, time.time())
                return host
        except Exception:
            pass

        return ip


async def fork(executable, args=(), env=None, path=None, timeout=3600):
    """Execute a subprocess and return (stdout, stderr, returncode).

    :param executable: Path to executable
    :param args: Tuple of arguments
    :param env: Environment dictionary (None inherits from parent)
    :param path: Working directory
    :param timeout: Kill process if timeout exceeded (seconds)
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            executable, *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env or None,
            cwd=path,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            log.warning('Killed source process: Timeout %s exceeded', timeout)
            raise Timeout("Process took longer than %s seconds" % timeout)

        return stdout.decode(), stderr.decode(), proc.returncode

    except FileNotFoundError as e:
        raise Exception("Executable not found: %s" % executable) from e


def _make_auth_headers(user, password, headers):
    """Add Basic auth header if credentials provided"""
    if user:
        import base64
        token = base64.b64encode(
            ('%s:%s' % (user, password)).encode()
        ).decode()
        headers['Authorization'] = 'Basic ' + token
    return headers


class HTTPRequest(object):
    """Helper class for async HTTP requests via aiohttp.

    :param timeout: Request timeout in seconds (default: 120)
    """

    def __init__(self, timeout=120):
        self.timeout = timeout

    def _make_connector(self, socket=None):
        if socket:
            return aiohttp.UnixConnector(path=socket)
        return None

    async def getBody(self, url, method='GET', headers=None, data=None,
                      socket=None, follow_redirect=True):
        """Make an HTTP request and return the response body as a string.

        :param url: URL to request
        :param method: HTTP method (default: GET)
        :param headers: Dict of headers
        :param data: Request body bytes or str
        :param socket: Unix socket path for local HTTP
        :param follow_redirect: Follow 301/302 redirects (default: True)
        """
        if headers is None:
            headers = {}

        if 'User-Agent' not in headers:
            headers['User-Agent'] = 'Ductd/2'

        connector = self._make_connector(socket)

        timeout = aiohttp.ClientTimeout(total=self.timeout)

        if isinstance(data, str):
            data = data.encode()

        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
            ) as session:
                async with session.request(
                    method, url,
                    headers=headers,
                    data=data,
                    allow_redirects=follow_redirect,
                    ssl=False if url.startswith('https') else None,
                ) as response:
                    if response.status < 200 or response.status > 299:
                        body = await response.text()
                        raise Exception((response.status, body))
                    return await response.text()

        except aiohttp.ServerTimeoutError:
            raise Timeout("Request took longer than %s seconds" % self.timeout)

    async def getJson(self, url, method='GET', headers=None, data=None,
                      socket=None):
        """Fetch a JSON result via HTTP"""
        if headers is None:
            headers = {}

        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        body = await self.getBody(url, method, headers, data, socket)

        if not body:
            return {}

        try:
            return json.loads(body)
        except ValueError:
            raise ValueError("Response was not JSON: %s" % repr(body))


class PersistentCache(object):
    """A basic dictionary cache backed by a JSON file."""

    def __init__(self, location='/var/lib/duct/cache'):
        self.store = {}
        self.location = location
        self.mtime = 0
        self._read()

    def _changed(self):
        if os.path.exists(self.location):
            mtime = os.stat(self.location).st_mtime
            return self.mtime != mtime
        return False

    def _acquire_cache(self):
        try:
            with open(self.location, 'r') as f:
                return json.loads(f.read())
        except (IOError, OSError):
            return {}

    def _write_cache(self, data):
        with open(self.location, 'w') as f:
            f.write(json.dumps(data))

    def _persist(self):
        cache = self._acquire_cache()
        for key, val in self.store.items():
            cache[key] = val
        self._write_cache(cache)

    def _read(self):
        cache = self._acquire_cache()
        for key, val in cache.items():
            self.store[key] = val

    def _remove_key(self, key):
        cache = self._acquire_cache()
        cache.pop(key, None)
        self.store.pop(key, None)
        self._write_cache(cache)

    def expire(self, age):
        """Expire any items in the cache older than `age` seconds"""
        now = time.time()
        cache = self._acquire_cache()
        expired = [key for key, val in cache.items() if (now - val[0]) > age]
        for key in expired:
            cache.pop(key, None)
            self.store.pop(key, None)
        self._write_cache(cache)

    def set(self, key, val):
        """Set a key to value `val`"""
        self.store[key] = (time.time(), val)
        self._persist()

    def get(self, k):
        """Returns (timestamp, value) tuple for key, or None"""
        if self._changed():
            self._read()
        if k in self.store:
            return tuple(self.store[k])
        return None

    def contains(self, k):
        """Return True if key `k` exists"""
        if self._changed():
            self._read()
        return k in self.store

    def delete(self, k):
        """Remove key `k` from the cache"""
        self._remove_key(k)
