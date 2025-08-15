import asyncio
import random
import socket
import logging
import ssl
from typing import Optional
from urllib.parse import urlparse, urlunparse

import certifi
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp.client_exceptions import ClientConnectorDNSError, ClientConnectorCertificateError
from aiohttp.resolver import AsyncResolver

from .config import HTTPOptions

log = logging.getLogger("core.http")


def _strip_www(url: str) -> str:
    parts = urlparse(url)
    hostname = parts.hostname or ""
    if hostname.startswith("www."):
        new_host = hostname[len("www."):]
        new_netloc = new_host
        if parts.port:
            new_netloc = f"{new_host}:{parts.port}"
        new_parts = parts._replace(netloc=new_netloc)
        return urlunparse(new_parts)
    return url


class HTTPClient:
    def __init__(
            self,
            session: Optional[ClientSession] = None,
            opts: HTTPOptions = HTTPOptions(),
            *,
            max_retries: int = 3,
            backoff_factor: float = 0.5,
            prefer_ipv4: bool = False,
            dns_cache_ttl: int = 300,
            insecure_host_whitelist: Optional[set] = None,
    ):
        self._external_session = session is not None
        self._session = session
        self._opts = opts

        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

        self._prefer_ipv4 = prefer_ipv4
        self._dns_cache_ttl = dns_cache_ttl
        self._connector: Optional[TCPConnector] = None

        self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._insecure_ssl_context = ssl.create_default_context()
        self._insecure_ssl_context.check_hostname = False
        self._insecure_ssl_context.verify_mode = ssl.CERT_NONE

        self._insecure_whitelist = insecure_host_whitelist or set()

    async def __aenter__(self) -> "HTTPClient":
        if self._session is None:
            timeout = ClientTimeout(total=self._opts.timeout)
            resolver = AsyncResolver()
            family = socket.AF_INET if self._prefer_ipv4 else 0
            self._connector = TCPConnector(
                resolver=resolver,
                ttl_dns_cache=self._dns_cache_ttl,
                family=family,
                ssl=self._ssl_context,
            )
            self._session = ClientSession(timeout=timeout, headers=self._opts.headers, connector=self._connector, trust_env=True)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session and not self._external_session:
            await self._session.close()
            self._session = None
        if self._connector:
            self._connector = None

    def _ensure(self) -> ClientSession:
        if self._session is None:
            raise RuntimeError("HTTP session is not started. Use 'async with HTTPClient(...)'.")
        return self._session

    async def _request_with_retries(self, method: str, url: str, **kwargs):
        sess = self._ensure()
        last_exc = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await getattr(sess, method)(url, **kwargs)
                resp.raise_for_status()
                return resp
            except ClientConnectorDNSError as e:
                log.warning("DNS error for %s (attempt %d/%d): %s", url, attempt, self._max_retries, e)
                last_exc = e
                if attempt == self._max_retries:
                    raise
                sleep_for = self._backoff_factor * (2 ** (attempt - 1))
                sleep_for = sleep_for + random.uniform(0, 0.1 * sleep_for)
                await asyncio.sleep(sleep_for)
            except ClientConnectorCertificateError as e:
                log.warning("Certificate error for %s (attempt %d/%d): %s", url, attempt, self._max_retries, e)
                last_exc = e

                alt_url = _strip_www(url)
                if alt_url != url:
                    log.info("Retrying with stripped 'www' host: %s -> %s", url, alt_url)
                    try:
                        resp = await getattr(sess, method)(alt_url, **kwargs)
                        resp.raise_for_status()
                        return resp
                    except Exception as e2:
                        log.warning("Retry with stripped host failed: %s", e2)
                        last_exc = e2

                try:
                    host = urlparse(url).hostname or ""
                except Exception:
                    host = ""

                if host in self._insecure_whitelist:
                    log.warning("Performing insecure retry for %s because it's in insecure_whitelist (INSECURE).", host)
                    try:
                        resp = await getattr(sess, method)(url, ssl=self._insecure_ssl_context, **kwargs)
                        resp.raise_for_status()
                        return resp
                    except Exception as e2:
                        log.warning("Insecure retry also failed: %s", e2)
                        last_exc = e2

                if attempt == self._max_retries:
                    raise last_exc
                sleep_for = self._backoff_factor * (2 ** (attempt - 1))
                sleep_for = sleep_for + random.uniform(0, 0.1 * sleep_for)
                await asyncio.sleep(sleep_for)
            except Exception as e:
                log.exception("Request to %s failed unexpectedly: %s", url, e)
                raise

        if last_exc:
            raise last_exc

    async def get_json(self, url: str, params: dict | None = None):
        resp = await self._request_with_retries("get", url, params=params)
        return await resp.json()

    async def get_text(self, url: str, params: dict | None = None) -> str:
        resp = await self._request_with_retries("get", url, params=params)
        return await resp.text()
