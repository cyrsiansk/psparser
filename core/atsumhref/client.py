import asyncio
import logging
import re
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from ..http import HTTPClient
from .config import RandomLinkClientConfig

log = logging.getLogger("randomlink.client")

try:
    import phonenumbers
    from phonenumbers import NumberParseException, PhoneNumberFormat
    _HAS_PHONENUMBERS = True
except Exception:
    _HAS_PHONENUMBERS = False

_PHONE_RE = re.compile(r"""
    (?:
      \+?\d{1,3}[\s\-.]?
    )?
    \(?\d{3}\)?[\s\-.]?
    \d{3}[\s\-.]?\d{4}
""", re.VERBOSE)

_EXT_RE = re.compile(r"(?:ext|x|extension)\s*[:.]?\s*\d+$", re.IGNORECASE)

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_URL_IN_TEXT_RE = re.compile(r"(?:https?://|www\.)[^\s\"'<>]+", re.IGNORECASE)
_CLEAN_TRIM_RE = re.compile(r"^[\s\-._()\[\]:;,+]+|[\s\-._()\[\]:;,+]+$")


class RandomLinkClient:
    def __init__(self, http: HTTPClient, cfg: RandomLinkClientConfig = RandomLinkClientConfig()):
        self._http = http
        self._cfg = cfg

    async def fetch_text(self, url: str) -> Optional[str]:
        try:
            return await self._http.get_text(url)
        except Exception:
            log.debug("Failed to GET %s", url, exc_info=True)
            return None

    @staticmethod
    def _unique_preserve_order(items: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for it in items:
            if it and it not in seen:
                seen.add(it)
                out.append(it)
        return out

    @staticmethod
    def _extract_hrefs(soup: BeautifulSoup, base_url: str) -> List[str]:
        out = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().startswith("javascript:"):
                continue
            out.append(urljoin(base_url, href))
        return out

    @staticmethod
    def _extract_emails_from_soup(soup: BeautifulSoup) -> List[str]:
        out = []
        for a in soup.find_all("a", href=True):
            if a["href"].lower().startswith("mailto:"):
                addr = a["href"].split(":", 1)[1].split("?")[0]
                out.append(addr)
        text = soup.get_text(" ", strip=True)
        out += _EMAIL_RE.findall(text)
        return RandomLinkClient._unique_preserve_order(out)

    @staticmethod
    def _normalize_host(url: str) -> str:
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            return host
        except Exception:
            return url.lower()

    def _should_exclude_url(self, url: str, base_url: str) -> bool:
        base_host = self._normalize_host(base_url)
        url_host = self._normalize_host(url)

        if self._cfg.exclude_internal and base_host == url_host:
            return True

        low_url = url.lower()
        for pat in self._cfg.exclude_patterns:
            if pat in low_url:
                return True

        return False

    def _extract_urls_from_text(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        out = RandomLinkClient._extract_hrefs(soup, base_url)
        text = soup.get_text(" ", strip=True)
        for m in _URL_IN_TEXT_RE.findall(text):
            if m.lower().startswith("www."):
                m = "http://" + m
            try:
                out.append(urljoin(base_url, m))
            except Exception:
                out.append(m)
        out = [u for u in out if not u.lower().startswith(("mailto:", "tel:"))]

        out = [
            u for u in out
            if not self._should_exclude_url(u, base_url)
        ]
        return RandomLinkClient._unique_preserve_order(out)

    def _is_valid_url(self, url: str) -> bool:
        try:
            parts = urlparse(url)
            return parts.scheme in ("http", "https") and bool(parts.netloc)
        except Exception:
            return False

    def extract_data_from_html(self, html: str, base_url: str) -> Dict[str, List[str]]:
        soup = BeautifulSoup(html, "html.parser")
        emails = self._extract_emails_from_soup(soup)
        phones = self._extract_phones_from_soup(soup)
        urls = [u for u in self._extract_urls_from_text(soup, base_url) if self._is_valid_url(u)]
        return {"phones": phones, "emails": emails, "urls": urls}

    def _to_e164_from_digits(self, digits: str, had_plus: bool) -> Optional[str]:
        if not digits:
            return None

        if len(digits) == 10:
            return "+1" + digits
        if len(digits) == 11 and digits.startswith("1"):
            return "+" + digits

        if had_plus and 8 <= len(digits) <= 15:
            return "+" + digits

        return None

    def _normalize_phone(self, raw: str) -> Optional[str]:
        if not raw:
            return None

        s = raw.strip()

        s = _EXT_RE.sub("", s)

        s = _CLEAN_TRIM_RE.sub("", s)
        s = s.strip(" .,-/;")

        if _HAS_PHONENUMBERS:
            try:
                pn = phonenumbers.parse(s, "US")
                if phonenumbers.is_possible_number(pn) and phonenumbers.is_valid_number(pn):
                    return phonenumbers.format_number(pn, PhoneNumberFormat.E164)
            except NumberParseException:
                pass

        had_plus = s.startswith("+")
        digits_only = "".join(ch for ch in s if ch.isdigit())

        if len(digits_only) < 10:
            return None
        if len(digits_only) > 15:
            return None

        e164 = self._to_e164_from_digits(digits_only, had_plus)
        return e164

    def _extract_phones_from_soup(self, soup: BeautifulSoup) -> List[str]:
        out: List[str] = []
        seen: Set[str] = set()

        for a in soup.find_all("a", href=True):
            if a["href"].lower().startswith("tel:"):
                raw = a["href"].split(":", 1)[1].split("?")[0]
                norm = self._normalize_phone(raw)
                if norm and norm not in seen:
                    seen.add(norm)
                    out.append(norm)

        text = soup.get_text(" ", strip=True)
        for m in _PHONE_RE.findall(text):
            raw = m if isinstance(m, str) else " ".join(m)
            norm = self._normalize_phone(raw)
            if norm and norm not in seen:
                seen.add(norm)
                out.append(norm)

        return out

    async def collect_from_urls(self, urls: List[str]) -> Dict[str, Any]:
        out = {}
        sem = asyncio.Semaphore(self._cfg.concurrency)

        async def _fetch_one(u: str):
            async with sem:
                log.info("Fetching %s", u)
                text = await self.fetch_text(u)
                if not text:
                    out[u] = {"phones": [], "emails": [], "urls": []}
                    return
                try:
                    data = self.extract_data_from_html(text, u)
                    out[u] = {**data}
                except Exception:
                    log.exception("Failed to parse %s", u)
                    out[u] = {"phones": [], "emails": [], "urls": []}

        tasks = [asyncio.create_task(_fetch_one(u)) for u in urls]
        if tasks:
            await asyncio.gather(*tasks)
        return out
