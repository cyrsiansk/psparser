from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Optional, Any, Dict, Sequence

from aiohttp.client_exceptions import ClientResponseError

from core.http import HTTPClient
from .exceptions import (
    ContactOutError,
    BadCredentialsError,
    BadRequestError,
    OutOfCreditsError,
    NoAccessError,
    RateLimitError,
    RemoteServerError,
)


class ContactOutClient:
    DEFAULT_BASE = "https://api.contactout.com/v1"

    def __init__(
            self,
            token: str,
            *,
            http_client: Optional[HTTPClient] = None,
            base_url: str = DEFAULT_BASE,
            max_retries_on_429: int = 2,
            backoff_factor: float = 1.0,
    ):
        if not token:
            raise ValueError("token is required")
        self._token = token
        self._external_http = http_client is not None
        self._http: Optional[HTTPClient] = http_client
        self._base_url = base_url.rstrip("/")
        self._max_retries_on_429 = max_retries_on_429
        self._backoff_factor = backoff_factor

        self._default_headers = {
            "authorization": "basic",
            "token": self._token,
            "accept": "application/json",
        }

    async def __aenter__(self) -> "ContactOutClient":
        if self._http is None:
            self._http = HTTPClient()
            await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._http and not self._external_http:
            await self._http.__aexit__(exc_type, exc, tb)
            self._http = None

    async def _request(
            self,
            method: str,
            path: str,
            *,
            params: Optional[Dict[str, Any]] = None,
            json: Optional[Any] = None,
            headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        if self._http is None:
            raise RuntimeError("HTTP client is not started. Use 'async with ContactOutClient(...)' or pass http_client in constructor.")

        headers_final = dict(self._default_headers)
        if headers:
            headers_final.update(headers)

        url = f"{self._base_url}/{path.lstrip('/')}"
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries_on_429 + 2):
            try:
                resp = await self._http._request_with_retries(method, url, params=params, json=json, headers=headers_final)
                return await resp.json()

            except ClientResponseError as cre:
                status = getattr(cre, "status", None)
                hdrs = getattr(cre, "headers", {}) or {}
                last_exc = cre

                if status == 429:
                    retry_after = None
                    ra = hdrs.get("Retry-After") or hdrs.get("retry-after")
                    if ra is not None:
                        try:
                            retry_after = int(ra)
                        except Exception:
                            retry_after = None

                    if attempt > self._max_retries_on_429:
                        raise RateLimitError(str(cre), retry_after=retry_after) from cre

                    if retry_after is not None:
                        sleep_for = retry_after
                    else:
                        sleep_for = self._backoff_factor * (2 ** (attempt - 1))
                    await asyncio.sleep(sleep_for)
                    continue

                if status == 400:
                    raise BadCredentialsError(str(cre)) from cre
                if status == 401:
                    raise BadRequestError(str(cre)) from cre
                if status == 403:
                    msg = str(cre).lower()
                    if "credit" in msg or "out of" in msg:
                        raise OutOfCreditsError(str(cre)) from cre
                    else:
                        raise NoAccessError(str(cre)) from cre

                if status and 500 <= status < 600:
                    raise RemoteServerError(f"{status}: {cre}") from cre

                raise ContactOutError(str(cre)) from cre

            except Exception as e:
                last_exc = e
                raise

        if last_exc:
            raise last_exc

    async def get_stats(self, period: Optional[str] = None) -> Dict[str, Any]:
        if period is None:
            period = datetime.now().strftime("%Y-%m")
        params = {"period": period}
        return await self._request("get", "stats", params=params)

    async def enrich_person(
            self,
            *,
            linkedin_url: Optional[str] = None,
            email: Optional[str] = None,
            phone: Optional[str] = None,
            full_name: Optional[str] = None,
            first_name: Optional[str] = None,
            last_name: Optional[str] = None,
            company: Optional[Sequence[str]] = None,
            company_domain: Optional[Sequence[str]] = None,
            education: Optional[Sequence[str]] = None,
            location: Optional[str] = None,
            job_title: Optional[str] = None,
            include: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        has_primary = any([linkedin_url, email, phone])
        has_name = bool(full_name) or (bool(first_name) and bool(last_name))
        has_secondary = any([company, company_domain, education, location])

        if not (has_primary or (has_name and has_secondary)):
            raise ValueError(
                "Для поиска нужен один primary (linkedin_url, email или phone) "
                "ИЛИ имя (full_name или first_name+last_name) и как минимум одна secondary (company, company_domain, education, location)."
            )

        payload: Dict[str, Any] = {}
        if linkedin_url:
            payload["linkedin_url"] = linkedin_url
        if email:
            payload["email"] = email
        if phone:
            payload["phone"] = phone

        if full_name:
            payload["full_name"] = full_name
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name

        if company:
            payload["company"] = list(company)[:10]
        if company_domain:
            payload["company_domain"] = list(company_domain)[:10]
        if education:
            payload["education"] = list(education)[:10]

        if location:
            payload["location"] = location
        if job_title:
            payload["job_title"] = job_title
        if include:
            payload["include"] = list(include)

        return await self._request("post", "people/enrich", json=payload)
