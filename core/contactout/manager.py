import hashlib
import json
import os
from typing import Any, Dict, List, Optional

from .client import ContactOutClient
from .exceptions import OutOfCreditsError, NoAccessError, RateLimitError, ContactOutError


class DiskCache:
    def __init__(self, path: str):
        self._path = path
        self._data: Dict[str, Any] = {}
        self._load()

    def _load(self):
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}
        else:
            self._data = {}

    def save(self):
        tmp_path = self._path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self._path)

    def _make_key(self, method: str, path: str, payload: Optional[dict]) -> str:
        h = hashlib.sha256()
        h.update(method.encode("utf-8"))
        h.update(path.encode("utf-8"))
        if payload is not None:
            h.update(json.dumps(payload, sort_keys=True).encode("utf-8"))
        return h.hexdigest()

    def get(self, method: str, path: str, payload: Optional[dict]) -> Optional[Any]:
        key = self._make_key(method, path, payload)
        return self._data.get(key)

    def set(self, method: str, path: str, payload: Optional[dict], value: Any):
        key = self._make_key(method, path, payload)
        self._data[key] = value
        self.save()


class ContactOutTokenManager:
    def __init__(self, tokens: List[str], cache_path: str):
        if not tokens:
            raise ValueError("Tokens list cannot be empty")
        self._tokens = tokens
        self._quotas: Dict[str, Dict[str, int]] = {}
        self._cache = DiskCache(cache_path)

    async def initialize(self):
        for token in self._tokens:
            async with ContactOutClient(token) as client:
                stats = await client.get_stats()
                print(f"Token {token} has stats: {stats}")
                self._quotas[token] = self._extract_quota(stats)

    def _extract_quota(self, stats: Dict[str, Any]) -> Dict[str, int]:
        usage = stats.get("usage", {})
        return {
            "quota": usage.get("remaining", usage.get("quota", 0)),
            "phone_quota": usage.get("phone_remaining", usage.get("phone_quota", 0)),
            "search_quota": usage.get("search_remaining", usage.get("search_quota", 0)),
        }

    async def enrich(self, **kwargs) -> Dict[str, Any]:
        cached = self._cache.get("POST", "/people/enrich", kwargs)
        if cached is not None:
            return cached

        required_quota = self._determine_required_quota(kwargs)

        token = self._select_token(required_quota)
        if not token:
            raise OutOfCreditsError("No token has enough quota for this request")

        async with ContactOutClient(token) as client:
            try:
                res = await client.enrich_person(**kwargs)
            except (OutOfCreditsError, NoAccessError):
                await self._refresh_token_quota(token)
                return await self.enrich(**kwargs)
            except RateLimitError as e:
                raise e
            except ContactOutError as e:
                raise e

        await self._refresh_token_quota(token)

        self._cache.set("POST", "/people/enrich", kwargs, res)

        return res

    def _determine_required_quota(self, kwargs: Dict[str, Any]) -> Dict[str, int]:
        include = kwargs.get("include") or []
        rq = {"quota": 0, "phone_quota": 0, "search_quota": 0}
        if "work_email" in include or "personal_email" in include:
            rq["quota"] = 1
        if "phone" in include:
            rq["phone_quota"] = 1
        if not any(kwargs.get(k) for k in ["linkedin_url", "email", "phone"]):
            rq["search_quota"] = 1
        return rq

    def _select_token(self, required: Dict[str, int]) -> Optional[str]:
        for token, quotas in self._quotas.items():
            if all(quotas.get(k, 0) >= required[k] for k in required):
                return token
        return None

    async def _refresh_token_quota(self, token: str):
        async with ContactOutClient(token) as client:
            stats = await client.get_stats()
            self._quotas[token] = self._extract_quota(stats)
