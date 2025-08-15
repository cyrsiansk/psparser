import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from .models import Vendor, LenientJSONDecoder
from .parser import merge_next_f_scripts
from .config import PartySlateClientConfig
from ..http import HTTPClient
from ..atsumhref import RandomLinkClient, RandomLinkClientConfig

log = logging.getLogger("partyslate.client")

class PartySlateClient:
    def __init__(self, http: HTTPClient, config: PartySlateClientConfig = PartySlateClientConfig()):
        self._http = http
        self._cfg = config

    async def get_find_vendors(self, page: int = 1, category: str = "planner", location: Optional[str] = None) -> Dict[str, Any]:
        params = {"category": category or self._cfg.default_category, "location": location or self._cfg.default_location, "page": page}
        params = {k: v for k, v in params.items() if v is not None}
        return await self._http.get_json(self._cfg.find_vendors_url, params=params)

    async def get_vendor_html(self, slug: str) -> str:
        url = f"{self._cfg.vendor_url_base.rstrip('/')}/{slug}"
        return await self._http.get_text(url)

    async def get_url_data(self, url: str):
        try:
            client = RandomLinkClient(http=self._http,
                                      cfg=RandomLinkClientConfig(concurrency=1))
            return (await client.collect_from_urls([url]))[url]
        except Exception:
            log.debug("Failed to collect from %s", url, exc_info=True)
            return {}

    @staticmethod
    def extract_script_tags(html: str) -> ResultSet:
        soup = BeautifulSoup(html, "html.parser")
        return soup.find_all("script")

    @staticmethod
    def extract_data_from_scripts(script_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        try:
            idx = PartySlateClient._get_script2_index(script_entries)
            if idx == -1:
                idx = PartySlateClient._get_script2_alt_index(script_entries)
            if idx != -1:
                script_text = script_entries[idx]["val"]
                data = json.loads(script_text)
                url = data.get("url")
                if url:
                    out["url"] = url
        except Exception:
            log.debug("Failed to parse 2nd script", exc_info=True)

        try:
            idx4 = PartySlateClient._get_script4_index(script_entries)
            if idx4 != -1:
                script_text = script_entries[idx4]["val"]
                inner_json = json.loads(script_text)[3]
                pro_data = inner_json.get("pro", {})
                for key in ("facebookUrl", "instagramUrl"):
                    val = pro_data.get(key)
                    if val:
                        out[key] = val
                team = pro_data.get("teamMembers")
                if team:
                    out["teamMembers"] = {m["name"]: m.get("title") for m in team if "name" in m}
        except Exception:
            log.debug("Failed to parse 4th script", exc_info=True)

        return out

    @staticmethod
    def _get_script2_index(scripts: List[Dict[str, Any]]) -> int:
        for i, script in enumerate(scripts):
            val = script.get("val", "")
            if isinstance(val, str) and val.startswith("{\"@context\""):
                return i
        return -1

    @staticmethod
    def _get_script2_alt_index(scripts: List[Dict[str, Any]]) -> int:
        s_marker = "{\"dangerouslySetInnerHTML\""
        s2 = "{\\\"@context\\\""
        for i, script in enumerate(scripts):
            val = script.get("val", "")
            idx = val.find(s_marker)
            if idx != -1:
                sub = val[idx:]
                if s2 in sub:
                    try:
                        new_script = json.loads(sub, cls=LenientJSONDecoder)
                        inner_html = new_script["dangerouslySetInnerHTML"]["__html"]
                        scripts[i]["val"] = inner_html
                        return i
                    except Exception:
                        log.debug("Alt script2 parse failed at index %d", i, exc_info=True)
        return -1

    @staticmethod
    def _get_script4_index(scripts: List[Dict[str, Any]]) -> int:
        for i, script in enumerate(scripts):
            if script.get("hex_string") == "4":
                return i
        return -1

    async def collect_vendors(self, n: int, start_page: int = 1, fetch_additional_for_each: bool = False, write_output: Optional[str] = None) -> List[Dict[str, Any]]:
        collected: List[Vendor] = []
        page = start_page
        semaphore = asyncio.Semaphore(self._cfg.concurrency)

        async def _fetch_and_parse_extra(v: Vendor):
            if not v.slug:
                return
            async with semaphore:
                try:
                    html = await self.get_vendor_html(v.slug)
                    scripts = self.extract_script_tags(html)
                    parsed = merge_next_f_scripts(scripts, marker_src_substring=self._cfg.marker_chunk_substring)
                    extra = self.extract_data_from_scripts(parsed)
                    if "url" in extra:
                        data = await self.get_url_data(extra["url"])
                        v.url_extra = data
                    v.extra = extra
                except Exception:
                    log.debug("Failed to fetch/parse vendor page for slug '%s'", v.slug, exc_info=True)

        while len(collected) < n:
            vendors_json = await self.get_find_vendors(page=page)
            vendors = vendors_json.get("vendors", []) or []
            if not vendors:
                log.info("No more vendors found on page %d", page)
                break

            page_vendors = [Vendor.from_api_item(item) for item in vendors]
            for v in page_vendors:
                if len(collected) >= n:
                    break
                collected.append(v)
                log.info("Collected vendor %s (%d/%d)", v.slug, len(collected), n)

            if fetch_additional_for_each:
                tasks = [asyncio.create_task(_fetch_and_parse_extra(v)) for v in collected if not v.extra]
                if tasks:
                    await asyncio.gather(*tasks)

            page += 1

        output = [v.to_dict() for v in collected[:n]]
        if write_output:
            try:
                with open(write_output, "w", encoding="utf-8") as f:
                    json.dump(output, f, ensure_ascii=False, indent=2)
                log.info("Wrote output to %s", write_output)
            except Exception:
                log.exception("Failed to write output file %s", write_output)
        return output
