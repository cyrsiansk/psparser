from json import JSONDecoder

import aiohttp
from bs4 import BeautifulSoup, ResultSet

_URL_BASE = "https://www.partyslate.com/find-vendors/event-planner/area/miami?page=2"
_URL_CATEGORIES = "https://www.partyslate.com/api/categorical_options"
_URL_FIND_VENDORS = "https://www.partyslate.com/api/seo-text/find-vendors.json"
_URL_FIND_MIAMI_VENDORS = "https://www.partyslate.com/api/find-vendors.json?category=planner&location=miami"
_URL_VENDORS = "https://www.partyslate.com/vendors/"

import re
import json
from typing import List, Any
from bs4.element import Tag

PUSH_RE = re.compile(r'self\.__next_f\.push\(\s*(\[[\s\S]*?])\s*\)', re.MULTILINE)

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)

class JsonDecoderIgnore(JSONDecoder):
    def decode(self, s, _w=WHITESPACE.match):
        obj, end = self.raw_decode(s, idx=_w(s, 0).end())
        return obj

    def get_obj_length(self, s, _w=WHITESPACE.match):
        obj, end = self.raw_decode(s, idx=_w(s, 0).end())
        end = _w(s, end).end()
        return end

dc = JsonDecoderIgnore()


def get_next_hex_string(s: str) -> str:
    res = ""
    alpha = "0123456789abcdef"
    for c in s:
        if c in alpha:
            res += c
        else:
            break
    return res


def get_next_data_type_string(s: str) -> str:
    res = ""
    alpha_stop = "\"{[n"
    for c in s:
        if c == "T":
            return "T"
        if c in alpha_stop:
            break
        else:
            res += c
    return res

def merge_next_f_scripts(script_tags: List[Tag],
                         marker_src_substring: str = "/_next/static/chunks/webpack-88b0373b6b6bc080.js"
                         ) -> list[Any]:
    marker_index = None
    for i, tag in enumerate(script_tags):
        src = tag.get('src') or ''
        if marker_src_substring in src:
            marker_index = i
            break

    if marker_index is None:
        return []

    raw_items = []

    for tag in script_tags[marker_index + 1:]:
        text = tag.string
        if text is None:
            text = tag.get_text(separator="\n")

        for m in PUSH_RE.finditer(text):
            raw = m.group(1).strip()
            raw_items.append(raw)

    parsed_items = []
    for raw in raw_items:
        parsed_items.append(json.loads(raw))

    hmm = ""

    for tp, data in parsed_items:
        if tp == 1:
            hmm += data

    combined = ""
    for tp, data in parsed_items:
        if tp == 1:
            combined += data

    data = []
    other = combined

    with open("scripts_combined.json", "w", encoding="utf-8") as f:
        f.write(combined)

    dc = JsonDecoderIgnore()

    while True:
        entry = {}
        hex_string = get_next_hex_string(other)
        other = other.split(hex_string, 1)[1][1:]

        data_type = get_next_data_type_string(other)
        obj_length = None
        if data_type:
            if data_type == "T":
                length_hex = get_next_hex_string(other[1:])
                length = int(length_hex, 16)
                other = other.split(length_hex, 1)[1][1:]
                obj_length = length
            else:
                other = other.split(data_type, 1)[1]

        if obj_length is None:
            obj_length = dc.get_obj_length(other)
        else:
            try:
                attempt = dc.get_obj_length(other)
                obj_length = attempt
            except:
                pass

        val = other[:obj_length]
        backup_other = other
        other = other[obj_length:]

        if data_type == "T":
            check = [":{\"__typename", "[\"$\",\"$L1f\","]
            for c in check:
                if c in val + other[:20]:
                    index_start = backup_other.find(c) - 2
                    other = backup_other[index_start:]
                    break

        entry['hex_string'] = hex_string
        entry['data_type'] = data_type
        entry['obj_length'] = obj_length
        entry['val'] = val

        data.append(entry)

        if len(other) == 0:
            break

    return data


class PartySlateProvider:
    def __init__(self):
        self._client = None

    async def __aenter__(self):
        self._client = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._client.close()

    async def get_find_miami_vendors(self, page=1):
        payload = {
            "category": "planner",
            "location": "miami",
            "page": page
        }
        async with self._client.get(_URL_FIND_MIAMI_VENDORS + f"&page={page}", params=payload) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Error fetching find vendors: {response.status}")

    async def get_vendor_html(self, slug):
        async with self._client.get(_URL_VENDORS + slug) as response:
            if response.status == 200:
                return await response.text()
            else:
                raise Exception(f"Error fetching find vendors: {response.status}")

    def get_data_from_scripts(self, scripts: list) -> dict:
        data = {}
        data.update(self._get_data_2nd_script(scripts))
        data.update(self._get_data_4th_script(scripts))
        return data

    def _get_script2_index(self, scripts: list):
        index = -1
        for i, script in enumerate(scripts):
            if script['val'].startswith("{\"@context\""):
                index = i
                break

        return index

    def _get_script2_alt_index(self, scripts: list):
        index = -1
        s = "{\"dangerouslySetInnerHTML\""
        s2 = "{\\\"@context\\\""
        for i, script in enumerate(scripts):
            substring_index = script['val'].find(s)
            other = script['val'][substring_index:]
            if substring_index != -1 and s2 in other:
                index = i
                new_script = script['val'][substring_index:]
                new_script = json.loads(new_script, cls=JsonDecoderIgnore)
                new_script = new_script['dangerouslySetInnerHTML']['__html']
                scripts[i]['val'] = new_script
                break

        return index

    def _get_data_2nd_script(self, scripts: list) -> dict:
        index = self._get_script2_index(scripts)

        if index == -1:
            index = self._get_script2_alt_index(scripts)

        if index == -1:
            raise Exception("No script found")

        script = scripts[index]['val']
        data = json.loads(script)

        website = data.get("url", None)
        return {
            "url": website
        }

    def _get_script4_index(self, scripts: list):
        for i, (script) in enumerate(scripts):
            if script['hex_string'] == "4":
                return i

        return -1

    def _get_data_4th_script(self, scripts: list) -> dict:
        result = {}

        index = self._get_script4_index(scripts)

        if index == -1:
            raise Exception("No script found")

        script = scripts[index]['val']

        inner_json = json.loads(script)[3]

        pro_data = inner_json.get("pro", None)
        if pro_data:
            facebookUrl = pro_data.get("facebookUrl", None)
            if facebookUrl:
                result["facebookUrl"] = facebookUrl

            instagramUrl = pro_data.get("instagramUrl", None)
            if instagramUrl:
                result["instagramUrl"] = instagramUrl

            teamMembers = pro_data.get("teamMembers", None)
            if teamMembers:
                teamMembersFiltered = {}
                for member in teamMembers:
                    teamMembersFiltered[member["name"]] = member["title"]
                result["teamMembers"] = teamMembersFiltered

        return result

    def get_scripts(self, soup) -> ResultSet:
        return soup.find_all("script")

    async def get_n_articles_elements(self, n: int, start_page: int = 1):
        useful_data = []
        page = start_page
        is_first = True
        i = 0

        while len(useful_data) < n:
            vendors_json = await self.get_find_miami_vendors(page)
            vendors = vendors_json.get('vendors', [])
            if not vendors:
                break

            for vendor in vendors:
                slug = vendor['slug']
                name = vendor['name']
                phone_number = vendor['phone_number']
                prices = vendor['prices']

                i+=1
                print(slug, i)

                minimum_spend = None
                prices = [x for x in prices if x['minimum_spend_cents'] is not None]
                if prices:
                    min_val = min(prices, key=lambda x: x['minimum_spend_cents'])
                    minimum_spend = min_val['minimum_spend_cents']

                additional_data = {}
                if is_first:
                    data = await self.get_vendor_html(slug)
                    soup = BeautifulSoup(data, "html.parser")
                    scripts = self.get_scripts(soup)

                    data = merge_next_f_scripts(scripts)

                    additional_data = self.get_data_from_scripts(data)
                    is_first = True

                useful_data.append({
                    "slug": slug,
                    "name": name,
                    "phone_number": phone_number,
                    "minimum_spend": minimum_spend,
                    **additional_data
                })

                if len(useful_data) >= n:
                    break

            page += 1

        with open("output.json", "w") as f:
            json.dump(useful_data, f, indent=4)

        return useful_data


