from dataclasses import dataclass, asdict, field
from typing import Any, Dict, Optional
import json
import re

_WHITESPACE = re.compile(r"[ \t\n\r]*", re.VERBOSE | re.MULTILINE | re.DOTALL)

class LenientJSONDecoder(json.JSONDecoder):
    def decode(self, s: str, _w=_WHITESPACE.match) -> Any:
        obj, end = self.raw_decode(s, idx=_w(s, 0).end())
        return obj

    def get_obj_length(self, s: str, _w=_WHITESPACE.match) -> int:
        _, end = self.raw_decode(s, idx=_w(s, 0).end())
        end = _w(s, end).end()
        return end

_dc = LenientJSONDecoder()


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
    alpha_stop = '"{[n'
    for c in s:
        if c == "T":
            return "T"
        if c in alpha_stop:
            break
        else:
            res += c
    return res


@dataclass
class Vendor:
    slug: str
    name: str
    phone_number: Optional[str] = None
    minimum_spend: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    url_extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_item(cls, item: Dict[str, Any]) -> "Vendor":
        prices = item.get("prices", []) or []
        non_null = [p for p in prices if p.get("minimum_spend_cents") is not None]
        minimum_spend = min((p["minimum_spend_cents"] for p in non_null), default=None) if non_null else None
        return cls(
            slug=item.get("slug", ""),
            name=item.get("name", ""),
            phone_number=item.get("phone_number"),
            minimum_spend=minimum_spend,
            extra={},
            url_extra={},
        )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.update(self.extra or {})
        d.pop("extra", None)
        return d
