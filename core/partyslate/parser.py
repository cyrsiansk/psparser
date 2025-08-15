import re
import json
import logging
from typing import Sequence, List, Dict, Any, Optional
from bs4.element import Tag
from .models import get_next_hex_string, get_next_data_type_string, _dc


log = logging.getLogger("partyslate.parser")
_PUSH_RE = re.compile(r'self\.__next_f\.push\(\s*(\[[\s\S]*?])\s*\)', re.MULTILINE)


def merge_next_f_scripts(script_tags: Sequence[Tag],
                         marker_src_substring: str = "/_next/static/chunks/webpack-88b0373b6b6bc080.js",
                         push_re: re.Pattern = _PUSH_RE
                         ) -> List[Dict[str, Any]]:
    marker_index: Optional[int] = None
    for i, tag in enumerate(script_tags):
        src = tag.get("src") or ""
        if marker_src_substring in src:
            marker_index = i
            break

    if marker_index is None:
        log.debug("Marker script not found (marker=%s)", marker_src_substring)
        return []

    raw_items: List[str] = []
    for tag in script_tags[marker_index + 1:]:
        text = tag.string
        if text is None:
            text = tag.get_text(separator="\n")

        for m in push_re.finditer(text):
            raw = m.group(1).strip()
            raw_items.append(raw)

    parsed_items: List[Any] = []
    for raw in raw_items:
        parsed_items.append(json.loads(raw))

    combined = ""
    for tp, data in parsed_items:
        if tp == 1:
            combined += data

    data: List[Dict[str, Any]] = []
    other = combined

    dc = _dc

    while True:
        entry: Dict[str, Any] = {}

        hex_string = get_next_hex_string(other)
        if not hex_string:
            log.debug("No hex string found, stopping.")
            break

        other = other.split(hex_string, 1)[1][1:]

        data_type = get_next_data_type_string(other)
        obj_length: Optional[int] = None

        if data_type:
            if data_type == "T":
                length_hex = get_next_hex_string(other[1:])
                if not length_hex:
                    log.debug("No length hex found after data_type T; stopping.")
                    break
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
            except Exception:
                pass

        val = other[:obj_length]
        backup_other = other
        other = other[obj_length:]

        if data_type == "T":
            checks = [":{\"__typename", "[\"$\",\"$L1f\","]
            for c in checks:
                if c in val + other[:20]:
                    index_start = backup_other.find(c) - 2
                    if index_start < 0:
                        index_start = 0
                    other = backup_other[index_start:]
                    break

        entry["hex_string"] = hex_string
        entry["data_type"] = data_type
        entry["obj_length"] = obj_length
        entry["val"] = val

        data.append(entry)

        if len(other) == 0:
            break

    return data
