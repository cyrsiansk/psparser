from dataclasses import dataclass, field
from typing import Dict

@dataclass
class HTTPOptions:
    timeout: int = 15
    headers: Dict[str, str] = field(default_factory=lambda: {
        "User-Agent": "partyslate-client/1.0 (+https://example.com)"
    })