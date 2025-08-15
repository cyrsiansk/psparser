from dataclasses import dataclass, field
from typing import List

@dataclass
class RandomLinkClientConfig:
    concurrency: int = 5
    user_agent: str = "randomlink-client/1.0 (+https://example.com)"
    request_timeout: int = 15
    exclude_internal: bool = True
    exclude_patterns: List[str] = field(default_factory=lambda: [
        "contact", "privacy", "terms", "about", "wp-login", "admin", "signup", "login"
    ])

