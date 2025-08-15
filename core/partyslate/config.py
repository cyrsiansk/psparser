from dataclasses import dataclass

@dataclass
class PartySlateClientConfig:
    find_vendors_url: str = "https://www.partyslate.com/api/find-vendors.json"
    vendor_url_base: str = "https://www.partyslate.com/vendors/"
    marker_chunk_substring: str = "/_next/static/chunks/webpack-88b0373b6b6bc080.js"
    concurrency: int = 5
    default_location: str | None = "miami"
    default_category: str = "planner"
