import asyncio
import sys
import json
import logging
import os

import enrich
from core.partyslate import PartySlateClient, PartySlateClientConfig
from core.http import HTTPClient, HTTPOptions
from compose import run as compose

if not os.path.exists("./output/"):
    os.makedirs("./output/")

log_client = logging.getLogger("partyslate.client")
log_parser = logging.getLogger("partyslate.parser")
log_random = logging.getLogger("randomlink.client")

for logger in (log_client, log_parser, log_random):
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def main():
    opts = HTTPOptions(timeout=20)
    async with HTTPClient(opts=opts) as http:
        client = PartySlateClient(
            http=http,
            config=PartySlateClientConfig(default_location="miami", default_category="event-vendors", concurrency=10),
        )
        vendors = await client.collect_vendors(n=5, start_page=1, fetch_additional_for_each=True)
        print(f"Collected {len(vendors)} vendors")
        if vendors:
            with open("./output/data.json", "w", encoding="utf-8") as f:
                json.dump(vendors, f, ensure_ascii=False, indent=2)

            compose(vendors, output_csv="./output/output.csv")

            # Теперь асинхронно обогащаем CSV — enrich.process_csv теперь async
            await enrich.process_csv(input_csv="./output/output.csv", output_csv="./output/output_enriched.csv",
                                     tokens_path="./tokens", cache_dir="./cache", concurrency=5)

        print("Done")


if __name__ == "__main__":
    asyncio.run(main())
