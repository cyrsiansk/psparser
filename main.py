import asyncio
import sys
from classes import PartySlateProvider

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


psp = PartySlateProvider()


async def main():
    async with psp:
        await psp.get_n_articles_elements(50, start_page=1)


if __name__ == "__main__":
    asyncio.run(main())