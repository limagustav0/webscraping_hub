import asyncio
from crawl4ai import AsyncWebCrawler

async def main():
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun("https://www.epocacosmeticos.com.br/pesquisa?q=4064666318356")
        print(result.markdown[:300])  # Print first 300 chars

if __name__ == "__main__":
    asyncio.run(main())
