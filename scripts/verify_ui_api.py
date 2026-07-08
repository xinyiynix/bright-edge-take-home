import asyncio

import httpx

from src.main import app


async def main() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        root = await client.get("/")
        assert root.status_code == 200
        assert "BrightEdge URL Metadata Crawler" in root.text

        health = await client.get("/health")
        assert health.json() == {"status": "ok"}

        crawl = await client.post(
            "/crawl",
            json={"url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"},
        )
        crawl.raise_for_status()
        data = crawl.json()
        assert data["page_type"] == "news"
        assert data["topics"]

    print("UI/API verification passed")


if __name__ == "__main__":
    asyncio.run(main())

