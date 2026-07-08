import argparse
import asyncio

import httpx

from src.main import app


DEFAULT_URL = "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"


async def verify(url: str) -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        health = await client.get("/health")
        health.raise_for_status()
        assert health.json() == {"status": "ok"}

        response = await client.post("/crawl", json={"url": url})
        response.raise_for_status()
        data = response.json()

    assert data["url"] == url
    assert data["title"]
    assert data["topics"]

    print("API verification passed")
    print(f"URL: {data['url']}")
    print(f"Page type: {data['page_type']}")
    print(f"Title: {data['title']}")
    print("Topics:", ", ".join(topic["topic"] for topic in data["topics"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify the FastAPI crawler API.")
    parser.add_argument("--url", default=DEFAULT_URL, help="URL to crawl during verification.")
    args = parser.parse_args()
    asyncio.run(verify(args.url))


if __name__ == "__main__":
    main()

