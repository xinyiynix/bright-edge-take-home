import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from src.classifier import classify_page
from src.crawler import content_hash, fetch_html
from src.models import CrawlResponse
from src.parser import parse_html
from src.topic_extractor import extract_topics


SAMPLES = {
    "amazon": "http://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C/ref=sr_1_1?s=kitchen&ie=UTF8&qid=1431620315&sr=1-1&keywords=toaster",
    "rei": "http://blog.rei.com/camp/how-to-introduce-your-indoorsy-friend-to-the-outdoors/",
    "cnn": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai",
}


async def crawl_to_dict(url: str) -> dict:
    try:
        html, final_url, status_code, content_type = await fetch_html(url)
        parsed = parse_html(html)
        response = CrawlResponse(
            url=url,
            final_url=final_url,
            status_code=status_code,
            title=parsed.title,
            description=parsed.description,
            canonical_url=parsed.canonical_url,
            language=parsed.language,
            page_type=classify_page(final_url, parsed),
            topics=extract_topics(parsed),
            headings=parsed.headings,
            body_excerpt=parsed.body_text[:1000],
            content_hash=content_hash(parsed.body_text),
            fetched_at=datetime.now(timezone.utc),
        )
        data = response.model_dump(mode="json")
        data["content_type"] = content_type
        return data
    except Exception as exc:
        return {
            "url": url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }


async def main() -> None:
    output_dir = Path("sample_outputs")
    output_dir.mkdir(exist_ok=True)

    for name, url in SAMPLES.items():
        data = await crawl_to_dict(url)
        output_path = output_dir / f"{name}.json"
        output_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"wrote {output_path}")


if __name__ == "__main__":
    asyncio.run(main())

