import hashlib

import httpx

from .config import DEFAULT_HEADERS, MAX_RESPONSE_BYTES, REQUEST_TIMEOUT_SECONDS


class CrawlError(Exception):
    """Raised when a URL cannot be fetched as HTML."""


async def fetch_html(url: str) -> tuple[str, str, int, str]:
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)

    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=DEFAULT_HEADERS) as client:
        response = await client.get(url)

    content_type = response.headers.get("content-type", "")
    content = response.content[:MAX_RESPONSE_BYTES]

    if response.status_code >= 400:
        raise CrawlError(f"HTTP {response.status_code} from server")

    looks_like_html = content.lstrip().lower().startswith((b"<!doctype html", b"<html"))
    if "html" not in content_type.lower() and not looks_like_html:
        raise CrawlError(f"Unsupported content type: {content_type or 'unknown'}")

    html = content.decode(response.encoding or "utf-8", errors="replace")
    return html, str(response.url), response.status_code, content_type


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
