from urllib.parse import urlparse

from .models import ParsedPage


def classify_page(url: str, page: ParsedPage) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()
    metadata_text = " ".join(f"{k} {v}" for k, v in page.metadata.items()).lower()
    title = (page.title or "").lower()

    if any(marker in path for marker in ["/dp/", "/product/", "/shop/", "/p/"]):
        return "product"

    if any(marker in metadata_text for marker in ["product", "price", "availability"]):
        return "product"

    if any(marker in path for marker in ["/blog/", "/article/", "/camp/"]):
        return "article"

    if any(marker in parsed.netloc.lower() for marker in ["cnn", "news"]) or "/news/" in path:
        return "news"

    if any(word in title for word in ["how to", "guide", "tips"]):
        return "article"

    return "unknown"

