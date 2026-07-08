import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from .models import ParsedPage


def _clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def _meta_content(soup: BeautifulSoup, *names: str) -> Optional[str]:
    for name in names:
        tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
        if tag and tag.get("content"):
            return _clean_text(tag.get("content"))
    return None


def parse_html(html: str) -> ParsedPage:
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "form", "button", "input", "footer", "nav"]):
        tag.decompose()

    title = _clean_text(soup.title.string if soup.title else None)
    description = _meta_content(soup, "description", "og:description", "twitter:description")
    language = soup.html.get("lang") if soup.html else None

    canonical = None
    canonical_tag = soup.find("link", rel=lambda value: value and "canonical" in value)
    if canonical_tag and canonical_tag.get("href"):
        canonical = _clean_text(canonical_tag.get("href"))

    headings: Dict[str, List[str]] = {}
    for level in ["h1", "h2", "h3"]:
        values = [_clean_text(tag.get_text(" ")) for tag in soup.find_all(level)]
        headings[level] = [value for value in values if value]

    body_text = _clean_text(soup.get_text(" ")) or ""

    metadata = {}
    for tag in soup.find_all("meta"):
        key = tag.get("name") or tag.get("property")
        value = tag.get("content")
        if key and value:
            metadata[key] = _clean_text(value) or ""

    return ParsedPage(
        title=title,
        description=description,
        canonical_url=canonical,
        language=language,
        headings=headings,
        body_text=body_text,
        metadata=metadata,
    )
