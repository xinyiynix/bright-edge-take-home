from pathlib import Path

from src.parser import parse_html


def test_parse_html_extracts_metadata():
    html = Path("tests/fixtures/sample_page.html").read_text(encoding="utf-8")
    page = parse_html(html)

    assert page.title == "Lightweight Camping Guide for New Hikers"
    assert "camping gear" in page.description.lower()
    assert page.canonical_url == "https://example.com/camping-guide"
    assert page.language == "en"
    assert page.headings["h1"] == ["Camping Guide for New Hikers"]

