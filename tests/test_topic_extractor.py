from src.parser import parse_html
from src.topic_extractor import extract_topics


def test_extract_topics_prefers_weighted_terms():
    html = """
    <html>
      <head>
        <title>Camping Gear Guide</title>
        <meta name="description" content="Camping gear and hiking safety guide">
      </head>
      <body>
        <h1>Camping Gear</h1>
        <p>Camping gear helps hikers prepare for outdoor hiking trips.</p>
      </body>
    </html>
    """
    page = parse_html(html)
    topics = extract_topics(page, limit=10)
    topic_names = [topic.topic for topic in topics]

    assert "camping" in topic_names
    assert any("gear" in topic for topic in topic_names)
