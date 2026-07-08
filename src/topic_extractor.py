import re
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Tuple

from .models import ParsedPage, Topic

WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9-]{1,}")

STOP_WORDS = {
    "the", "and", "for", "that", "this", "with", "from", "into", "your", "you",
    "of", "to", "in", "on", "at", "by", "as", "is", "be", "am", "or", "if",
    "do", "no", "so", "up",
    "are", "was", "were", "have", "has", "had", "not", "but", "can", "will",
    "about", "after", "before", "between", "through", "while", "what", "when",
    "where", "which", "their", "there", "than", "then", "also", "more", "most",
    "our", "out", "all", "how", "why", "its", "it's", "they", "them", "his",
    "her", "she", "him", "who", "use", "using", "used", "new", "one", "two",
    "says", "said", "cnn", "feedback", "video", "content", "loaded", "load",
    "submit", "close", "account", "sign", "edition", "watch", "listen", "ad",
    "ads", "relevant", "technical", "issues", "issue", "previously", "cancel",
    "now", "amazon", "product", "item", "items", "customers", "viewed", "price",
    "buy", "free", "returns", "return", "brand", "brands",
    "business", "rei", "co-op", "coop", "uncommon", "path", "publication",
}


def _tokens(text: str) -> List[str]:
    return [word.lower() for word in WORD_RE.findall(text or "") if word.lower() not in STOP_WORDS]


def _bigrams(tokens: List[str]) -> Iterable[str]:
    for index in range(len(tokens) - 1):
        yield f"{tokens[index]} {tokens[index + 1]}"


def extract_topics(page: ParsedPage, limit: int = 8) -> List[Topic]:
    weighted_counts: Dict[str, float] = defaultdict(float)
    evidence: Dict[str, set[str]] = defaultdict(set)

    fields: List[Tuple[str, str, float]] = [
        ("title", page.title or "", 5.0),
        ("description", page.description or "", 3.0),
        ("body", page.body_text, 1.0),
    ]

    for heading_level, headings in page.headings.items():
        fields.append((heading_level, " ".join(headings), 4.0 if heading_level == "h1" else 2.0))

    for field_name, text, weight in fields:
        tokens = _tokens(text)
        counts = Counter(tokens)
        counts.update(_bigrams(tokens))
        for term, count in counts.items():
            weighted_counts[term] += count * weight
            evidence[term].add(field_name)

    if not weighted_counts:
        return []

    max_score = max(weighted_counts.values())
    topics = []
    for term, score in sorted(weighted_counts.items(), key=lambda item: item[1], reverse=True):
        if len(term) < 3:
            continue
        topics.append(
            Topic(
                topic=term,
                score=round(score / max_score, 4),
                evidence=sorted(evidence[term]),
            )
        )
        if len(topics) >= limit:
            break

    return topics
