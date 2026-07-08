from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class CrawlRequest(BaseModel):
    url: HttpUrl


class Topic(BaseModel):
    topic: str
    score: float
    evidence: List[str] = Field(default_factory=list)


class CrawlResponse(BaseModel):
    url: str
    final_url: Optional[str] = None
    status_code: Optional[int] = None
    title: Optional[str] = None
    description: Optional[str] = None
    canonical_url: Optional[str] = None
    language: Optional[str] = None
    page_type: str = "unknown"
    topics: List[Topic] = Field(default_factory=list)
    headings: Dict[str, List[str]] = Field(default_factory=dict)
    body_excerpt: Optional[str] = None
    content_hash: Optional[str] = None
    fetched_at: datetime


class ParsedPage(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    canonical_url: Optional[str] = None
    language: Optional[str] = None
    headings: Dict[str, List[str]] = Field(default_factory=dict)
    body_text: str = ""
    metadata: Dict[str, str] = Field(default_factory=dict)

