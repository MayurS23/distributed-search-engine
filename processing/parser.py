"""
processing/parser.py - HTML to structured text extraction.
"""
import re
from dataclasses import dataclass, field
from typing import Optional
from bs4 import BeautifulSoup, Tag
from utils.logger import get_logger

log = get_logger(__name__)
_NOISE_TAGS = {"script","style","noscript","iframe","object","embed","nav","footer","header","aside","form","button","svg","canvas"}
_WHITESPACE_RE = re.compile(r"\s+")

@dataclass
class ParsedDocument:
    doc_id: str
    url: str
    title: str = ""
    meta_description: str = ""
    headings: list[str] = field(default_factory=list)
    body_text: str = ""
    anchor_texts: list[str] = field(default_factory=list)
    language: str = "en"

    @property
    def full_text(self) -> str:
        parts = [(self.title + " ") * 3, " ".join(self.headings) + " ", self.meta_description + " ", self.body_text]
        return _WHITESPACE_RE.sub(" ", " ".join(parts)).strip()

    @property
    def is_empty(self) -> bool:
        return not self.body_text.strip()

class HTMLParser:
    def parse(self, doc_id: str, url: str, html: str) -> Optional[ParsedDocument]:
        if not html or not html.strip():
            log.warning("parser_empty_html", doc_id=doc_id, url=url)
            return None
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            log.error("parser_error", doc_id=doc_id, error=str(exc))
            return None
        for tag in soup.find_all(_NOISE_TAGS):
            tag.decompose()
        doc = ParsedDocument(doc_id=doc_id, url=url)
        title_tag = soup.find("title")
        doc.title = title_tag.string.strip() if title_tag and title_tag.string else ""
        meta = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        if meta and isinstance(meta, Tag):
            content = meta.get("content", "")
            doc.meta_description = content.strip() if isinstance(content, str) else ""
        html_tag = soup.find("html")
        if html_tag and isinstance(html_tag, Tag):
            doc.language = str(html_tag.get("lang", "en"))[:2].lower()
        for level in ("h1", "h2", "h3"):
            for tag in soup.find_all(level):
                t = _WHITESPACE_RE.sub(" ", tag.get_text()).strip()
                if t:
                    doc.headings.append(t)
        container = soup.find("main") or soup.find("article") or soup.find("body")
        if container:
            for tag in container.find_all(["h1","h2","h3"]):
                tag.decompose()
            doc.body_text = _WHITESPACE_RE.sub(" ", container.get_text(separator=" ")).strip()
        for a_tag in soup.find_all("a", href=True):
            t = _WHITESPACE_RE.sub(" ", a_tag.get_text()).strip()
            if t and 2 < len(t) < 100:
                doc.anchor_texts.append(t)
        if doc.is_empty:
            log.warning("parser_no_body_text", doc_id=doc_id, url=url)
            return None
        return doc
