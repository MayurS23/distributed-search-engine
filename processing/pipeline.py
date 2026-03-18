"""
processing/pipeline.py - Document processing pipeline.
"""
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from processing.parser import HTMLParser
from processing.tokenizer import get_tokenizer
from storage.postgres_client import DocumentRepository
from utils.logger import get_logger

log = get_logger(__name__)

@dataclass
class ProcessingResult:
    doc_id: str
    url: str
    success: bool
    token_count: int = 0
    unique_terms: int = 0
    error: Optional[str] = None

class DocumentProcessor:
    def __init__(self) -> None:
        self._parser = HTMLParser()
        self._tokenizer = get_tokenizer()
        self._repo = DocumentRepository()

    async def process_batch(self, batch_size: int = 100) -> list[ProcessingResult]:
        docs = await self._repo.get_unprocessed_documents(limit=batch_size)
        if not docs:
            return []
        log.info("processing_batch", count=len(docs))
        results = await asyncio.gather(*[self._process_one(doc) for doc in docs])
        return list(results)

    async def _process_one(self, doc: dict) -> ProcessingResult:
        doc_id, url, html = doc["doc_id"], doc["url"], doc.get("raw_html","")
        try:
            parsed = self._parser.parse(doc_id=doc_id, url=url, html=html)
            if parsed is None:
                return ProcessingResult(doc_id=doc_id, url=url, success=False, error="parse_failed")
            stream = self._tokenizer.tokenize(parsed.full_text)
            term_freqs = stream.term_frequencies()
            term_positions = stream.term_positions()
            token_payload = [{"term": t, "tf": f, "positions": term_positions.get(t,[])} for t,f in term_freqs.items()]
            await self._repo.upsert_document({
                "doc_id": doc_id, "url": url, "title": parsed.title,
                "raw_html": html, "clean_text": parsed.body_text,
                "tokens": token_payload, "token_count": stream.token_count,
                "depth": doc.get("depth", 0), "domain": doc.get("domain",""),
                "status": "processed", "processed_at": datetime.utcnow(),
                "crawled_at": doc.get("crawled_at", datetime.utcnow()),
            })
            return ProcessingResult(doc_id=doc_id, url=url, success=True,
                                    token_count=stream.token_count, unique_terms=len(term_freqs))
        except Exception as exc:
            log.error("processing_error", doc_id=doc_id, error=str(exc))
            return ProcessingResult(doc_id=doc_id, url=url, success=False, error=str(exc))
