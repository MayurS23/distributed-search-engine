"""
query/search_engine.py - Full search pipeline: query string -> ranked results.
"""
import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
from indexing.inverted_index import IndexReader, IndexStats, PostingEntry
from ranking.bm25 import BM25Scorer, ScoredDocument
from processing.tokenizer import get_tokenizer
from storage.postgres_client import DocumentRepository
from storage.redis_client import RedisStore
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)

class QueryType(Enum):
    SINGLE_TERM = auto()
    MULTI_TERM_AND = auto()
    MULTI_TERM_OR = auto()
    PHRASE = auto()

@dataclass
class ParsedQuery:
    raw: str
    terms: list[str]
    negative_terms: list[str]
    phrase: Optional[str]
    query_type: QueryType
    is_phrase: bool = False

@dataclass
class SearchResult:
    doc_id: str
    url: str
    title: str
    snippet: str
    score: float
    matched_terms: list[str]
    rank: int

@dataclass
class SearchResponse:
    query: str
    results: list[SearchResult]
    total_found: int
    page: int
    page_size: int
    total_pages: int
    query_time_ms: float
    from_cache: bool = False

    @property
    def has_results(self) -> bool:
        return len(self.results) > 0

class QueryParser:
    def __init__(self):
        self._tokenizer = get_tokenizer()

    def parse(self, raw_query: str) -> ParsedQuery:
        if not raw_query or not raw_query.strip():
            return ParsedQuery(raw=raw_query, terms=[], negative_terms=[], phrase=None, query_type=QueryType.SINGLE_TERM)
        raw = raw_query.strip()
        if raw.startswith('"') and raw.endswith('"') and len(raw) > 2:
            phrase_text = raw[1:-1]
            terms = self._tokenizer.tokenize_query(phrase_text)
            return ParsedQuery(raw=raw, terms=terms, negative_terms=[], phrase=phrase_text, query_type=QueryType.PHRASE, is_phrase=True)
        if " OR " in raw:
            parts = [p.strip() for p in raw.split(" OR ")]
            terms = []
            for part in parts:
                terms.extend(self._tokenizer.tokenize_query(part))
            return ParsedQuery(raw=raw, terms=list(dict.fromkeys(terms)), negative_terms=[], phrase=None, query_type=QueryType.MULTI_TERM_OR)
        positive_parts, negative_parts = [], []
        for token in raw.split():
            if token.startswith("-") and len(token) > 1:
                negative_parts.append(token[1:])
            else:
                positive_parts.append(token)
        terms = self._tokenizer.tokenize_query(" ".join(positive_parts))
        negative_terms = self._tokenizer.tokenize_query(" ".join(negative_parts))
        query_type = QueryType.SINGLE_TERM if len(terms) <= 1 else QueryType.MULTI_TERM_AND
        return ParsedQuery(raw=raw, terms=terms, negative_terms=negative_terms, phrase=None, query_type=query_type)

class ResultHydrator:
    def __init__(self):
        self._redis = RedisStore()
        self._doc_repo = DocumentRepository()

    async def hydrate(self, scored_docs: list[ScoredDocument], query_terms: list[str], limit: int) -> list[SearchResult]:
        top_docs = scored_docs[:limit]
        doc_ids = [d.doc_id for d in top_docs]
        metas = await self._redis.mget_doc_meta(doc_ids)
        results = []
        for rank, (scored, meta) in enumerate(zip(top_docs, metas), start=1):
            if meta is None:
                meta = await self._doc_repo.get_document(scored.doc_id) or {}
            url = meta.get("url", "")
            title = meta.get("title", url)
            clean_text = meta.get("clean_text", "")
            snippet = self._generate_snippet(text=clean_text or title, query_terms=query_terms, max_length=200)
            results.append(SearchResult(doc_id=scored.doc_id, url=url, title=title or url,
                                        snippet=snippet, score=scored.score,
                                        matched_terms=scored.matched_terms, rank=rank))
        return results

    @staticmethod
    def _generate_snippet(text: str, query_terms: list[str], max_length: int = 200) -> str:
        if not text:
            return ""
        words = text.split()
        if len(words) <= max_length // 5:
            return text[:max_length]
        window_size = 30
        best_start, best_hits = 0, 0
        for i in range(len(words) - window_size):
            window = " ".join(words[i:i+window_size]).lower()
            hits = sum(1 for term in query_terms if term in window)
            if hits > best_hits:
                best_hits, best_start = hits, i
        snippet = " ".join(words[best_start:best_start+window_size])
        if best_start > 0:
            snippet = "..." + snippet
        if best_start + window_size < len(words):
            snippet += "..."
        return snippet[:max_length]

def check_phrase_match(terms: list[str], postings_map: dict) -> set[str]:
    if not terms:
        return set()
    if len(terms) == 1:
        return {e.doc_id for e in postings_map.get(terms[0], [])}
    candidate_docs = {e.doc_id for e in postings_map.get(terms[0], [])}
    for term in terms[1:]:
        candidate_docs &= {e.doc_id for e in postings_map.get(term, [])}
    matched_docs = set()
    for doc_id in candidate_docs:
        positions_by_term = {}
        for term in terms:
            for entry in postings_map.get(term, []):
                if entry.doc_id == doc_id:
                    positions_by_term[term] = set(entry.positions)
                    break
        for start_pos in positions_by_term.get(terms[0], set()):
            if all((start_pos+i) in positions_by_term.get(terms[i], set()) for i in range(1, len(terms))):
                matched_docs.add(doc_id)
                break
    return matched_docs

class SearchEngine:
    def __init__(self) -> None:
        self._query_parser = QueryParser()
        self._index_reader = IndexReader()
        self._scorer = BM25Scorer()
        self._hydrator = ResultHydrator()
        self._doc_repo = DocumentRepository()
        self._cache = RedisStore()

    async def search(self, query: str, page: int = 1, page_size: int = 10, use_cache: bool = True) -> SearchResponse:
        start_time = time.perf_counter()
        page_size = min(page_size, settings.query_default_page_size)
        page = max(1, page)
        if use_cache:
            cache_key = self._make_cache_key(query, page, page_size)
            cached = await self._cache.cache_get(cache_key)
            if cached is not None:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                response = SearchResponse(**cached)
                response.from_cache = True
                response.query_time_ms = elapsed_ms
                return response
        parsed = self._query_parser.parse(query)
        if not parsed.terms:
            return self._empty_response(query, page, page_size, start_time)
        all_terms = list(set(parsed.terms + parsed.negative_terms))
        postings_map = await self._index_reader.get_postings_multi(all_terms)
        if not any(postings_map.values()):
            return self._empty_response(query, page, page_size, start_time)
        phrase_matches = None
        if parsed.is_phrase and parsed.terms:
            phrase_matches = check_phrase_match(parsed.terms, postings_map)
            if not phrase_matches:
                return self._empty_response(query, page, page_size, start_time)
        if parsed.query_type == QueryType.MULTI_TERM_AND and len(parsed.terms) > 1:
            postings_map = self._filter_and(parsed.terms, postings_map)
        if parsed.negative_terms:
            postings_map = self._apply_negation(parsed.terms, parsed.negative_terms, postings_map)
        stats = await self._index_reader.get_stats()
        doc_lengths = await self._fetch_doc_lengths(postings_map, parsed.terms)
        scored = self._scorer.score_documents(query_terms=parsed.terms, postings_map=postings_map,
                                              stats=stats, doc_lengths=doc_lengths)
        if phrase_matches is not None:
            scored = [s for s in scored if s.doc_id in phrase_matches]
        total_found = len(scored)
        start_idx = (page - 1) * page_size
        results = await self._hydrator.hydrate(scored_docs=scored[start_idx:start_idx+page_size],
                                               query_terms=parsed.terms, limit=page_size)
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        total_pages = max(1, (total_found + page_size - 1) // page_size)
        response = SearchResponse(query=query, results=results, total_found=total_found,
                                  page=page, page_size=page_size, total_pages=total_pages,
                                  query_time_ms=round(elapsed_ms, 2), from_cache=False)
        if use_cache and results:
            await self._cache.cache_set(key=self._make_cache_key(query, page, page_size),
                                        value=self._response_to_cache_dict(response), ttl=settings.cache_ttl_seconds)
        return response

    def _filter_and(self, terms, postings_map):
        doc_sets = [{e.doc_id for e in postings_map.get(t,[])} for t in terms if postings_map.get(t)]
        if not doc_sets:
            return postings_map
        intersection = doc_sets[0]
        for ds in doc_sets[1:]:
            intersection &= ds
        return {term: [e for e in postings_map.get(term,[]) if e.doc_id in intersection] for term in terms}

    def _apply_negation(self, positive_terms, negative_terms, postings_map):
        excluded = {e.doc_id for neg in negative_terms for e in postings_map.get(neg,[])}
        return {term: [e for e in postings_map.get(term,[]) if e.doc_id not in excluded]
                for term in positive_terms}

    async def _fetch_doc_lengths(self, postings_map, terms):
        doc_ids = list({e.doc_id for term in terms for e in postings_map.get(term,[])})
        metas = await self._cache.mget_doc_meta(doc_ids)
        return {doc_id: (meta.get("token_count",0) if meta else 0) for doc_id, meta in zip(doc_ids, metas)}

    @staticmethod
    def _make_cache_key(query, page, page_size):
        return hashlib.sha256(f"{query.lower().strip()}|p{page}|s{page_size}".encode()).hexdigest()[:16]

    @staticmethod
    def _response_to_cache_dict(response):
        return {"query": response.query,
                "results": [{"doc_id":r.doc_id,"url":r.url,"title":r.title,"snippet":r.snippet,
                              "score":r.score,"matched_terms":r.matched_terms,"rank":r.rank} for r in response.results],
                "total_found": response.total_found, "page": response.page,
                "page_size": response.page_size, "total_pages": response.total_pages,
                "query_time_ms": response.query_time_ms, "from_cache": False}

    @staticmethod
    def _empty_response(query, page, page_size, start_time):
        return SearchResponse(query=query, results=[], total_found=0, page=page,
                              page_size=page_size, total_pages=0,
                              query_time_ms=round((time.perf_counter()-start_time)*1000, 2))
