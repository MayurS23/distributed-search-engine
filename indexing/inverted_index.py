"""
indexing/inverted_index.py - Sharded inverted index.
"""
import hashlib
import json
from dataclasses import dataclass
from typing import Optional
from storage.postgres_client import DocumentRepository
from storage.redis_client import RedisStore
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)

@dataclass
class PostingEntry:
    doc_id: str
    tf: int
    positions: list[int]

    def to_dict(self) -> dict:
        return {"doc_id": self.doc_id, "tf": self.tf, "positions": self.positions}

    @classmethod
    def from_dict(cls, d: dict) -> "PostingEntry":
        return cls(doc_id=d["doc_id"], tf=d["tf"], positions=d.get("positions", []))

@dataclass
class IndexStats:
    total_docs: int = 0
    total_tokens: int = 0
    avg_doc_length: float = 0.0
    doc_frequencies: dict = None

    def __post_init__(self):
        if self.doc_frequencies is None:
            self.doc_frequencies = {}

    def to_dict(self) -> dict:
        return {"total_docs": self.total_docs, "total_tokens": self.total_tokens,
                "avg_doc_length": self.avg_doc_length, "doc_frequencies": self.doc_frequencies}

    @classmethod
    def from_dict(cls, d: dict) -> "IndexStats":
        return cls(total_docs=d.get("total_docs",0), total_tokens=d.get("total_tokens",0),
                   avg_doc_length=d.get("avg_doc_length",0.0), doc_frequencies=d.get("doc_frequencies",{}))

def get_shard(term: str, num_shards: Optional[int] = None) -> int:
    n = num_shards or settings.index_num_shards
    digest = hashlib.sha256(term.encode()).digest()
    return int.from_bytes(digest[:4], "big") % n

class IndexShard:
    def __init__(self, shard_id: int) -> None:
        self.shard_id = shard_id
        self._index: dict[str, list[PostingEntry]] = {}
        self._doc_lengths: dict[str, int] = {}

    def add_document(self, doc_id: str, token_count: int, term_freqs: dict, term_positions: dict) -> None:
        self._doc_lengths[doc_id] = token_count
        for term, tf in term_freqs.items():
            if get_shard(term) != self.shard_id:
                continue
            entry = PostingEntry(doc_id=doc_id, tf=tf, positions=sorted(term_positions.get(term,[])))
            if term not in self._index:
                self._index[term] = []
            self._index[term] = [e for e in self._index[term] if e.doc_id != doc_id]
            self._index[term].append(entry)

    def get_postings(self, term: str) -> list[PostingEntry]:
        return self._index.get(term, [])

    def get_doc_frequency(self, term: str) -> int:
        return len(self._index.get(term, []))

    @property
    def term_count(self) -> int:
        return len(self._index)

    @property
    def doc_count(self) -> int:
        return len(self._doc_lengths)

    def iter_terms(self):
        return self._index.items()

class IndexBuilder:
    def __init__(self) -> None:
        self._doc_repo = DocumentRepository()
        self._redis = RedisStore()
        self._num_shards = settings.index_num_shards
        self._shards = {i: IndexShard(shard_id=i) for i in range(self._num_shards)}
        self._stats = IndexStats()

    async def build(self, batch_size: int = 500) -> IndexStats:
        log.info("index_build_started", num_shards=self._num_shards)
        total_processed, offset, total_token_sum = 0, 0, 0
        while True:
            docs = await self._doc_repo.get_processed_documents(offset=offset, limit=batch_size)
            if not docs:
                break
            for doc in docs:
                await self._index_document(doc)
                total_token_sum += doc.get("token_count", 0)
            total_processed += len(docs)
            offset += len(docs)
            if len(docs) < batch_size:
                break
        if total_processed == 0:
            log.warning("index_build_no_documents")
            return self._stats
        self._stats.total_docs = total_processed
        self._stats.total_tokens = total_token_sum
        self._stats.avg_doc_length = total_token_sum / total_processed
        for shard in self._shards.values():
            for term, postings in shard.iter_terms():
                df = len(postings)
                self._stats.doc_frequencies[term] = self._stats.doc_frequencies.get(term, 0) + df
        await self._flush_to_redis()
        log.info("index_build_complete", total_docs=self._stats.total_docs, unique_terms=len(self._stats.doc_frequencies))
        return self._stats

    async def _index_document(self, doc: dict) -> None:
        tokens = doc.get("tokens")
        if not tokens:
            return
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        term_freqs, term_positions = {}, {}
        for entry in tokens:
            if isinstance(entry, dict):
                term = entry.get("term","")
                term_freqs[term] = entry.get("tf", 1)
                term_positions[term] = entry.get("positions", [])
        for shard in self._shards.values():
            shard.add_document(doc["doc_id"], doc.get("token_count",0), term_freqs, term_positions)

    async def _flush_to_redis(self) -> None:
        for shard_id, shard in self._shards.items():
            for term, postings in shard.iter_terms():
                await self._redis.set_postings(shard=shard_id, term=term, postings=[e.to_dict() for e in postings])
        await self._redis.set_index_stats(self._stats.to_dict())

class IndexReader:
    def __init__(self) -> None:
        self._redis = RedisStore()
        self._cached_stats: Optional[IndexStats] = None

    async def get_postings(self, term: str) -> list[PostingEntry]:
        shard = get_shard(term)
        raw = await self._redis.get_postings(shard=shard, term=term)
        return [PostingEntry.from_dict(e) for e in raw]

    async def get_postings_multi(self, terms: list[str]) -> dict[str, list[PostingEntry]]:
        import asyncio
        tasks = {term: asyncio.create_task(self.get_postings(term)) for term in terms}
        return {term: await task for term, task in tasks.items()}

    async def get_stats(self) -> IndexStats:
        if self._cached_stats is None:
            raw = await self._redis.get_index_stats()
            self._cached_stats = IndexStats.from_dict(raw) if raw else IndexStats()
        return self._cached_stats

    def invalidate_stats_cache(self) -> None:
        self._cached_stats = None
