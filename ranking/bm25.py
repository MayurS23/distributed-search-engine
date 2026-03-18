"""
ranking/bm25.py - BM25 ranking algorithm.
"""
import math
from dataclasses import dataclass
from typing import Optional
from indexing.inverted_index import IndexStats, PostingEntry
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)

@dataclass(frozen=True)
class ScoredDocument:
    doc_id: str
    score: float
    matched_terms: list[str]

    def __lt__(self, other): return self.score < other.score
    def __gt__(self, other): return self.score > other.score

class BM25Scorer:
    def __init__(self, k1: Optional[float] = None, b: Optional[float] = None) -> None:
        self.k1 = k1 if k1 is not None else settings.bm25_k1
        self.b = b if b is not None else settings.bm25_b

    def idf(self, df: int, total_docs: int) -> float:
        if total_docs == 0 or df == 0:
            return 0.0
        return math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)

    def tf_norm(self, tf: int, doc_length: int, avg_doc_length: float) -> float:
        if avg_doc_length == 0:
            return 0.0
        k1, b = self.k1, self.b
        normaliser = k1 * (1.0 - b + b * (doc_length / avg_doc_length))
        return (tf * (k1 + 1.0)) / (tf + normaliser)

    def score_documents(self, query_terms: list[str], postings_map: dict,
                        stats: IndexStats, doc_lengths: dict) -> list[ScoredDocument]:
        if not query_terms or stats.total_docs == 0:
            return []
        score_accumulator: dict[str, float] = {}
        matched_terms_map: dict[str, list[str]] = {}
        for term in query_terms:
            postings = postings_map.get(term, [])
            if not postings:
                continue
            df = stats.doc_frequencies.get(term, len(postings))
            term_idf = self.idf(df=df, total_docs=stats.total_docs)
            if term_idf <= 0:
                continue
            for entry in postings:
                doc_id = entry.doc_id
                doc_len = doc_lengths.get(doc_id, int(stats.avg_doc_length))
                tf_n = self.tf_norm(tf=entry.tf, doc_length=doc_len, avg_doc_length=stats.avg_doc_length)
                score_accumulator[doc_id] = score_accumulator.get(doc_id, 0.0) + term_idf * tf_n
                matched_terms_map.setdefault(doc_id, [])
                if term not in matched_terms_map[doc_id]:
                    matched_terms_map[doc_id].append(term)
        if not score_accumulator:
            return []
        results = [ScoredDocument(doc_id=d, score=round(s,6), matched_terms=matched_terms_map.get(d,[]))
                   for d, s in score_accumulator.items()]
        results.sort(key=lambda x: x.score, reverse=True)
        return results
