"""
api/server.py - FastAPI application: HTTP interface for the search engine.
"""
import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from indexing.inverted_index import IndexBuilder
from processing.pipeline import DocumentProcessor
from crawler.crawler import Crawler
from query.search_engine import SearchEngine, SearchResponse
from storage.postgres_client import DocumentRepository, close_engine, init_db
from storage.redis_client import RedisStore, close_pool
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("application_starting", env=settings.app_env)
    try:
        await init_db()
    except Exception as exc:
        log.warning("database_init_warning", error=str(exc))
    yield
    await close_engine()
    await close_pool()

app = FastAPI(title="Distributed Search Engine",
              description="Production-grade distributed search engine with BM25 ranking",
              version="1.0.0", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    start = time.perf_counter()
    request.state.request_id = request_id
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    log.info("http_request", method=request.method, path=request.url.path,
             status=response.status_code, ms=round(elapsed_ms, 2))
    response.headers["X-Request-ID"] = request_id
    return response

class SearchResultModel(BaseModel):
    rank: int
    doc_id: str
    url: str
    title: str
    snippet: str
    score: float
    matched_terms: list[str]

class SearchResponseModel(BaseModel):
    query: str
    results: list[SearchResultModel]
    total_found: int
    page: int
    page_size: int
    total_pages: int
    query_time_ms: float
    from_cache: bool

class CrawlRequest(BaseModel):
    seed_urls: list[str] = Field(..., min_length=1, max_length=20)
    max_pages: Optional[int] = Field(default=None, ge=1, le=10000)

    @field_validator("seed_urls")
    @classmethod
    def validate_urls(cls, v):
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL: {url}")
        return v

@app.get("/search", response_model=SearchResponseModel, tags=["Search"])
async def search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=500),
    page: int = Query(default=1, ge=1, le=1000),
    page_size: int = Query(default=10, ge=1, le=50),
    cache: bool = Query(default=True),
):
    """Search documents. Supports simple, phrase (quotes), OR, and negation (-term) queries."""
    if not q.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    try:
        engine = SearchEngine()
        response = await engine.search(query=q, page=page, page_size=page_size, use_cache=cache)
    except Exception as exc:
        log.error("search_error", query=q, error=str(exc))
        raise HTTPException(status_code=500, detail="Search service temporarily unavailable")
    return SearchResponseModel(
        query=response.query,
        results=[SearchResultModel(rank=r.rank, doc_id=r.doc_id, url=r.url, title=r.title,
                                   snippet=r.snippet, score=r.score, matched_terms=r.matched_terms)
                 for r in response.results],
        total_found=response.total_found, page=response.page, page_size=response.page_size,
        total_pages=response.total_pages, query_time_ms=response.query_time_ms, from_cache=response.from_cache,
    )

@app.get("/health", tags=["System"])
async def health_check():
    redis_store = RedisStore()
    doc_repo = DocumentRepository()
    redis_ok = await redis_store.ping()
    postgres_ok = await doc_repo.health_check()
    overall = "healthy" if (redis_ok and postgres_ok) else "degraded"
    return JSONResponse(status_code=200 if overall == "healthy" else 503,
                        content={"status": overall, "redis": redis_ok, "postgres": postgres_ok, "version": "1.0.0"})

@app.get("/metrics", tags=["System"])
async def get_metrics():
    redis_store = RedisStore()
    doc_repo = DocumentRepository()
    stats = await redis_store.get_index_stats()
    return {
        "corpus": {"total_documents": await doc_repo.get_document_count(),
                   "processed_documents": await doc_repo.get_processed_count(),
                   "indexed_terms": len(stats.get("doc_frequencies", {})),
                   "avg_doc_length": round(stats.get("avg_doc_length", 0), 2)},
        "crawler": {"frontier_size": await redis_store.frontier_size(),
                    "seen_urls": await redis_store.seen_count()},
        "index": {"num_shards": settings.index_num_shards, "total_docs_indexed": stats.get("total_docs", 0)},
    }

@app.post("/admin/crawl", tags=["Admin"])
async def trigger_crawl(crawl_request: CrawlRequest):
    async def _run_crawl():
        try:
            crawler = Crawler()
            stats = await crawler.run(seed_urls=crawl_request.seed_urls)
            processor = DocumentProcessor()
            await processor.process_batch(batch_size=200)
            log.info("crawl_job_complete", crawled=stats.pages_crawled)
        except Exception as exc:
            log.error("crawl_job_error", error=str(exc))
    asyncio.create_task(_run_crawl())
    return {"status": "started", "seed_urls": crawl_request.seed_urls,
            "message": "Crawl job started. Check /metrics for progress."}

@app.post("/admin/index/rebuild", tags=["Admin"])
async def rebuild_index():
    async def _rebuild():
        try:
            builder = IndexBuilder()
            stats = await builder.build()
            log.info("index_rebuild_complete", total_docs=stats.total_docs)
        except Exception as exc:
            log.error("index_rebuild_error", error=str(exc))
    asyncio.create_task(_rebuild())
    return {"status": "started", "message": "Index rebuild started in background."}

@app.delete("/admin/cache", tags=["Admin"])
async def flush_cache():
    from storage.redis_client import get_client
    client = get_client()
    keys = [key async for key in client.scan_iter("cache:*")]
    if keys:
        await client.delete(*keys)
    return {"status": "flushed", "keys_deleted": len(keys)}

@app.get("/admin/stats", tags=["Admin"])
async def admin_stats():
    redis_store = RedisStore()
    doc_repo = DocumentRepository()
    index_stats = await redis_store.get_index_stats()
    return {
        "documents": {"total": await doc_repo.get_document_count(),
                      "processed": await doc_repo.get_processed_count(),
                      "avg_token_count": round(await doc_repo.get_avg_token_count(), 2)},
        "index": {"shards": settings.index_num_shards,
                  "unique_terms": len(index_stats.get("doc_frequencies", {})),
                  "avg_doc_length": round(index_stats.get("avg_doc_length", 0), 2)},
        "ranking": {"algorithm": "BM25", "k1": settings.bm25_k1, "b": settings.bm25_b},
    }

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    log.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(status_code=500, content={"error": "Internal server error"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.server:app", host=settings.app_host, port=settings.app_port, reload=True)
