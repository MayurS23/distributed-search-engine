"""
storage/postgres_client.py - Async PostgreSQL client.
"""
import json
from datetime import datetime
from typing import Any, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)
_engine: Optional[AsyncEngine] = None
_session_factory = None

def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.postgres_dsn, pool_size=settings.postgres_pool_size,
            max_overflow=5, pool_pre_ping=True,
        )
    return _engine

def get_session_factory():
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(), expire_on_commit=False, class_=AsyncSession
        )
    return _session_factory

async def close_engine() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None

DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY, url TEXT NOT NULL UNIQUE, title TEXT,
        raw_html TEXT, clean_text TEXT, tokens JSONB, token_count INTEGER DEFAULT 0,
        crawled_at TIMESTAMPTZ DEFAULT NOW(), processed_at TIMESTAMPTZ,
        depth INTEGER DEFAULT 0, domain TEXT, status TEXT DEFAULT 'raw'
    )""",
    "CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url)",
    "CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status)",
    """CREATE TABLE IF NOT EXISTS crawl_jobs (
        job_id TEXT PRIMARY KEY, seed_url TEXT NOT NULL,
        started_at TIMESTAMPTZ DEFAULT NOW(), finished_at TIMESTAMPTZ,
        pages_crawled INTEGER DEFAULT 0, status TEXT DEFAULT 'running'
    )""",
]

async def init_db() -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            await conn.execute(text(stmt))
    log.info("database_initialized")

class DocumentRepository:
    def __init__(self) -> None:
        self._factory = get_session_factory()

    async def upsert_document(self, doc: dict[str, Any]) -> None:
        async with self._factory() as session:
            await session.execute(text("""
                INSERT INTO documents
                    (doc_id, url, title, raw_html, clean_text, tokens, token_count,
                     crawled_at, processed_at, depth, domain, status)
                VALUES
                    (:doc_id, :url, :title, :raw_html, :clean_text, :tokens, :token_count,
                     :crawled_at, :processed_at, :depth, :domain, :status)
                ON CONFLICT (url) DO UPDATE SET
                    title=EXCLUDED.title, clean_text=EXCLUDED.clean_text,
                    tokens=EXCLUDED.tokens, token_count=EXCLUDED.token_count,
                    processed_at=EXCLUDED.processed_at, status=EXCLUDED.status
            """), {
                **doc,
                "tokens": json.dumps(doc.get("tokens", [])),
                "crawled_at": doc.get("crawled_at", datetime.utcnow()),
                "processed_at": doc.get("processed_at"),
            })
            await session.commit()

    async def get_document(self, doc_id: str) -> Optional[dict[str, Any]]:
        async with self._factory() as session:
            result = await session.execute(
                text("SELECT * FROM documents WHERE doc_id = :doc_id"), {"doc_id": doc_id}
            )
            row = result.mappings().fetchone()
            return dict(row) if row else None

    async def get_unprocessed_documents(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self._factory() as session:
            result = await session.execute(
                text("SELECT * FROM documents WHERE status = 'raw' ORDER BY crawled_at ASC LIMIT :limit"),
                {"limit": limit},
            )
            return [dict(row) for row in result.mappings().fetchall()]

    async def get_processed_documents(self, offset: int = 0, limit: int = 1000) -> list[dict[str, Any]]:
        async with self._factory() as session:
            result = await session.execute(
                text("SELECT doc_id, url, title, tokens, token_count, domain FROM documents WHERE status='processed' ORDER BY doc_id OFFSET :offset LIMIT :limit"),
                {"offset": offset, "limit": limit},
            )
            return [dict(row) for row in result.mappings().fetchall()]

    async def get_document_count(self) -> int:
        async with self._factory() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM documents"))
            return result.scalar() or 0

    async def get_processed_count(self) -> int:
        async with self._factory() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM documents WHERE status='processed'"))
            return result.scalar() or 0

    async def get_avg_token_count(self) -> float:
        async with self._factory() as session:
            result = await session.execute(text("SELECT AVG(token_count) FROM documents WHERE status='processed'"))
            val = result.scalar()
            return float(val) if val else 0.0

    async def health_check(self) -> bool:
        try:
            async with self._factory() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
