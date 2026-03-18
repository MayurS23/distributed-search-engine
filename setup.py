import os

files = {}

files[".env"] = """APP_ENV=development
APP_HOST=0.0.0.0
APP_PORT=8000
APP_WORKERS=4
LOG_LEVEL=INFO
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=
REDIS_MAX_CONNECTIONS=20
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=search_engine
POSTGRES_USER=search_user
POSTGRES_PASSWORD=search_pass
POSTGRES_POOL_SIZE=10
CRAWLER_MAX_DEPTH=3
CRAWLER_MAX_PAGES=500
CRAWLER_CONCURRENCY=10
CRAWLER_DELAY_SECONDS=1.0
CRAWLER_REQUEST_TIMEOUT=30
CRAWLER_USER_AGENT=DistributedSearchBot/1.0
INDEX_NUM_SHARDS=4
INDEX_SHARD_PREFIX=shard
BM25_K1=1.5
BM25_B=0.75
QUERY_MAX_RESULTS=100
QUERY_DEFAULT_PAGE_SIZE=10
CACHE_TTL_SECONDS=300
CACHE_MAX_SIZE=1000
"""

files["requirements.txt"] = """fastapi==0.111.0
uvicorn[standard]==0.29.0
pydantic==2.7.1
pydantic-settings==2.2.1
aiohttp==3.9.5
aiofiles==23.2.1
beautifulsoup4==4.12.3
lxml==5.2.1
nltk==3.8.1
regex==2024.4.28
redis==5.0.4
asyncpg==0.29.0
psycopg2-binary==2.9.9
sqlalchemy[asyncio]==2.0.30
cachetools==5.3.3
structlog==24.1.0
python-dotenv==1.0.1
httpx==0.27.0
tenacity==8.3.0
xxhash==3.4.1
pytest==8.2.0
pytest-asyncio==0.23.6
pytest-cov==5.0.0
black==24.4.2
isort==5.13.2
"""

files["pytest.ini"] = """[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
log_cli = true
log_cli_level = WARNING
addopts = 
    -v
    --tb=short
    --no-header
    -p no:warnings
"""

files["utils/config.py"] = '''"""
utils/config.py - Centralised configuration using Pydantic Settings.
"""
from functools import lru_cache
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8",
        case_sensitive=False, extra="ignore",
    )
    app_env: str = Field(default="development")
    app_host: str = Field(default="0.0.0.0")
    app_port: int = Field(default=8000)
    app_workers: int = Field(default=4)
    log_level: str = Field(default="INFO")
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_db: int = Field(default=0)
    redis_password: Optional[str] = Field(default=None)
    redis_max_connections: int = Field(default=20)
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="search_engine")
    postgres_user: str = Field(default="search_user")
    postgres_password: str = Field(default="search_pass")
    postgres_pool_size: int = Field(default=10)
    crawler_max_depth: int = Field(default=3)
    crawler_max_pages: int = Field(default=500)
    crawler_concurrency: int = Field(default=10)
    crawler_delay_seconds: float = Field(default=1.0)
    crawler_request_timeout: int = Field(default=30)
    crawler_user_agent: str = Field(default="DistributedSearchBot/1.0")
    index_num_shards: int = Field(default=4)
    index_shard_prefix: str = Field(default="shard")
    bm25_k1: float = Field(default=1.5)
    bm25_b: float = Field(default=0.75)
    query_max_results: int = Field(default=100)
    query_default_page_size: int = Field(default=10)
    cache_ttl_seconds: int = Field(default=300)
    cache_max_size: int = Field(default=1000)

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def postgres_dsn(self) -> str:
        return (f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

    @property
    def postgres_dsn_sync(self) -> str:
        return (f"postgresql://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()

settings = get_settings()
'''

files["utils/logger.py"] = '''"""
utils/logger.py - Structured JSON logging using structlog.
"""
import logging
import sys
from typing import Any
import structlog
from structlog.types import EventDict, WrappedLogger
from utils.config import settings

def _add_log_level(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    event_dict["level"] = method_name.upper()
    return event_dict

def _order_keys(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    ordered: dict[str, Any] = {}
    for key in ("timestamp", "level", "logger", "event"):
        if key in event_dict:
            ordered[key] = event_dict.pop(key)
    ordered.update(event_dict)
    return ordered

def configure_logging() -> None:
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)
    for noisy in ("asyncio", "aiohttp", "urllib3", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        _add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _order_keys,
    ]
    renderer: Any = structlog.dev.ConsoleRenderer(colors=True) if settings.app_env == "development" else structlog.processors.JSONRenderer()
    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)

configure_logging()
'''

files["storage/redis_client.py"] = '''"""
storage/redis_client.py - Async Redis client with connection pooling.
"""
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)
_pool: Optional[ConnectionPool] = None

def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool.from_url(
            settings.redis_url, max_connections=settings.redis_max_connections,
            decode_responses=True,
        )
    return _pool

def get_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=get_pool())

async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None

class RedisStore:
    def __init__(self) -> None:
        self._client = get_client()

    async def frontier_push(self, url: str) -> None:
        await self._client.rpush("frontier:queue", url)

    async def frontier_pop(self) -> Optional[str]:
        return await self._client.lpop("frontier:queue")

    async def frontier_size(self) -> int:
        return await self._client.llen("frontier:queue")

    async def mark_seen(self, url_hash: str) -> bool:
        return bool(await self._client.sadd("seen:urls", url_hash))

    async def is_seen(self, url_hash: str) -> bool:
        return bool(await self._client.sismember("seen:urls", url_hash))

    async def seen_count(self) -> int:
        return await self._client.scard("seen:urls")

    async def set_postings(self, shard: int, term: str, postings: list[dict]) -> None:
        await self._client.set(f"index:shard{shard}:{term}", json.dumps(postings))

    async def get_postings(self, shard: int, term: str) -> list[dict]:
        raw = await self._client.get(f"index:shard{shard}:{term}")
        return json.loads(raw) if raw else []

    async def set_index_stats(self, stats: dict[str, Any]) -> None:
        await self._client.set("index:stats", json.dumps(stats))

    async def get_index_stats(self) -> dict[str, Any]:
        raw = await self._client.get("index:stats")
        return json.loads(raw) if raw else {}

    async def set_doc_meta(self, doc_id: str, meta: dict[str, Any]) -> None:
        await self._client.set(f"doc:{doc_id}", json.dumps(meta))

    async def get_doc_meta(self, doc_id: str) -> Optional[dict[str, Any]]:
        raw = await self._client.get(f"doc:{doc_id}")
        return json.loads(raw) if raw else None

    async def mget_doc_meta(self, doc_ids: list[str]) -> list[Optional[dict]]:
        if not doc_ids:
            return []
        keys = [f"doc:{doc_id}" for doc_id in doc_ids]
        raws = await self._client.mget(keys)
        return [json.loads(r) if r else None for r in raws]

    async def cache_set(self, key: str, value: Any, ttl: int) -> None:
        await self._client.setex(f"cache:{key}", ttl, json.dumps(value))

    async def cache_get(self, key: str) -> Optional[Any]:
        raw = await self._client.get(f"cache:{key}")
        return json.loads(raw) if raw else None

    async def ping(self) -> bool:
        try:
            return await self._client.ping()
        except Exception:
            return False
'''

files["storage/postgres_client.py"] = '''"""
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
        depth INTEGER DEFAULT 0, domain TEXT, status TEXT DEFAULT \'raw\'
    )""",
    "CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url)",
    "CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status)",
    """CREATE TABLE IF NOT EXISTS crawl_jobs (
        job_id TEXT PRIMARY KEY, seed_url TEXT NOT NULL,
        started_at TIMESTAMPTZ DEFAULT NOW(), finished_at TIMESTAMPTZ,
        pages_crawled INTEGER DEFAULT 0, status TEXT DEFAULT \'running\'
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
                text("SELECT * FROM documents WHERE status = \'raw\' ORDER BY crawled_at ASC LIMIT :limit"),
                {"limit": limit},
            )
            return [dict(row) for row in result.mappings().fetchall()]

    async def get_processed_documents(self, offset: int = 0, limit: int = 1000) -> list[dict[str, Any]]:
        async with self._factory() as session:
            result = await session.execute(
                text("SELECT doc_id, url, title, tokens, token_count, domain FROM documents WHERE status=\'processed\' ORDER BY doc_id OFFSET :offset LIMIT :limit"),
                {"offset": offset, "limit": limit},
            )
            return [dict(row) for row in result.mappings().fetchall()]

    async def get_document_count(self) -> int:
        async with self._factory() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM documents"))
            return result.scalar() or 0

    async def get_processed_count(self) -> int:
        async with self._factory() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM documents WHERE status=\'processed\'"))
            return result.scalar() or 0

    async def get_avg_token_count(self) -> float:
        async with self._factory() as session:
            result = await session.execute(text("SELECT AVG(token_count) FROM documents WHERE status=\'processed\'"))
            val = result.scalar()
            return float(val) if val else 0.0

    async def health_check(self) -> bool:
        try:
            async with self._factory() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
'''

files["crawler/robots.py"] = '''"""
crawler/robots.py - Robots.txt parser with caching.
"""
import asyncio
import time
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import aiohttp
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)
_robots_cache: dict[str, tuple] = {}
_CACHE_TTL = 3600
_lock = asyncio.Lock()

async def _fetch_robots_txt(session: aiohttp.ClientSession, base_url: str) -> Optional[str]:
    robots_url = f"{base_url}/robots.txt"
    try:
        async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
    except Exception:
        pass
    return None

async def get_robots_parser(session: aiohttp.ClientSession, url: str) -> Optional[RobotFileParser]:
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    async with _lock:
        cached = _robots_cache.get(base_url)
        if cached:
            parser, fetched_at = cached
            if time.time() - fetched_at < _CACHE_TTL:
                return parser
        content = await _fetch_robots_txt(session, base_url)
        if content is None:
            _robots_cache[base_url] = (None, time.time())
            return None
        parser = RobotFileParser()
        parser.parse(content.splitlines())
        _robots_cache[base_url] = (parser, time.time())
        return parser

async def can_fetch(session: aiohttp.ClientSession, url: str, user_agent: Optional[str] = None) -> bool:
    agent = user_agent or settings.crawler_user_agent
    parser = await get_robots_parser(session, url)
    if parser is None:
        return True
    return parser.can_fetch(agent, url)
'''

files["crawler/crawler.py"] = '''"""
crawler/crawler.py - Async BFS web crawler.
"""
import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse
import uuid
import aiohttp
from bs4 import BeautifulSoup
from crawler.robots import can_fetch
from storage.postgres_client import DocumentRepository
from storage.redis_client import RedisStore
from utils.config import settings
from utils.logger import get_logger

log = get_logger(__name__)

@dataclass
class CrawlStats:
    pages_crawled: int = 0
    pages_failed: int = 0
    pages_skipped: int = 0
    bytes_downloaded: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def pages_per_second(self) -> float:
        e = self.elapsed_seconds
        return self.pages_crawled / e if e > 0 else 0.0

def hash_url(url: str) -> str:
    return hashlib.sha256(url.strip().lower().encode()).hexdigest()

def normalise_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url.strip())
        if parsed.scheme not in ("http", "https"):
            return None
        return parsed._replace(fragment="", scheme=parsed.scheme.lower()).geturl().rstrip("/")
    except Exception:
        return None

def extract_domain(url: str) -> str:
    return urlparse(url).netloc.lower()

def extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href or href.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        abs_url = urljoin(base_url, href)
        n = normalise_url(abs_url)
        if n:
            links.append(n)
    return list(set(links))

def extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    t = soup.find("title")
    if t and t.string:
        return t.string.strip()
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""

class Crawler:
    def __init__(self) -> None:
        self._store = RedisStore()
        self._doc_repo = DocumentRepository()
        self._stats = CrawlStats()
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._domain_last_fetch: dict[str, float] = {}
        self._shutdown = asyncio.Event()
        self._semaphore = asyncio.Semaphore(settings.crawler_concurrency)

    async def run(self, seed_urls: list[str]) -> CrawlStats:
        log.info("crawl_started", seed_count=len(seed_urls))
        for url in seed_urls:
            n = normalise_url(url)
            if n:
                await self._enqueue(n, 0)
        connector = aiohttp.TCPConnector(limit=settings.crawler_concurrency * 2, ssl=False)
        timeout = aiohttp.ClientTimeout(total=settings.crawler_request_timeout)
        headers = {"User-Agent": settings.crawler_user_agent, "Accept": "text/html"}
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
            workers = [asyncio.create_task(self._worker(session, i)) for i in range(settings.crawler_concurrency)]
            await asyncio.gather(*workers, return_exceptions=True)
        log.info("crawl_finished", pages_crawled=self._stats.pages_crawled, elapsed=round(self._stats.elapsed_seconds, 2))
        return self._stats

    async def _worker(self, session: aiohttp.ClientSession, worker_id: int) -> None:
        consecutive_empty = 0
        while not self._shutdown.is_set():
            if self._stats.pages_crawled >= settings.crawler_max_pages:
                self._shutdown.set()
                break
            url_str = await self._store.frontier_pop()
            if url_str is None:
                consecutive_empty += 1
                if consecutive_empty >= 5:
                    break
                await asyncio.sleep(0.5)
                continue
            consecutive_empty = 0
            try:
                parts = url_str.split("|", 1)
                url, depth = parts[0], int(parts[1]) if len(parts) > 1 else 0
            except Exception:
                continue
            async with self._semaphore:
                await self._apply_rate_limit(url)
                await self._fetch(session, url, depth)

    async def _fetch(self, session: aiohttp.ClientSession, url: str, depth: int) -> None:
        doc_id = str(uuid.uuid4())
        if not await can_fetch(session, url):
            self._stats.pages_skipped += 1
            return
        try:
            async with session.get(url) as response:
                if "text/html" not in response.headers.get("Content-Type", ""):
                    self._stats.pages_skipped += 1
                    return
                html = await response.text(errors="replace")
                self._stats.bytes_downloaded += len(html.encode())
                links = extract_links(html, url) if depth < settings.crawler_max_depth else []
                title = extract_title(html)
                domain = extract_domain(url)
                self._stats.pages_crawled += 1
                log.info("page_fetched", url=url, status=response.status, depth=depth)
                await self._doc_repo.upsert_document({
                    "doc_id": doc_id, "url": url, "title": title,
                    "raw_html": html, "clean_text": None, "tokens": [],
                    "token_count": 0, "crawled_at": datetime.utcnow(),
                    "processed_at": None, "depth": depth,
                    "domain": domain, "status": "raw",
                })
                await self._store.set_doc_meta(doc_id, {"doc_id": doc_id, "url": url, "title": title, "domain": domain, "depth": depth})
                if depth < settings.crawler_max_depth:
                    for link in links:
                        await self._enqueue(link, depth + 1)
        except Exception as exc:
            self._stats.pages_failed += 1
            log.warning("fetch_error", url=url, error=str(exc))

    async def _enqueue(self, url: str, depth: int) -> None:
        url_hash = hash_url(url)
        if await self._store.mark_seen(url_hash):
            await self._store.frontier_push(f"{url}|{depth}")

    async def _apply_rate_limit(self, url: str) -> None:
        domain = extract_domain(url)
        if domain not in self._domain_locks:
            self._domain_locks[domain] = asyncio.Lock()
        async with self._domain_locks[domain]:
            last = self._domain_last_fetch.get(domain, 0.0)
            elapsed = time.time() - last
            if elapsed < settings.crawler_delay_seconds:
                await asyncio.sleep(settings.crawler_delay_seconds - elapsed)
            self._domain_last_fetch[domain] = time.time()
'''

files["processing/parser.py"] = '''"""
processing/parser.py - HTML to structured text extraction.
"""
import re
from dataclasses import dataclass, field
from typing import Optional
from bs4 import BeautifulSoup, Tag
from utils.logger import get_logger

log = get_logger(__name__)
_NOISE_TAGS = {"script","style","noscript","iframe","object","embed","nav","footer","header","aside","form","button","svg","canvas"}
_WHITESPACE_RE = re.compile(r"\\s+")

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
'''

files["processing/tokenizer.py"] = '''"""
processing/tokenizer.py - Text to normalised token stream.
CRITICAL: This same pipeline must be used at both index time and query time.
"""
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional
from utils.logger import get_logger

log = get_logger(__name__)

try:
    from nltk.stem import PorterStemmer as _PorterStemmer
    _stemmer_class = _PorterStemmer
except ImportError:
    class _PorterStemmer:
        def stem(self, word):
            for s in ["ing","tion","ed","er","ly","ness","ment","s"]:
                if word.endswith(s) and len(word)-len(s)>2:
                    return word[:-len(s)]
            return word
    _stemmer_class = _PorterStemmer

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[\'\\-][a-z0-9]+)*")

_STOP_WORDS = frozenset({
    "i","me","my","myself","we","our","ours","ourselves","you","your","yours",
    "yourself","yourselves","he","him","his","himself","she","her","hers",
    "herself","it","its","itself","they","them","their","theirs","themselves",
    "what","which","who","whom","this","that","these","those","am","is","are",
    "was","were","be","been","being","have","has","had","having","do","does",
    "did","doing","a","an","the","and","but","if","or","because","as","until",
    "while","of","at","by","for","with","about","against","between","into",
    "through","during","before","after","above","below","to","from","up","down",
    "in","out","on","off","over","under","again","further","then","once","here",
    "there","when","where","why","how","all","both","each","few","more","most",
    "other","some","such","no","nor","not","only","own","same","so","than","too",
    "very","s","t","can","will","just","don","should","now","d","ll","m","o",
    "re","ve","y","ain","aren","couldn","didn","doesn","hadn","hasn","haven",
    "isn","ma","mightn","mustn","needn","shan","shouldn","wasn","weren","won","wouldn",
    "http","https","www","com","org","net","edu","gov","html","htm","php",
    "click","read","see","also","via","page","site","website","web","link","url",
    "copyright","rights","reserved","privacy","policy","cookie","cookies","terms",
})

_MIN_TOKEN_LEN = 2
_MAX_TOKEN_LEN = 64

@dataclass(frozen=True)
class Token:
    term: str
    raw: str
    position: int

@dataclass
class TokenStream:
    tokens: list[Token]
    token_count: int

    @property
    def terms(self) -> list[str]:
        return [t.term for t in self.tokens]

    @property
    def unique_terms(self) -> set[str]:
        return set(self.terms)

    def term_frequencies(self) -> dict[str, int]:
        freq: dict[str, int] = {}
        for token in self.tokens:
            freq[token.term] = freq.get(token.term, 0) + 1
        return freq

    def term_positions(self) -> dict[str, list[int]]:
        pos: dict[str, list[int]] = {}
        for token in self.tokens:
            pos.setdefault(token.term, []).append(token.position)
        return pos

class Tokenizer:
    def __init__(self) -> None:
        self._stemmer = _stemmer_class()

    def tokenize(self, text: str, count_all: bool = True) -> TokenStream:
        if not text:
            return TokenStream(tokens=[], token_count=0)
        text = unicodedata.normalize("NFC", text).lower()
        raw_tokens = _TOKEN_RE.findall(text)
        total_count = len(raw_tokens) if count_all else 0
        tokens: list[Token] = []
        position = 0
        for raw in raw_tokens:
            if not (_MIN_TOKEN_LEN <= len(raw) <= _MAX_TOKEN_LEN):
                continue
            if raw in _STOP_WORDS:
                continue
            stemmed = self._stem(raw)
            tokens.append(Token(term=stemmed, raw=raw, position=position))
            position += 1
        return TokenStream(tokens=tokens, token_count=total_count if count_all else position)

    def tokenize_query(self, query: str) -> list[str]:
        return self.tokenize(query, count_all=False).terms

    @lru_cache(maxsize=50_000)
    def _stem(self, word: str) -> str:
        return self._stemmer.stem(word)

    def is_stop_word(self, word: str) -> bool:
        return word.lower() in _STOP_WORDS

_tokenizer_instance: Optional[Tokenizer] = None

def get_tokenizer() -> Tokenizer:
    global _tokenizer_instance
    if _tokenizer_instance is None:
        _tokenizer_instance = Tokenizer()
    return _tokenizer_instance
'''

files["processing/pipeline.py"] = '''"""
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
'''

files["indexing/inverted_index.py"] = '''"""
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
'''

files["ranking/bm25.py"] = '''"""
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
'''

files["query/search_engine.py"] = '''"""
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
        if raw.startswith(\'"\') and raw.endswith(\'"\') and len(raw) > 2:
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
'''

files["api/server.py"] = '''"""
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
'''

files["docker-compose.yml"] = """version: "3.9"
services:
  redis:
    image: redis:7.2-alpine
    container_name: search_redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes --maxmemory 512mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    container_name: search_postgres
    environment:
      POSTGRES_DB: search_engine
      POSTGRES_USER: search_user
      POSTGRES_PASSWORD: search_pass
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U search_user -d search_engine"]
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped

  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    container_name: search_api
    ports:
      - "8000:8000"
    environment:
      - APP_ENV=production
      - REDIS_HOST=redis
      - POSTGRES_HOST=postgres
      - POSTGRES_USER=search_user
      - POSTGRES_PASSWORD=search_pass
      - POSTGRES_DB=search_engine
    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  redis_data:
  postgres_data:
"""

files["docker/Dockerfile.api"] = """FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
"""

files["docker/Dockerfile.crawler"] = """FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-c", "import asyncio; from crawler.crawler import Crawler; from storage.postgres_client import init_db; import os; async def main(): await init_db(); seeds=os.environ.get('SEED_URLS','https://example.com').split(','); await Crawler().run(seeds); asyncio.run(main())"]
"""

files["docker/Dockerfile.indexer"] = """FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-c", "import asyncio; from processing.pipeline import DocumentProcessor; from indexing.inverted_index import IndexBuilder; from storage.postgres_client import init_db; async def main(): await init_db(); await DocumentProcessor().process_batch(500); stats=await IndexBuilder().build(); print(f'Done: {stats.total_docs} docs'); asyncio.run(main())"]
"""

# Write all files
for filepath, content in files.items():
    os.makedirs(os.path.dirname(filepath), exist_ok=True) if os.path.dirname(filepath) else None
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ Created: {filepath}")

print("\n🚀 All files created successfully!")
print("Next step: run   pip install -r requirements.txt")