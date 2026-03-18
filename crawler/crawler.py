"""
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
