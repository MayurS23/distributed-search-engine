"""
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
