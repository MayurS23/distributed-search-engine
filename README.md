# Distributed Search Engine

A production-grade distributed search engine built from scratch in Python.
Implements core concepts used in Google-scale search systems.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green)
![Redis](https://img.shields.io/badge/Redis-7.2-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Tests](https://img.shields.io/badge/Tests-92%20passing-brightgreen)

---

## What This Project Does

This system crawls web pages, processes and indexes documents, and serves
ranked search results through a REST API — exactly like a real search engine.

---

## Architecture
```
Crawler → Document Processor → Inverted Index → BM25 Ranker → FastAPI
```

- **Crawler** — Async BFS web crawler with robots.txt compliance and rate limiting
- **Document Processor** — HTML parser + tokenizer + Porter stemmer pipeline
- **Inverted Index** — 4-shard term-based hash index with positional postings
- **BM25 Ranking** — Industry-standard ranking algorithm (used by Elasticsearch)
- **Query Engine** — Supports simple, phrase, OR, and negation queries
- **FastAPI Layer** — REST API with caching, pagination, and admin endpoints

---

## Project Structure
```
distributed-search-engine/
├── crawler/          # Async web crawler (aiohttp)
├── processing/       # HTML parser + tokenizer + pipeline
├── indexing/         # Sharded inverted index
├── ranking/          # BM25 scoring algorithm
├── query/            # Query parser + search engine
├── api/              # FastAPI REST endpoints
├── storage/          # Redis + PostgreSQL clients
├── utils/            # Config + structured logging
├── tests/            # 92 unit + integration tests
└── docker-compose.yml
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/search?q=query` | Search documents |
| GET | `/health` | Health check |
| GET | `/metrics` | System metrics |
| POST | `/admin/crawl` | Trigger a crawl |
| POST | `/admin/index/rebuild` | Rebuild the index |
| DELETE | `/admin/cache` | Flush query cache |

### Search Query Types
```bash
# Simple query
GET /search?q=distributed+systems

# Phrase query
GET /search?q="exact phrase"

# OR query
GET /search?q=python+OR+java

# Negation
GET /search?q=programming+-java

# Pagination
GET /search?q=search+engine&page=2&page_size=5
```

### Sample Response
```json
{
  "query": "distributed systems",
  "results": [
    {
      "rank": 1,
      "url": "https://example.com/distributed",
      "title": "Distributed Systems",
      "snippet": "...a distributed system coordinates multiple nodes...",
      "score": 4.821,
      "matched_terms": ["distribut", "system"]
    }
  ],
  "total_found": 42,
  "query_time_ms": 8.3,
  "from_cache": false
}
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API Framework | FastAPI |
| Async HTTP | aiohttp |
| HTML Parsing | BeautifulSoup4 |
| Text Processing | NLTK + Porter Stemmer |
| Ranking | BM25 (custom implementation) |
| Index Storage | Redis |
| Document Storage | PostgreSQL |
| Containerization | Docker + Docker Compose |
| Testing | pytest (92 tests) |

---

## Quick Start
```bash
# Start Redis and PostgreSQL
docker-compose up -d redis postgres

# Install dependencies
pip install -r requirements.txt

# Start the API
uvicorn api.server:app --reload --port 8000

# Trigger a crawl
curl -X POST http://localhost:8000/admin/crawl \
  -H "Content-Type: application/json" \
  -d '{"seed_urls": ["https://en.wikipedia.org/wiki/Search_engine"]}'

# Rebuild index
curl -X POST http://localhost:8000/admin/index/rebuild

# Search!
curl "http://localhost:8000/search?q=inverted+index"
```

---

## Key Concepts Demonstrated

- **Distributed Systems** — Sharded index across multiple nodes
- **Search Algorithms** — BM25 ranking with TF-IDF comparison
- **Async Programming** — Full asyncio + aiohttp crawler
- **Data Structures** — Positional inverted index with postings lists
- **API Design** — RESTful FastAPI with caching and pagination
- **Testing** — 92 unit + integration tests, zero external dependencies

---

## Running Tests
```bash
# Run all 92 tests
python -m pytest tests/ -v

# Run specific module
python -m pytest tests/test_ranking.py -v
python -m pytest tests/test_integration.py -v
```

---

## Author

Built as a deep-dive into distributed search systems architecture.