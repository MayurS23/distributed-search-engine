# 🔍 Distributed Search Engine

> A production-grade distributed search engine built from scratch in Python — crawls web pages, builds a sharded inverted index, and serves ranked results via a REST API. Implements the core concepts behind Google-scale search systems.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/FastAPI-0.111-009688?style=for-the-badge&logo=fastapi&logoColor=white"/>
  <img src="https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Tests-92%20Passing-brightgreen?style=for-the-badge"/>
</p>

---

## What This Project Does

This system works exactly like a real search engine:

1. Crawls web pages starting from seed URLs
2. Parses HTML and extracts clean text
3. Tokenizes text using stemming and stop word removal
4. Builds a sharded inverted index stored in Redis
5. Ranks results using the BM25 algorithm (same as Elasticsearch)
6. Serves search results through a FastAPI REST API

---

## System Architecture

```
INDEXING PIPELINE
  Crawler --> Raw HTML --> Parser --> Tokenizer --> Inverted Index (4 Redis Shards)

QUERY PIPELINE
  Client --> FastAPI --> Query Parser --> BM25 Scorer --> Ranked Results
                  |
             Redis Cache
```

---

## Project Structure

```
distributed-search-engine/
├── crawler/
│   ├── crawler.py          # Async BFS crawler (aiohttp + asyncio)
│   └── robots.py           # robots.txt compliance
├── processing/
│   ├── parser.py           # HTML to clean text extraction
│   ├── tokenizer.py        # Normalize, tokenize, stem
│   └── pipeline.py         # Full processing orchestrator
├── indexing/
│   └── inverted_index.py   # 4-shard term-based hash index
├── ranking/
│   └── bm25.py             # BM25 + TF-IDF scoring
├── query/
│   └── search_engine.py    # Query parser + result hydrator
├── api/
│   └── server.py           # FastAPI endpoints
├── storage/
│   ├── redis_client.py     # Redis connection pool
│   └── postgres_client.py  # Async PostgreSQL client
├── utils/
│   ├── config.py           # Pydantic settings
│   └── logger.py           # Structured JSON logging
├── tests/                  # 92 unit + integration tests
├── docker/                 # Per-service Dockerfiles
├── docker-compose.yml
└── requirements.txt
```

---

## Prerequisites

| Tool | Version | Download |
|------|---------|----------|
| Python | 3.12 | https://python.org/downloads/release/python-3129/ |
| Docker Desktop | Latest | https://docker.com/products/docker-desktop |
| Git | Latest | https://git-scm.com |

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/MayurS23/distributed-search-engine.git
cd distributed-search-engine
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Redis and PostgreSQL

```bash
docker-compose up -d redis postgres
```

### 4. Start the API Server

```bash
uvicorn api.server:app --reload --port 8000
```

### 5. Open API Docs

Visit http://localhost:8000/docs for the interactive Swagger UI.

---

## Trigger a Crawl and Search

```bash
# Step 1: Crawl a website
curl -X POST http://localhost:8000/admin/crawl \
  -H "Content-Type: application/json" \
  -d '{"seed_urls": ["https://en.wikipedia.org/wiki/Search_engine"], "max_pages": 100}'

# Step 2: Rebuild the index
curl -X POST http://localhost:8000/admin/index/rebuild

# Step 3: Search!
curl "http://localhost:8000/search?q=inverted+index"
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/search?q=query` | Search indexed documents |
| GET | `/health` | Check system health |
| GET | `/metrics` | Corpus and index statistics |
| GET | `/docs` | Interactive Swagger UI |
| POST | `/admin/crawl` | Trigger a web crawl |
| POST | `/admin/index/rebuild` | Rebuild the inverted index |
| DELETE | `/admin/cache` | Flush query cache |

### Query Syntax

```bash
# Simple
GET /search?q=distributed+systems

# Phrase (exact match)
GET /search?q="inverted index"

# OR
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
      "title": "Distributed Systems Architecture",
      "snippet": "...a distributed system coordinates nodes across a network...",
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

## Running Tests

```bash
# Run all 92 tests
python -m pytest tests/ -v

# Specific modules
python -m pytest tests/test_ranking.py -v
python -m pytest tests/test_integration.py -v

# With coverage
python -m pytest tests/ --cov=. --cov-report=html
```

Result: **92 passed in 8.42s**

---

## Configuration

Edit `.env` to change any setting:

```env
CRAWLER_MAX_PAGES=500        # Max pages per crawl
CRAWLER_CONCURRENCY=10       # Parallel crawler workers
INDEX_NUM_SHARDS=4           # Number of index shards
BM25_K1=1.5                  # Term frequency saturation
BM25_B=0.75                  # Document length normalization
CACHE_TTL_SECONDS=300        # Query cache expiry (seconds)
```

---

## Docker Deployment

Run the full stack with one command:

```bash
docker-compose up --build
```

Services started:
- Redis on port 6379
- PostgreSQL on port 5432
- API Server on port 8000

---

## Key Concepts

### BM25 Ranking
The same algorithm used by Elasticsearch and Apache Lucene. Scores documents based on term frequency with saturation and document length normalization.

```
Score(D,Q) = sum[ IDF(q) x TF(q,D) x (k1+1) / (TF(q,D) + k1 x (1-b+b x |D|/avgdl)) ]
```

### Sharded Inverted Index
Terms distributed across 4 shards via SHA-256 hashing for parallel query processing and horizontal scaling.

### Positional Index
Every posting stores token positions enabling phrase queries like "distributed systems".

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API Framework | FastAPI |
| Async HTTP | aiohttp |
| HTML Parsing | BeautifulSoup4 + lxml |
| Text Processing | NLTK + Porter Stemmer |
| Ranking | BM25 (custom implementation) |
| Index Storage | Redis |
| Document Storage | PostgreSQL + SQLAlchemy |
| Logging | Structlog |
| Config | Pydantic Settings |
| Testing | pytest (92 tests) |
| Containers | Docker + Docker Compose |

---

## Author

**Mayur S** — [@MayurS23](https://github.com/MayurS23)

If you found this useful, please give it a star on GitHub!