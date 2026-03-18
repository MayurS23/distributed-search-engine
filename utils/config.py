"""
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
