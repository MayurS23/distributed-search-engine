"""
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
