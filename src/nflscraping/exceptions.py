"""Custom exception types for the nflscraping package."""
from __future__ import annotations

class ScraperError(Exception):
    """Base exception for scraper errors."""

class FetchError(ScraperError):
    """Raised when an HTTP fetch fails after retries."""

__all__ = ["ScraperError", "FetchError"]
