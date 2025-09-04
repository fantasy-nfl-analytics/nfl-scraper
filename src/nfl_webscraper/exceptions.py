"""Custom exception types.

Distribution: nfl-webscraper; import package: nflscraping
"""

from __future__ import annotations


class ScraperError(Exception):
	"""Base exception for scraper errors."""


class FetchError(ScraperError):
	"""Raised when an HTTP fetch fails after retries."""


__all__ = ['ScraperError', 'FetchError']
