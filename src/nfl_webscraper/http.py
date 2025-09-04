"""HTTP utilities and fetching with retry logic."""

from __future__ import annotations

import asyncio

import httpx
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; nfl-scraper/0.1)'}


async def fetch_html(
	client: httpx.AsyncClient, url: str, *, retries: int = 3, backoff: float = 0.5
) -> BeautifulSoup:
	last_exc: Exception | None = None
	for attempt in range(retries):
		try:
			resp = await client.get(url, headers=DEFAULT_HEADERS, timeout=30.0)
			resp.raise_for_status()
			return BeautifulSoup(resp.text, 'html.parser')
		except Exception as exc:  # noqa: BLE001
			last_exc = exc
			await asyncio.sleep(backoff * (2**attempt))
	raise RuntimeError(f'Failed to fetch {url}: {last_exc}')


__all__ = ['fetch_html', 'DEFAULT_HEADERS']
