"""Pagination logic for stats pages."""
from __future__ import annotations
import asyncio
import polars as pl
from .http import fetch_html
from .parsing import parse_stats_table
from .discover import BASE_URL
import httpx

async def fetch_all_stats_parallel(client: httpx.AsyncClient, start_url: str, *, concurrency: int = 20) -> pl.DataFrame:
    first_soup = await fetch_html(client, start_url)
    first_df, next_links = parse_stats_table(first_soup)
    headers = list(first_df.columns) if first_df.shape[1] > 0 else None
    rows = list(first_df.rows()) if first_df.shape[0] > 0 else []
    page_urls: set[str] = set()
    for a in first_soup.find_all('a'):
        tx = a.get_text(strip=True)
        if tx.isdigit() or tx.lower() == 'next':
            href = a.get('href')
            if href:
                if not href.startswith('http'):
                    href = BASE_URL.rstrip('/') + '/' + href.lstrip('/')
                page_urls.add(href)
    for nl in next_links:
        if not nl.startswith('http'):
            nl = BASE_URL.rstrip('/') + '/' + nl.lstrip('/')
        page_urls.add(nl)
    if start_url in page_urls:
        page_urls.remove(start_url)
    if not page_urls:
            return pl.DataFrame(rows, schema=headers, orient="row") if headers else pl.DataFrame(rows, orient="row")
    semaphore = asyncio.Semaphore(concurrency)
    async def fetch_and_parse(u: str) -> list[tuple]:
        async with semaphore:
            soup = await fetch_html(client, u)
            df, _ = parse_stats_table(soup)
            return list(df.rows()) if df.shape[0] > 0 else []
    tasks = [fetch_and_parse(u) for u in sorted(page_urls)]
    for part in await asyncio.gather(*tasks):
        rows.extend(part)
        return pl.DataFrame(rows, schema=headers, orient="row") if headers else pl.DataFrame(rows, orient="row")

__all__ = ["fetch_all_stats_parallel"]
