"""Public API surface.

Distribution name: nfl-webscraper (install)
Import: import nflscraping
"""
from __future__ import annotations
import asyncio
import re
from typing import Iterable
import polars as pl
import httpx
from .discover import (
    PLAYER_ROOT, TEAM_ROOT, PLAYER_CATEGORIES, TEAM_CATEGORIES,
    get_year_urls, get_category_links
)
from .pagination import fetch_all_stats_parallel
from .schema import unify_frames

def ensure_year_in_url(url: str, year: str) -> str:
    if re.search(r"20\d{2}", url):
        return url
    joiner = '&' if '?' in url else '?'
    return f"{url}{joiner}season={year}"

async def _gather_stats(years: Iterable[int] | None, *, player: bool, export: str | None, filename: str | None) -> pl.DataFrame:
    root = PLAYER_ROOT if player else TEAM_ROOT
    categories = PLAYER_CATEGORIES if player else TEAM_CATEGORIES
    async with httpx.AsyncClient(http2=True) as client:
        year_urls = await get_year_urls(client, root)
        if years:
            year_urls = {str(y): u for y, u in year_urls.items() if int(y) in years}
        frames: list[pl.DataFrame] = []
        tasks = []
        semaphore = asyncio.Semaphore(20)
        async def fetch_year_cat(year: str, cat: str, url: str):
            async with semaphore:
                df = await fetch_all_stats_parallel(client, ensure_year_in_url(url, year))
                if df.shape[0] > 0:
                    df = df.with_columns([
                        pl.lit(int(year)).alias("year"),
                        pl.lit(cat).alias("category"),
                    ])
                    frames.append(df)
        for year, base_url in year_urls.items():
            cat_links = await get_category_links(client, base_url, categories)
            if not cat_links:
                cat_links = {list(categories)[0]: base_url}
            for cat, url in cat_links.items():
                tasks.append(fetch_year_cat(year, cat, url))
        await asyncio.gather(*tasks)
    unified = unify_frames(frames)
    if export and filename:
        if export.lower() == 'csv':
            unified.write_csv(filename)
        elif export.lower() in {'parquet','pq'}:
            unified.write_parquet(filename)
        else:
            raise ValueError("export must be 'csv' or 'parquet'")
    return unified

# Synchronous convenience wrappers

def get_all_player_stats(years: list[int] | None = None, *, export: str | None = None, filename: str | None = None) -> pl.DataFrame:
    return asyncio.run(_gather_stats(years, player=True, export=export, filename=filename))

def get_all_team_stats(years: list[int] | None = None, *, export: str | None = None, filename: str | None = None) -> pl.DataFrame:
    return asyncio.run(_gather_stats(years, player=False, export=export, filename=filename))

async def async_main():  # pragma: no cover
    players = await _gather_stats(None, player=True, export=None, filename=None)
    teams = await _gather_stats(None, player=False, export=None, filename=None)
    print(players.head(5))
    print(teams.head(5))

__all__ = [
    "get_all_player_stats","get_all_team_stats","async_main","ensure_year_in_url"
]
