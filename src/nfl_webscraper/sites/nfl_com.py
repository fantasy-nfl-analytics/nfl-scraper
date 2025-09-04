"""NFL.com specific scraper implementation."""

from __future__ import annotations

import asyncio
import re

import httpx
import polars as pl

from ..discover import (
    PLAYER_CATEGORIES,
    PLAYER_ROOT,
    TEAM_CATEGORIES,
    TEAM_ROOT,
    get_category_links,
    get_year_urls,
)
from ..pagination import fetch_all_stats_parallel
from ..schema import unify_frames
from .base import BaseSiteScraper


def ensure_year_in_url(url: str, year: str) -> str:
    """Ensure the target stats URL includes an explicit `season` query param.

    Some category links already contain a year (e.g., `/stats/.../2023/...`).
    If a year-like pattern (20xx) is found anywhere in the URL we leave it untouched.
    Otherwise we append `?season=<year>` (or `&season=<year>` if a query already exists).
    """
    if re.search(r'20\d{2}', url):  # Already year-specific
        return url
    joiner = '&' if '?' in url else '?'
    return f'{url}{joiner}season={year}'


class NFLComScraper(BaseSiteScraper):
    """Scraper for NFL.com stats."""

    @property
    def site_name(self) -> str:
        return "NFL.com"

    async def get_player_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch player stats from NFL.com for given years."""
        return await self._gather_stats(client, years, player=True)

    async def get_team_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch team stats from NFL.com for given years."""
        return await self._gather_stats(client, years, player=False)

    async def _gather_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None,
        *,
        player: bool
    ) -> pl.DataFrame:
        """Internal method that orchestrates the full NFL.com scrape.

        Parameters
        ----------
        client:
            HTTP client to use for requests.
        years:
            Optional collection of season years to restrict the scrape to. If None
            all discovered years are included (typically most recent first).
        player:
            If True scrape player statistics; otherwise team statistics.

        Returns
        -------
        pl.DataFrame
            Unified table containing all rows from every (year, category) combo.
            Always includes at least the columns: ['year', 'category', 'source'] when data
            exists; may be empty if no rows were fetched.
        """
        root = PLAYER_ROOT if player else TEAM_ROOT
        categories = PLAYER_CATEGORIES if player else TEAM_CATEGORIES

        # Discover available years from the root stats page.
        year_urls = await get_year_urls(client, root)
        if years:
            # Filter discovered years to requested subset (keeping only those present).
            year_urls = {str(y): u for y, u in year_urls.items() if int(y) in years}

        frames: list[pl.DataFrame] = []  # Collected raw DataFrames per category/year.
        tasks: list[asyncio.Task] = []

        # Throttle maximum concurrent page-category fetches to avoid overloading the site.
        semaphore = asyncio.Semaphore(20)

        async def fetch_year_cat(year: str, cat: str, url: str) -> None:
            """Fetch one (year, category) table (with pagination) and append to frames if non-empty."""
            async with semaphore:
                df = await fetch_all_stats_parallel(client, ensure_year_in_url(url, year))
                if df.shape[0] > 0:
                    # Attach context columns before accumulation.
                    df = df.with_columns([
                        pl.lit(int(year)).alias('year'),
                        pl.lit(cat).alias('category'),
                        pl.lit(self.site_name).alias('source'),
                    ])
                    frames.append(df)

        # For each year, discover category links and enqueue fetch tasks.
        for year, base_url in year_urls.items():
            cat_links = await get_category_links(client, base_url, categories)
            if not cat_links:  # Fallback: treat base page as a single category (first of the set)
                cat_links = {list(categories)[0]: base_url}
            for cat, url in cat_links.items():
                tasks.append(asyncio.create_task(fetch_year_cat(year, cat, url)))

        # Execute all tasks concurrently (respecting semaphore limits).
        await asyncio.gather(*tasks)

        # Unify schemas across all gathered frames (handles missing columns & dtypes).
        return unify_frames(frames)
