"""Multi-site public API functions exposed to users of the library.

This module coordinates scraping from multiple sports data sites:
- NFL.com (default)
- Future sites can be added via the sites/ module architecture

Architecture:
1. Site-specific scrapers implement BaseSiteScraper interface
2. Each scraper handles its own discovery, pagination, and parsing logic
3. Results are unified with source attribution
4. Export functionality available for all scrapers

Public entry points (synchronous for convenience):
- `get_all_player_stats(..., sites=...)`
- `get_all_team_stats(..., sites=...)`

Async internal orchestrator: `_gather_multi_site_stats` coordinates across scrapers.
"""

from __future__ import annotations

import asyncio
from typing import Literal

import httpx
import polars as pl

from .schema import unify_frames
from .sites import ESPNScraper, NFLComScraper

# Site registry
SCRAPERS = {
    'nfl.com': NFLComScraper(),
    'espn.com': ESPNScraper(),
}

SiteName = Literal['nfl.com', 'espn.com']


async def _gather_multi_site_stats(
    years: list[int] | None,
    sites: list[SiteName] | SiteName,
    *,
    player: bool,
    export: str | None = None,
    filename: str | None = None,
) -> pl.DataFrame:
    """Gather stats from one or multiple sites.

    Parameters
    ----------
    years:
        Optional collection of season years to restrict the scrape to. If None
        all discovered years are included (typically most recent first).
    sites:
        Site(s) to scrape from. Can be a single site name or list of site names.
    player:
        If True scrape player statistics; otherwise team statistics.
    export:
        Optional string specifying an on-disk export format: 'csv' or 'parquet'.
    filename:
        Path to write export if `export` is provided. Ignored when None.

    Returns
    -------
    pl.DataFrame
        Unified table containing all rows from every (site, year, category) combo.
        Always includes the columns: ['year', 'category', 'source'] when data
        exists; may be empty if no rows were fetched.
    """
    if isinstance(sites, str):
        sites = [sites]

    async with httpx.AsyncClient() as client:
        tasks = []
        for site in sites:
            if site not in SCRAPERS:
                continue
            scraper = SCRAPERS[site]
            if player:
                tasks.append(scraper.get_player_stats(client, years))
            else:
                tasks.append(scraper.get_team_stats(client, years))

        results = await asyncio.gather(*tasks)

    # Unify all results from different sites
    unified = unify_frames([df for df in results if df.shape[0] > 0])

    # Optional export to disk
    if export and filename:
        fmt = export.lower()
        if fmt == 'csv':
            unified.write_csv(filename)
        elif fmt in {'parquet', 'pq'}:
            unified.write_parquet(filename)
        else:
            raise ValueError("export must be 'csv' or 'parquet'")

    return unified


def get_all_player_stats(
    years: list[int] | None = None,
    *,
    sites: list[SiteName] | SiteName = 'nfl.com',
    export: str | None = None,
    filename: str | None = None,
) -> pl.DataFrame:
    """Scrape player stats from one or multiple sites.

    Parameters
    ----------
    years:
        Optional list of season years to scrape. If None, all available years.
    sites:
        Site(s) to scrape from. Defaults to 'nfl.com' for backward compatibility.
    export:
        Optional export format: 'csv' or 'parquet'.
    filename:
        Path to write export file if export is specified.

    Returns
    -------
    pl.DataFrame
        Unified player statistics with columns including ['year', 'category', 'source'].
    """
    return asyncio.run(_gather_multi_site_stats(
        years, sites, player=True, export=export, filename=filename
    ))


def get_all_team_stats(
    years: list[int] | None = None,
    *,
    sites: list[SiteName] | SiteName = 'nfl.com',
    export: str | None = None,
    filename: str | None = None,
) -> pl.DataFrame:
    """Scrape team stats from one or multiple sites.

    Parameters
    ----------
    years:
        Optional list of season years to scrape. If None, all available years.
    sites:
        Site(s) to scrape from. Defaults to 'nfl.com' for backward compatibility.
    export:
        Optional export format: 'csv' or 'parquet'.
    filename:
        Path to write export file if export is specified.

    Returns
    -------
    pl.DataFrame
        Unified team statistics with columns including ['year', 'category', 'source'].
    """
    return asyncio.run(_gather_multi_site_stats(
        years, sites, player=False, export=export, filename=filename
    ))


async def async_main():  # pragma: no cover
    """Demonstration entrypoint printing sample heads for players & teams."""
    players = await _gather_multi_site_stats(None, ['nfl.com'], player=True)
    teams = await _gather_multi_site_stats(None, ['nfl.com'], player=False)
    print(players.head(5))
    print(teams.head(5))


__all__ = ['get_all_player_stats', 'get_all_team_stats', 'async_main']
