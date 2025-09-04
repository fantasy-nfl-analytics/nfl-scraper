"""Abstract base class for site-specific scrapers."""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx
import polars as pl


class BaseSiteScraper(ABC):
    """Abstract scraper interface for different sports sites."""

    @property
    @abstractmethod
    def site_name(self) -> str:
        """Human-readable site identifier."""

    @abstractmethod
    async def get_player_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch player stats for given years."""

    @abstractmethod
    async def get_team_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch team stats for given years."""
