"""ESPN.com specific scraper implementation."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import polars as pl
from bs4 import BeautifulSoup

from ..http import fetch_html
from ..schema import unify_frames
from .base import BaseSiteScraper


class ESPNScraper(BaseSiteScraper):
    """Scraper for ESPN.com NFL stats."""

    BASE_URL = "https://www.espn.com/nfl/weekly/leaders"

    # ESPN categories mapping
    STAT_TYPES = {
        'passing': 'passing',
        'rushing': 'rushing',
        'receiving': 'receiving',
        'defensive': 'defensive'
    }

    # Season types
    SEASON_TYPES = {
        'regular': 2,
        'postseason': 3
    }

    @property
    def site_name(self) -> str:
        return "ESPN.com"

    async def get_player_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch player stats from ESPN.com for given years."""
        return await self._gather_stats(client, years, player=True)

    async def get_team_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None = None
    ) -> pl.DataFrame:
        """Fetch team stats from ESPN.com for given years."""
        # ESPN weekly leaders are player-focused, but we can aggregate
        # For now, return empty DataFrame - team stats would need different endpoint
        return pl.DataFrame([])

    async def _gather_stats(
        self,
        client: httpx.AsyncClient,
        years: list[int] | None,
        *,
        player: bool
    ) -> pl.DataFrame:
        """Internal method that orchestrates ESPN scraping.

        ESPN provides weekly stats, so we'll fetch all weeks for each year/stat type.
        """
        if not player:
            # Team stats not implemented yet for ESPN weekly leaders
            return pl.DataFrame([])

        # ESPN supports historical years - discover available years or use defaults
        if years is None:
            years = await self._discover_available_years(client)
        
        frames: list[pl.DataFrame] = []
        tasks: list[asyncio.Task] = []

        # Throttle requests to be respectful
        semaphore = asyncio.Semaphore(10)

        async def fetch_week_stats(year: int, week: int, stat_type: str, season_type: str) -> None:
            """Fetch stats for one week/stat type combination."""
            async with semaphore:
                try:
                    url = self._build_url(week, season_type, stat_type, year)
                    soup = await fetch_html(client, url)
                    df = self._parse_stats_table(soup)

                    if df.shape[0] > 0:
                        # Add context columns
                        df = df.with_columns([
                            pl.lit(year).alias('year'),
                            pl.lit(week).alias('week'),
                            pl.lit(season_type).alias('season_type'),
                            pl.lit(stat_type).alias('category'),
                            pl.lit(self.site_name).alias('source'),
                        ])
                        frames.append(df)

                except Exception as e:
                    # Log error but continue with other weeks
                    print(f"Error fetching ESPN {year} week {week} {stat_type}: {e}")

        # Generate tasks for all combinations
        for year in years:
            for stat_type in self.STAT_TYPES.keys():
                # Regular season weeks 1-18
                for week in range(1, 19):
                    tasks.append(asyncio.create_task(
                        fetch_week_stats(year, week, stat_type, 'regular')
                    ))

                # Postseason weeks (typically 4-5 weeks: Wild Card, Divisional, Conference, Super Bowl)
                for week in range(1, 6):
                    tasks.append(asyncio.create_task(
                        fetch_week_stats(year, week, stat_type, 'postseason')
                    ))

        # Execute all tasks
        await asyncio.gather(*tasks, return_exceptions=True)

        return unify_frames(frames)

    async def _discover_available_years(self, client: httpx.AsyncClient) -> list[int]:
        """Discover available years for ESPN weekly leaders.
        
        ESPN supports historical data back several years.
        We'll test a reasonable range and return years that have data.
        """
        current_year = 2024  # You might want to make this dynamic
        test_years = list(range(current_year - 5, current_year + 1))  # Last 6 years
        available_years = []
        
        # Test each year by checking if we can get week 1 passing data
        for year in test_years:
            try:
                url = self._build_url(1, 'regular', 'passing', year)
                soup = await fetch_html(client, url)
                
                # Check if we found actual data
                df = self._parse_stats_table(soup)
                if df.shape[0] > 0:
                    available_years.append(year)
                    
            except Exception:
                # If we can't fetch the year, skip it
                continue
        
        # If we found no years, fall back to current year
        return available_years if available_years else [current_year]

    def _build_url(self, week: int, season_type: str, stat_type: str, year: int | None = None) -> str:
        """Build ESPN URL for specific week/type combination."""
        season_type_num = self.SEASON_TYPES[season_type]
        base_url = f"{self.BASE_URL}/_/week/{week}/seasontype/{season_type_num}/type/{stat_type}"
        
        # Add year if specified (for historical data)
        if year is not None:
            base_url += f"/year/{year}"
            
        return base_url

    def _parse_stats_table(self, soup: BeautifulSoup) -> pl.DataFrame:
        """Parse ESPN stats table from BeautifulSoup object."""
        # ESPN uses specific table structure - find table with "Sortable" in title
        tables = soup.find_all('table', class_='tablehead')
        target_table = None
        
        for table in tables:
            # Look for the table that contains "Sortable" in the first row
            first_row = table.find('tr')
            if first_row:
                first_cell = first_row.find(['td', 'th'])
                if first_cell and 'Sortable' in first_cell.get_text(strip=True):
                    target_table = table
                    break
        
        if not target_table:
            return pl.DataFrame([])

        try:
            rows = target_table.find_all('tr')
            min_rows = 2  # Need at least title + header rows
            if len(rows) < min_rows:
                return pl.DataFrame([])
            
            # Row 0: Title (e.g., "Sortable Passing Leaders")
            # Row 1: Headers (RK, PLAYER, TEAM, etc.)
            # Row 2+: Data
            
            header_row = rows[1] if len(rows) > 1 else None
            if not header_row:
                return pl.DataFrame([])
            
            # Extract headers
            header_cells = header_row.find_all(['th', 'td'])
            headers = [self._clean_header(cell.get_text(strip=True)) for cell in header_cells]
            headers = [h for h in headers if h]  # Remove empty headers
            
            if not headers:
                return pl.DataFrame([])

            # Extract data rows (start from row 2)
            data_rows = []
            for row in rows[2:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= len(headers):
                    row_data = []
                    for i, cell in enumerate(cells[:len(headers)]):
                        text = cell.get_text(strip=True)
                        cleaned = self._clean_cell_value(text, headers[i] if i < len(headers) else '')
                        row_data.append(cleaned)
                    
                    # Only add rows that have meaningful data
                    if any(val is not None and str(val).strip() != '' for val in row_data):
                        data_rows.append(row_data)

            if not data_rows:
                return pl.DataFrame([])

            # Create DataFrame
            return pl.DataFrame(data_rows, schema=headers, orient='row')

        except Exception as e:
            print(f"Error parsing ESPN table: {e}")
            return pl.DataFrame([])

    def _clean_header(self, header: str) -> str:
        """Clean and standardize header names."""
        # Map ESPN headers to standard names
        header_mapping = {
            'RK': 'rank',
            'PLAYER': 'player',
            'TEAM': 'team',
            'RESULT': 'result',
            'COMP': 'completions',
            'ATT': 'attempts',
            'YDS': 'yards',
            'TD': 'touchdowns',
            'INT': 'interceptions',
            'SACK': 'sacks',
            'FUM': 'fumbles',
            'RAT': 'rating',
            'CAR': 'carries',
            'AVG': 'average',
            'LNG': 'longest',
            'REC': 'receptions',
            'TGT': 'targets'
        }

        cleaned = header.upper().strip()
        return header_mapping.get(cleaned, cleaned.lower())

    def _clean_cell_value(self, value: str, header: str) -> Any:
        """Clean and convert cell values to appropriate types."""
        if not value or value == '--':
            return None

        # Type conversion mapping
        type_mapping = {
            'numeric': {
                'rank', 'completions', 'attempts', 'yards', 'touchdowns',
                'interceptions', 'sacks', 'fumbles', 'carries', 'receptions',
                'targets', 'longest'
            },
            'float': {'rating', 'average'}
        }

        # Conversion functions
        converters = {
            'numeric': lambda v: int(v.replace(',', '')),
            'float': lambda v: float(v)
        }

        # Try type conversions
        for data_type, headers in type_mapping.items():
            if header in headers:
                try:
                    return converters[data_type](value)
                except ValueError:
                    return None

        # Handle special cases and return cleaned string
        if header == 'player' and ',' in value:
            name, _ = value.split(',', 1)
            return name.strip()

        return value.strip()
