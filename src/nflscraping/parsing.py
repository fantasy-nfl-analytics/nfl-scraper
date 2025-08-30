"""HTML parsing helpers for stats tables."""
from __future__ import annotations
import polars as pl
from bs4 import BeautifulSoup

def parse_stats_table(soup: BeautifulSoup) -> tuple[pl.DataFrame, list[str]]:
    table = soup.find('table')
    if not table:
        return pl.DataFrame([]), []
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    rows: list[list[str]] = []
    for tr in table.find_all('tr')[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        if cells:
            rows.append(cells)
        df = pl.DataFrame(rows, schema=headers, orient="row") if headers else pl.DataFrame(rows, orient="row")
    next_links: list[str] = []
    for a in soup.find_all('a'):
        if 'next' in a.get_text(strip=True).lower():
            href = a.get('href')
            if href:
                next_links.append(href)
    return df, next_links

__all__ = ["parse_stats_table"]
