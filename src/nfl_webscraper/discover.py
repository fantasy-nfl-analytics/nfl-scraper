"""Discovery of years and category links."""
from __future__ import annotations
import re
from datetime import datetime
from typing import Dict
import httpx
from .http import fetch_html

BASE_URL = "https://www.nfl.com"
PLAYER_ROOT = f"{BASE_URL}/stats/player-stats/"
TEAM_ROOT = f"{BASE_URL}/stats/team-stats/"

PLAYER_CATEGORIES = {
    'passing','rushing','receiving','fumbles','tackles','interceptions',
    'field goals','kickoffs','kickoff returns','punting','punt returns'
}
TEAM_CATEGORIES = {'passing','rushing','receiving','scoring','downs'}

async def get_year_urls(client: httpx.AsyncClient, stats_url: str) -> Dict[str,str]:
    try:
        soup = await fetch_html(client, stats_url)
    except Exception:
        cur = str(datetime.now().year)
        return {cur: stats_url}
    year_map: Dict[str,str] = {}
    for opt in soup.find_all('option'):
        text = opt.get_text(strip=True)
        if re.fullmatch(r"20\d{2}", text):
            href = opt.get('value') or opt.get('data-url')
            if href and not href.startswith('http'):
                href = BASE_URL.rstrip('/') + '/' + href.lstrip('/')
            year_map[text] = href or stats_url
    if not year_map:
        for a in soup.find_all('a'):
            txt = a.get_text(strip=True)
            if re.fullmatch(r"20\d{2}", txt):
                href = a.get('href')
                if href and not href.startswith('http'):
                    href = BASE_URL.rstrip('/') + '/' + href.lstrip('/')
                year_map[txt] = href or stats_url
    if not year_map:
        year_map[str(datetime.now().year)] = stats_url
    return dict(sorted(year_map.items(), reverse=True))

async def get_category_links(client: httpx.AsyncClient, root_url: str, wanted: set[str]) -> dict[str,str]:
    soup = await fetch_html(client, root_url)
    sections: dict[str,str] = {}
    for a in soup.find_all('a'):
        text = a.get_text(strip=True).lower()
        if text in wanted:
            href = a.get('href')
            if not href:
                continue
            if not href.startswith('http'):
                href = BASE_URL.rstrip('/') + '/' + href.lstrip('/')
            sections[text] = href
    return sections

__all__ = [
    "BASE_URL","PLAYER_ROOT","TEAM_ROOT","PLAYER_CATEGORIES","TEAM_CATEGORIES","get_year_urls","get_category_links"
]
