#!/usr/bin/env python3
"""Quick test of ESPN scraper functionality."""

import nfl_webscraper as nws

# Test a small ESPN scrape
print("Testing ESPN scraper...")
df = nws.get_all_player_stats(years=[2024], sites='espn.com')

print(f"ESPN data shape: {df.shape}")
print(f"Columns: {df.columns}")

if df.shape[0] > 0:
    print("\nFirst few rows:")
    print(df.head())
    
    print(f"\nUnique sources: {df['source'].unique().to_list()}")
    if 'week' in df.columns:
        print(f"Weeks found: {sorted(df['week'].unique().to_list())}")
    if 'season_type' in df.columns:
        print(f"Season types: {df['season_type'].unique().to_list()}")
    if 'category' in df.columns:
        print(f"Categories: {df['category'].unique().to_list()}")
else:
    print("No data returned from ESPN")

print("\nTesting a manual ESPN URL...")
import httpx
import asyncio
from nfl_webscraper.sites.espn_com import ESPNScraper

async def test_single_url():
    scraper = ESPNScraper()
    async with httpx.AsyncClient(http2=True) as client:
        url = "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing"
        print(f"Fetching: {url}")
        
        from nfl_webscraper.http import fetch_html
        soup = await fetch_html(client, url)
        df = scraper._parse_stats_table(soup)
        
        print(f"Single URL result shape: {df.shape}")
        if df.shape[0] > 0:
            print("Headers:", df.columns)
            print(df.head(3))

asyncio.run(test_single_url())
