#!/usr/bin/env python3
"""Test ESPN year availability."""

import asyncio
import httpx
from nfl_webscraper.http import fetch_html

async def test_espn_years():
    """Test if ESPN supports historical years by trying different years."""
    test_years = [2024, 2023, 2022, 2021, 2020]
    
    async with httpx.AsyncClient(http2=True) as client:
        for year in test_years:
            # Try different URL patterns that might include year
            test_urls = [
                f"https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing/year/{year}",
                f"https://www.espn.com/nfl/weekly/leaders/{year}/_/week/1/seasontype/2/type/passing",
                f"https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing?year={year}",
            ]
            
            for url in test_urls:
                try:
                    print(f"Testing {year}: {url}")
                    soup = await fetch_html(client, url)
                    
                    # Look for year in title
                    title = soup.find('title')
                    if title:
                        title_text = title.get_text()
                        print(f"  Title: {title_text}")
                        if str(year) in title_text:
                            print(f"  ✅ Found {year} in title!")
                        else:
                            print(f"  ❌ {year} not in title")
                    
                    # Look for data table
                    table = soup.find('table', class_='tablehead')
                    if table and 'Sortable' in str(table):
                        print(f"  ✅ Found data table")
                    else:
                        print(f"  ❌ No data table found")
                        
                except Exception as e:
                    print(f"  ❌ Error: {e}")
                
                print()

asyncio.run(test_espn_years())
