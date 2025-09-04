#!/usr/bin/env python3
"""Find the correct ESPN data table."""

import asyncio
import httpx
from nfl_webscraper.http import fetch_html

async def find_correct_table():
    url = "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing"
    
    async with httpx.AsyncClient(http2=True) as client:
        soup = await fetch_html(client, url)
        
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables total")
        
        for i, table in enumerate(tables):
            print(f"\n=== TABLE {i} ===")
            print(f"Class: {table.get('class', [])}")
            
            rows = table.find_all('tr')
            print(f"Rows: {len(rows)}")
            
            # Look at first few rows to identify the data table
            for j, row in enumerate(rows[:3]):
                cells = row.find_all(['td', 'th'])
                texts = [cell.get_text(strip=True) for cell in cells]
                print(f"  Row {j} ({len(cells)} cells): {texts[:15]}")
                
                # Check for the specific pattern we saw in the web data
                if 'Sortable Passing Leaders' in texts:
                    print("  *** FOUND HEADER TABLE ***")
                elif any(text in ['RK', 'PLAYER', 'TEAM'] for text in texts):
                    print("  *** FOUND COLUMN HEADERS ***")
                elif len(texts) > 5 and any('QB' in str(text) for text in texts):
                    print("  *** FOUND DATA ROW ***")

asyncio.run(find_correct_table())
