#!/usr/bin/env python3
"""Debug ESPN parsing step by step."""

import asyncio
import httpx
from bs4 import BeautifulSoup
from nfl_webscraper.http import fetch_html

async def debug_parsing():
    url = "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing"
    
    async with httpx.AsyncClient(http2=True) as client:
        soup = await fetch_html(client, url)
        
        # Find the tablehead table
        table = soup.find('table', class_='tablehead')
        print(f"Found tablehead table: {table is not None}")
        
        if table:
            rows = table.find_all('tr')
            print(f"Number of rows in table: {len(rows)}")
            
            # Analyze each row
            for i, row in enumerate(rows[:5]):  # First 5 rows
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                print(f"Row {i}: {len(cells)} cells")
                print(f"  Texts: {cell_texts[:10]}")  # First 10 cell texts
                
                # Check if this looks like a header row
                has_header_keywords = any(text in ['RK', 'PLAYER', 'TEAM', 'COMP', 'ATT'] for text in cell_texts)
                print(f"  Has header keywords: {has_header_keywords}")
                print()

asyncio.run(debug_parsing())
