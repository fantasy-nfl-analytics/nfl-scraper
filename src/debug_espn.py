#!/usr/bin/env python3
"""Debug ESPN page structure."""

import asyncio
import httpx
from bs4 import BeautifulSoup
from nfl_webscraper.http import fetch_html

async def debug_espn():
    url = "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing"
    
    async with httpx.AsyncClient(http2=True) as client:
        soup = await fetch_html(client, url)
        
        print("=== Finding tables ===")
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables")
        
        for i, table in enumerate(tables):
            print(f"\nTable {i}:")
            print(f"Class: {table.get('class', 'No class')}")
            print(f"ID: {table.get('id', 'No ID')}")
            
            # Look for header row
            header_rows = table.find_all('tr')[:3]  # First 3 rows
            for j, row in enumerate(header_rows):
                cells = row.find_all(['th', 'td'])
                texts = [cell.get_text(strip=True) for cell in cells]
                print(f"  Row {j}: {texts}")
        
        print("\n=== Looking for other stat elements ===")
        
        # Look for other patterns ESPN might use
        stat_divs = soup.find_all('div', class_=lambda x: x and 'stat' in x.lower())
        print(f"Found {len(stat_divs)} stat divs")
        
        # Look for Table classes specifically
        table_divs = soup.find_all(['div', 'section'], class_=lambda x: x and 'table' in x.lower())
        print(f"Found {len(table_divs)} elements with 'table' in class")
        
        # Print some sample text to see if data is there
        print(f"\n=== Sample page text ===")
        text = soup.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Look for quarterback names
        qb_lines = [line for line in lines if any(name in line for name in ['Mahomes', 'Allen', 'Burrow', 'Herbert'])]
        print("QB-related lines:")
        for line in qb_lines[:5]:
            print(f"  {line}")

asyncio.run(debug_espn())
