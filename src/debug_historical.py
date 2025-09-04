#!/usr/bin/env python3
"""Debug ESPN historical year page structure."""

import asyncio
import httpx
from nfl_webscraper.http import fetch_html

async def debug_historical_espn():
    """Debug what's different about historical ESPN pages."""
    
    # Compare 2024 (working) vs 2023 (not working)
    urls = {
        "2024_current": "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing",
        "2024_with_year": "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing/year/2024", 
        "2023_with_year": "https://www.espn.com/nfl/weekly/leaders/_/week/1/seasontype/2/type/passing/year/2023"
    }
    
    async with httpx.AsyncClient(http2=True) as client:
        for name, url in urls.items():
            print(f"\n=== {name.upper()} ===")
            print(f"URL: {url}")
            
            try:
                soup = await fetch_html(client, url)
                
                # Check title
                title = soup.find('title')
                if title:
                    print(f"Title: {title.get_text()}")
                
                # Look for different table types
                all_tables = soup.find_all('table')
                print(f"Total tables found: {len(all_tables)}")
                
                for i, table in enumerate(all_tables):
                    classes = table.get('class', [])
                    print(f"  Table {i}: classes={classes}")
                    
                    # Check if it has sortable content
                    if 'tablehead' in classes:
                        rows = table.find_all('tr')[:3]
                        for j, row in enumerate(rows):
                            cells = row.find_all(['td', 'th'])
                            texts = [cell.get_text(strip=True) for cell in cells[:5]]
                            print(f"    Row {j}: {texts}")
                
                # Look for any text that might indicate no data
                page_text = soup.get_text()
                if 'no data' in page_text.lower() or 'not found' in page_text.lower():
                    print("⚠️  Found 'no data' or 'not found' text")
                
                # Look for JavaScript that might be loading data
                scripts = soup.find_all('script')
                for script in scripts:
                    script_text = script.string or ""
                    if 'leader' in script_text.lower() or 'stats' in script_text.lower():
                        print("📜 Found stats-related JavaScript")
                        break
                        
            except Exception as e:
                print(f"❌ Error: {e}")

asyncio.run(debug_historical_espn())
