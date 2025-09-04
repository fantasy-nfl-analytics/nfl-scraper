#!/usr/bin/env python3
"""Test ESPN scraper year discovery and historical data."""

import asyncio
import httpx
import os
import sys

# Add src directory to path to import the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from nfl_webscraper.sites.espn_com import ESPNScraper


async def test_espn_years():
    """Test ESPN year discovery and historical data fetching."""
    print("Testing ESPN year discovery...")
    
    scraper = ESPNScraper()
    
    async with httpx.AsyncClient() as client:
        # First test year discovery
        print("Discovering available years...")
        years = await scraper._discover_available_years(client)
        print(f"Discovered years: {years}")
        
        # Test gathering stats for discovered years (limit to first 2 for speed)
        test_years = years[:2] if len(years) > 2 else years
        print(f"Testing data fetch for years: {test_years}")
        
        result = await scraper._gather_stats(client, test_years, player=True)
        
        if result.shape[0] > 0:
            print(f"ESPN data fetch successful! Found {result.shape[0]} rows across {result.shape[1]} columns")
            
            # Check what years were actually found in data
            unique_years = sorted(result['year'].unique())
            print(f"Years in data: {unique_years}")
            
            # Show sample data from each year
            for year in unique_years:
                year_data = result.filter(result['year'] == year)
                unique_weeks = sorted(year_data['week'].unique())
                print(f"\n{year} data: {year_data.shape[0]} rows, weeks {unique_weeks[:5]}{'...' if len(unique_weeks) > 5 else ''}")
                
                # Show a sample of passing stats
                passing_sample = year_data.filter(
                    (year_data['category'] == 'passing') &
                    (year_data['week'] == unique_weeks[0])
                ).head(3)
                print(f"Sample {year} week {unique_weeks[0]} passing stats:")
                # Use correct column names - let's check what we have
                cols = passing_sample.columns
                basic_cols = ['player', 'team', 'completions', 'attempts', 'yards', 'touchdowns']
                available_cols = [col for col in basic_cols if col in cols]
                if available_cols:
                    print(passing_sample.select(available_cols).to_pandas().to_string(index=False))
                else:
                    print(f"Available columns: {cols[:10]}")  # Show first 10 columns
        else:
            print("No data found!")


if __name__ == "__main__":
    asyncio.run(test_espn_years())
