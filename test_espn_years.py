#!/usr/bin/env python3
"""Test ESPN scraper year discovery and historical data."""

import asyncio
import os
import sys

# Add src directory to path to import the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from nfl_webscraper.api import get_all_player_stats


async def test_espn_years():
    """Test ESPN year discovery and historical data fetching."""
    print("Testing ESPN year discovery...")
    
    # Test with years=None to trigger discovery
    result = await get_all_player_stats(years=None, sites='espn.com')
    
    if result.shape[0] > 0:
        print(f"ESPN discovery successful! Found {result.shape[0]} rows across {result.shape[1]} columns")
        
        # Check what years were discovered
        unique_years = sorted(result['year'].unique())
        print(f"Years discovered: {unique_years}")
        
        # Show sample data from multiple years if available
        for year in unique_years[:2]:  # Show first 2 years
            year_data = result.filter(result['year'] == year)
            print(f"\n{year} sample data ({year_data.shape[0]} rows):")
            print(year_data.head(3).to_pandas().to_string())
    else:
        print("No data found!")


if __name__ == "__main__":
    asyncio.run(test_espn_years())
