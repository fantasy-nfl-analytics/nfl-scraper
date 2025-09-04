#!/usr/bin/env python3
"""Test the main API with ESPN year discovery."""

import os
import sys

# Add src directory to path to import the package  
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from nfl_webscraper.api import _gather_multi_site_stats
import asyncio


async def test_api_year_discovery():
    """Test that the API properly discovers ESPN years."""
    print("Testing API with ESPN year discovery...")
    
    # Use the internal async function to avoid nested event loops
    result = await _gather_multi_site_stats(
        years=None,  # This should trigger year discovery
        sites=['espn.com'],
        player=True,
        export=None,
        filename=None
    )
    
    if result.shape[0] > 0:
        print(f"API test successful! Found {result.shape[0]} rows across {result.shape[1]} columns")
        
        # Check what years were discovered
        unique_years = sorted(result['year'].unique())
        print(f"Years automatically discovered: {unique_years}")
        
        # Show data breakdown by year
        for year in unique_years:
            year_data = result.filter(result['year'] == year)
            categories = sorted(year_data['category'].unique())
            print(f"  {year}: {year_data.shape[0]} rows, categories: {categories}")
            
        # Show a quick sample
        sample = result.head(5)
        print(f"\nSample data:")
        print(sample.select(['year', 'week', 'season_type', 'category', 'player', 'team', 'yards']).to_pandas().to_string(index=False))
        
    else:
        print("No data found!")


if __name__ == "__main__":
    asyncio.run(test_api_year_discovery())
