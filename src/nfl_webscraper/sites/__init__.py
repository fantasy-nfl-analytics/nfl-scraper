"""Site-specific scraping implementations."""

from .espn_com import ESPNScraper
from .nfl_com import NFLComScraper

__all__ = ['NFLComScraper', 'ESPNScraper']
