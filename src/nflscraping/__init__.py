"""Public package interface for nflscraping."""
from .api import get_all_player_stats, get_all_team_stats

__version__ = "0.1.1"

__all__ = ["get_all_player_stats", "get_all_team_stats", "__version__"]
