"""Backward-compatible shim; real implementation split into modules."""
from .api import get_all_player_stats, get_all_team_stats, async_main  # re-export

__all__ = ["get_all_player_stats", "get_all_team_stats", "async_main"]
