"""Public package interface.

Distribution name: nfl-webscraper
Import package name: nfl_webscraper
Version is resolved dynamically from installed metadata.

New in this version: Multi-site support via sites parameter.
"""

from __future__ import annotations

from importlib import metadata as _md

from .api import get_all_player_stats, get_all_team_stats

try:  # Resolve version from the distribution metadata
    __version__ = _md.version('nfl-webscraper')
except _md.PackageNotFoundError:  # pragma: no cover - dev editable fallback
    __version__ = '0.1.2'

__all__ = ['get_all_player_stats', 'get_all_team_stats', '__version__']
