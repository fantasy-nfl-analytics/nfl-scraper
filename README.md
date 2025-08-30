# nfl-webscraper

Async scraper for NFL player & team stats using httpx + Polars.

Distribution name: `nfl-webscraper`
Import name: `nflscraping`

## Quick start

Install (latest release):

```
uv tool install nfl-webscraper  # or: pip install nfl-webscraper
```

Install dev extras (from source clone):

```
uv sync --extra dev
```

Usage (Python):
```python
import nflscraping as ns
players = ns.get_all_player_stats([2023])
print(players.head())
```

Repository: https://github.com/fantasy-nfl-analytics/nfl-webscraper/

## License
MIT
