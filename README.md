# nflScraping

Async scraper for NFL player & team stats using httpx + Polars.

## Quick start

Install dev extras:

```
uv sync --extra dev
```

Usage (Python):
```python
import nflscraping as ns
players = ns.get_all_player_stats([2023])
print(players.head())
```

## License
MIT
