import polars as pl
from nfl_webscraper import get_all_player_stats, get_all_team_stats

def test_player_stats_function_signature():
    df = get_all_player_stats(years=None, export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    assert "year" in df.columns or df.shape[0] == 0

def test_team_stats_function_signature():
    df = get_all_team_stats(years=None, export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    assert "year" in df.columns or df.shape[0] == 0
