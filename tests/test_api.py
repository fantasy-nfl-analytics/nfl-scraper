import polars as pl

from nfl_webscraper import get_all_player_stats, get_all_team_stats


def test_player_stats_function_signature():
    """Test player stats function with default NFL.com site."""
    df = get_all_player_stats(years=None, sites='nfl.com', export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    assert 'year' in df.columns or df.shape[0] == 0
    # Check that source column is added
    if df.shape[0] > 0:
        assert 'source' in df.columns
        assert df['source'].unique().to_list() == ['NFL.com']


def test_team_stats_function_signature():
    """Test team stats function with default NFL.com site."""
    df = get_all_team_stats(years=None, sites='nfl.com', export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    assert 'year' in df.columns or df.shape[0] == 0
    # Check that source column is added
    if df.shape[0] > 0:
        assert 'source' in df.columns
        assert df['source'].unique().to_list() == ['NFL.com']


def test_backward_compatibility():
    """Test that old function calls still work (sites defaults to nfl.com)."""
    df = get_all_player_stats(years=None, export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    assert 'year' in df.columns or df.shape[0] == 0


def test_espn_player_stats():
    """Test ESPN scraper functionality."""
    # Limited test with just one recent week to avoid overloading ESPN
    df = get_all_player_stats(years=[2024], sites='espn.com', export=None, filename=None)
    assert isinstance(df, pl.DataFrame)
    # ESPN might return no data or might have data
    if df.shape[0] > 0:
        assert 'source' in df.columns
        assert 'week' in df.columns  # ESPN-specific column
        assert 'season_type' in df.columns  # ESPN-specific column
        assert all(source == 'ESPN.com' for source in df['source'].unique().to_list())
