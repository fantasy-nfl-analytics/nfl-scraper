from nfl_webscraper import get_all_player_stats

df = get_all_player_stats(years=None, sites='espn.com', export='parquet', filename='player_stats.parquet')

print(df.head(10))