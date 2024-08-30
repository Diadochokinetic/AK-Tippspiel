import polars as pl

from ._team_based_views import _create_team_based_views


def _performance_openligadb(
    match_results_team1: pl.LazyFrame,
    match_results_team2: pl.LazyFrame,
    performance_class: str = "overall",
) -> pl.LazyFrame:
    """Calculate the performance statistics for all, home or away games.

    Parameters
    ----------
    match_results_team1 : pl.LazyFrame
        Match results for the home team.
    match_results_team12 : pl.LazyFrame
        Match results for the away team
    performance_class : str default='overall'
        "overall" - all games
        "home_away" - home_away games seperated

    Returns
    -------
    performance_statisticts : pl.LazyFrame
        LazyFrame with the calculated performance statistics.
    """

    # set grouping based on performance_class
    if performance_class == "home_away":
        group_vars = ["league_id", "team_id", "home_team"]
    elif performance_class == "overall":
        group_vars = ["league_id", "team_id"]
    else:
        raise ValueError("performance_class must be in ['home_away', 'overall']")

    return (
        pl.concat([match_results_team1, match_results_team2], how="vertical")
        .select(
            pl.col("match_id"),
            pl.col("league_id"),
            pl.col("match_day"),
            pl.col("team_id"),
            pl.col("team_name"),
            pl.col("home_team"),
            pl.col("wins").rolling_sum(3).over(group_vars, order_by="match_day").alias("wins_last_3_games"),
            pl.col("wins").rolling_sum(5).over(group_vars, order_by="match_day").alias("wins_last_5_games"),
            (
                pl.col("wins").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("wins_avg"),
            pl.col("draws").rolling_sum(3).over(group_vars, order_by="match_day").alias("draws_last_3_games"),
            pl.col("draws").rolling_sum(5).over(group_vars, order_by="match_day").alias("draws_last_5_games"),
            (
                pl.col("draws").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("draws_avg"),
            pl.col("losses").rolling_sum(3).over(group_vars, order_by="match_day").alias("losses_last_3_games"),
            pl.col("losses").rolling_sum(5).over(group_vars, order_by="match_day").alias("losses_last_5_games"),
            (
                pl.col("losses").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("losses_avg"),
            pl.col("points").rolling_sum(3).over(group_vars, order_by="match_day").alias("points_last_3_games"),
            pl.col("points").rolling_sum(5).over(group_vars, order_by="match_day").alias("points_last_5_games"),
            (
                pl.col("points").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("points_avg"),
            pl.col("goals_scored").rolling_sum(3).over(group_vars, order_by="match_day").alias("goals_scored_last_3_games"),
            pl.col("goals_scored").rolling_sum(5).over(group_vars, order_by="match_day").alias("goals_scored_last_5_games"),
            (
                pl.col("goals_scored").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("goals_scored_avg"),
            pl.col("goals_conceded").rolling_sum(3).over(group_vars, order_by="match_day").alias("goals_conceded_last_3_games"),
            pl.col("goals_conceded").rolling_sum(5).over(group_vars, order_by="match_day").alias("goals_conceded_last_5_games"),
            (
                pl.col("goals_conceded").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("goals_conceded_avg"),
            pl.col("goals_diff").rolling_sum(3).over(group_vars, order_by="match_day").alias("goals_diff_last_3_games"),
            pl.col("goals_diff").rolling_sum(5).over(group_vars, order_by="match_day").alias("goals_diff_last_5_games"),
            (
                pl.col("goals_diff").cum_sum().over(group_vars, order_by="match_day") \
                /pl.col("games").cum_sum().over(group_vars, order_by="match_day")
            ).alias("goals_diff_avg"),
        )
    )  # fmt: skip


def create_performance_openligadb(
    match_results_data_path: str,
    performance_data_path: str,
    performance_class: str = "overall",
) -> pl.LazyFrame:
    """Create a base table with an idiciator which team is the home team.

    Parameters
    ----------
    match_results_data_path : str
        Path to the clean match results parquet files.
    performance_data_path : str
        Path to the result file.
    performance_class : str default='overall'
        "overall" - all games
        "home_away" - home_away games seperated
    """

    match_results = pl.scan_parquet(match_results_data_path)

    # Filter match results
    # - Only consider final results
    # - Disregard relegation games
    # - Disregard games without a final result
    match_results_filtered = match_results.filter(
        (pl.col("result_name")=="Endergebnis")
        & (~pl.col("match_day_name").str.to_lowercase().str.contains("relegation"))
        & (pl.col("result_class").is_not_nan())
    )  # fmt: skip

    # Create team based views
    match_results_team1 = _create_team_based_views(
        match_results_filtered, team=1, standings_class="overall"
    )
    match_results_team2 = _create_team_based_views(
        match_results_filtered, team=2, standings_class="overall"
    )

    _performance_openligadb(
        match_results_team1, match_results_team2, performance_class
    ).collect().write_parquet(performance_data_path)
