import polars as pl

from ._helper import _suffix_alias


def _load_match_results(match_results_data_path: str) -> pl.LazyFrame:
    """Load match results.

    Parameters
    ----------
    match_results_data_path : str
        Path to the clean match results parquet files.

    Returns
    -------
    match_results: pl.LazyFrame
        match_results.
    """

    return pl.scan_parquet(match_results_data_path)


def _goals_scored_based_view(match_results: pl.LazyFrame) -> pl.LazyFrame:
    """Create a table with goals scored as target variable. Start with the regular
    match notation team1 = home, duplicate and reverse for all the away teams.

    Parameters
    ----------
    match_results : pl.LazyFrame
        LazyFrame with the match_results.

    Returns
    -------
    goals_scored_based_view : pl.LazyFrame
        Table with goales scored as target variable.
    """

    goals_scored_home_team = match_results.select(
        pl.col("match_id"),
        pl.col("league_id"),
        pl.col("match_day"),
        pl.col("team_id_1"),
        pl.col("team_name_1"),
        pl.col("team_id_2"),
        pl.col("team_name_2"),
        pl.lit(1).alias("home_team"),
        pl.col("goals_team_1").alias("goals"),
    )

    goals_scored_away_team = match_results.select(
        pl.col("match_id"),
        pl.col("league_id"),
        pl.col("match_day"),
        pl.col("team_id_2").alias("team_id_1"),
        pl.col("team_name_2").alias("team_name_1"),
        pl.col("team_id_1").alias("team_id_2"),
        pl.col("team_name_1").alias("team_name_2"),
        pl.lit(0).alias("home_team"),
        pl.col("goals_team_2").alias("goals"),
    )

    return pl.concat([goals_scored_home_team, goals_scored_away_team], how="vertical")


def _add_overall_standings(
    base: pl.LazyFrame, standings_data_path: str
) -> pl.LazyFrame:
    """Add number of games and rank from previous match day.

    Parameters
    ----------
    base : pl.Lazyframe
        Base Frame.
    standings_data_path : str
        Path to the standings parquet file.

    Returns
    -------
    result : pl.LazyFrame
        Result frame with overall standings data.

    """

    standings = pl.scan_parquet(standings_data_path)

    standings_select_columns = ["team_id", "games", "rank"]

    standings_team_1 = standings.select(
        pl.col("league_id"),
        (pl.col("match_day") - 1).alias("match_day"),
        _suffix_alias(standings_select_columns, suffix="_1"),
    )

    standings_team_2 = standings.select(
        pl.col("league_id"),
        (pl.col("match_day") - 1).alias("match_day"),
        _suffix_alias(standings_select_columns, suffix="_2"),
    )

    return base.join(
        other=standings_team_1, on=["league_id", "team_id_1", "match_day"], how="left"
    ).join(
        other=standings_team_2, on=["league_id", "team_id_2", "match_day"], how="left"
    )


def _add_overall_performance(
    base: pl.LazyFrame, performance_data_path: str
) -> pl.LazyFrame:
    """Add overall performance KPIs from previous match day.

    Parameters
    ----------
    base : pl.Lazyframe
        Base Frame.
    performance_data_path : str
        Path to the performance parquet file.

    Returns
    -------
    result : pl.LazyFrame
        Result frame with overall performance data.

    """

    performance = pl.scan_parquet(performance_data_path)

    performance_select_columns = [
        "team_id",
        "wins_last_3_games",
        "wins_last_5_games",
        "wins_avg",
        "draws_last_3_games",
        "draws_last_5_games",
        "draws_avg",
        "losses_last_3_games",
        "losses_last_5_games",
        "losses_avg",
        "points_last_3_games",
        "points_last_5_games",
        "points_avg",
        "goals_scored_last_3_games",
        "goals_scored_last_5_games",
        "goals_scored_avg",
        "goals_conceded_last_3_games",
        "goals_conceded_last_5_games",
        "goals_conceded_avg",
        "goals_diff_last_3_games",
        "goals_diff_last_5_games",
        "goals_diff_avg",
    ]

    performance_team_1 = performance.select(
        pl.col("league_id"),
        (pl.col("match_day") - 1).alias("match_day"),
        _suffix_alias(performance_select_columns, suffix="_1"),
    )

    performance_team_2 = performance.select(
        pl.col("league_id"),
        (pl.col("match_day") - 1).alias("match_day"),
        _suffix_alias(performance_select_columns, suffix="_2"),
    )

    return base.join(
        other=performance_team_1, on=["league_id", "team_id_1", "match_day"], how="left"
    ).join(
        other=performance_team_2, on=["league_id", "team_id_2", "match_day"], how="left"
    )
