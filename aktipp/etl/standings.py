import polars as pl

from ._team_based_views import _create_team_based_views


def _create_standings_openligadb(
    match_results_team1: pl.LazyFrame, match_results_team2: pl.LazyFrame
) -> pl.DataFrame:
    """Aggregations for the standings.

    Parameters
    ----------
    match_results_team1 : pl.LazyFrame
        Match results for the home team.
    match_results_team12 : pl.LazyFrame
        Match results for the away team

    Returns
    -------
    standings : pl.DataFrame
        DataFrame with the aggreageted standings.
    """

    return (
        pl.concat([match_results_team1, match_results_team2], how="vertical")
        .select(
            pl.col("league_id"),
            pl.col("league_name"),
            pl.col("season_name"),
            pl.col("match_day"),
            pl.col("team_id"),
            pl.col("team_name"),
            pl.col("games")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("games"),
            pl.col("wins")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("wins"),
            pl.col("draws")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("draws"),
            pl.col("losses")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("losses"),
            pl.col("goals_scored")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("goals_scored"),
            pl.col("goals_conceded")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("goals_conceded"),
            pl.col("goals_diff")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("goals_diff"),
            pl.col("points")
            .cum_sum()
            .over(["league_id", "team_id"], order_by="match_day")
            .alias("points"),
        )
        .with_columns(
            pl.struct(["points", "goals_diff", "goals_scored"])
            .rank(descending=True, method="min")
            .over(["league_id", "match_day"])
            .alias("rank")
        )
        .collect()  # fmt: skip
    )


def create_standings_openligadb(
    match_results_data_path: str,
    standings_data_path: str,
    standings_class: str = "overall",
) -> None:
    """Create a history of all standings based on the openligadb match results.
    - Only consider final results
    - Disregard relegation games
    - Disregard games without a final result

    Parameters
    ----------
    match_results_data_path : str
        Path to the clean match results parquet files.
    standings_data_path : str
        Path to the result file.
    standings_class : str, default='overall'
        "overall" - the KPIs will be generated for both teams.
        "home" - the KPIs will only be generated for the home team.
        "away" - the KPIs will only be generated for the away team.
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
        match_results_filtered, team=1, standings_class=standings_class
    )
    match_results_team2 = _create_team_based_views(
        match_results_filtered, team=2, standings_class=standings_class
    )

    # Create standings
    _create_standings_openligadb(
        match_results_team1, match_results_team2
    ).write_parquet(standings_data_path)
