import polars as pl


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


def _create_team_based_views(
    match_results: pl.LazyFrame, team: int, standings_class: str
) -> pl.LazyFrame:
    """Create team based views.

    Parameters
    ----------
    match_results : pl.LazyFrame
        LazyFrame with the match results to be turned in a team based view.
    team : int
        1 for team_1 and 2 for team_2
    standings_class : str
        "overall" - the KPIs will be generated for both teams.
        "home" - the KPIs will only be generated for the home team.
        "away" - the KPIs will only be generated for the away team.

    Returns
    -------
    team_based_view : pl.LazyFrame
        LazyFrame with the team based view.
    """

    # validate team
    if team not in [1, 2]:
        raise ValueError("team must bei in [1, 2]")

    # validate standings_class
    if standings_class not in ["home", "away", "overall"]:
        raise ValueError("standing_class must be in ['home', 'away', 'overall']")

    # create team based views
    if team == 1:
        if standings_class in ["home", "overall"]:
            return (
                match_results.select(
                    pl.col("league_id"),
                    pl.col("league_name"),
                    pl.col("season_name"),
                    pl.col("match_day"),
                    pl.col("team_id_1").alias("team_id"),
                    pl.col("team_name_1").alias("team_name"),
                    pl.lit(1).alias("games"),
                    pl.when(pl.col("result_class") == 1)
                    .then(1)
                    .otherwise(0)
                    .alias("wins"),
                    pl.when(pl.col("result_class") == 0)
                    .then(1)
                    .otherwise(0)
                    .alias("draws"),
                    pl.when(pl.col("result_class") == -1)
                    .then(1)
                    .otherwise(0)
                    .alias("losses"),
                    pl.col("goals_team_1").alias("goals_scored"),
                    pl.col("goals_team_2").alias("goals_conceded"),
                    pl.col("goals_diff"),
                    pl.col("points_team_1").alias("points"),
                )  # fmt: skip
            )
        else:
            return (
                match_results.select(
                    pl.col("league_id"),
                    pl.col("league_name"),
                    pl.col("season_name"),
                    pl.col("match_day"),
                    pl.col("team_id_1").alias("team_id"),
                    pl.col("team_name_1").alias("team_name"),
                    pl.lit(0, dtype=pl.Int32).alias("games"),
                    pl.lit(0, dtype=pl.Int32).alias("wins"),
                    pl.lit(0, dtype=pl.Int32).alias("draws"),
                    pl.lit(0, dtype=pl.Int32).alias("losses"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_scored"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_conceded"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_diff"),
                    pl.lit(0, dtype=pl.Int32).alias("points"),
                )  # fmt: skip
            )
    else:
        if standings_class in ["away", "overall"]:
            return (
                match_results.select(
                    pl.col("league_id"),
                    pl.col("league_name"),
                    pl.col("season_name"),
                    pl.col("match_day"),
                    pl.col("team_id_2").alias("team_id"),
                    pl.col("team_name_2").alias("team_name"),
                    pl.lit(1).alias("games"),
                    pl.when(pl.col("result_class") == -1)
                    .then(1)
                    .otherwise(0)
                    .alias("wins"),
                    pl.when(pl.col("result_class") == 0)
                    .then(1)
                    .otherwise(0)
                    .alias("draws"),
                    pl.when(pl.col("result_class") == 1)
                    .then(1)
                    .otherwise(0)
                    .alias("losses"),
                    pl.col("goals_team_2").alias("goals_scored"),
                    pl.col("goals_team_1").alias("goals_conceded"),
                    (-1 * pl.col("goals_diff")).alias("goals_diff"),
                    pl.col("points_team_2").alias("points"),
                )  # fmt: skip
            )
        else:
            return (
                match_results.select(
                    pl.col("league_id"),
                    pl.col("league_name"),
                    pl.col("season_name"),
                    pl.col("match_day"),
                    pl.col("team_id_2").alias("team_id"),
                    pl.col("team_name_2").alias("team_name"),
                    pl.lit(0, dtype=pl.Int32).alias("games"),
                    pl.lit(0, dtype=pl.Int32).alias("wins"),
                    pl.lit(0, dtype=pl.Int32).alias("draws"),
                    pl.lit(0, dtype=pl.Int32).alias("losses"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_scored"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_conceded"),
                    pl.lit(0, dtype=pl.Int64).alias("goals_diff"),
                    pl.lit(0, dtype=pl.Int32).alias("points"),
                )  # fmt: skipaway
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
    standings_class : str
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
