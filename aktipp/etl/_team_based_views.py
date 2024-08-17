import polars as pl


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
                    pl.lit(1).alias("home_team"),
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
                    pl.lit(1).alias("home_team"),
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
                    pl.lit(0).alias("home_team"),
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
                    pl.lit(0).alias("home_team"),
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
