import polars as pl


def create_overall_standings_openligadb(
    match_results_data_path: str, standings_data_path: str
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
    match_results_team1 = match_results_filtered.select(
        pl.col("league_id"),
        pl.col("league_name"),
        pl.col("season_name"),
        pl.col("match_day"),
        pl.col("team_id_1").alias("team_id"),
        pl.col("team_name_1").alias("team_name"),
        pl.when(pl.col("result_class").is_not_nan()).then(1).otherwise(0).alias("games"),
        pl.when(pl.col("result_class")==1).then(1).otherwise(0).alias("wins"),
        pl.when(pl.col("result_class")==0).then(1).otherwise(0).alias("draws"),
        pl.when(pl.col("result_class")==-1).then(1).otherwise(0).alias("losses"),
        pl.col("goals_team_1").alias("goals_scored"),
        pl.col("goals_team_2").alias("goals_conceded"),
        pl.col("goals_diff"),
        pl.col("points_team_1").alias("points")
    )  # fmt: skip

    match_results_team2 = match_results_filtered.select(
        pl.col("league_id"),
        pl.col("league_name"),
        pl.col("season_name"),
        pl.col("match_day"),
        pl.col("team_id_2").alias("team_id"),
        pl.col("team_name_2").alias("team_name"),
        pl.when(pl.col("result_class").is_not_nan()).then(1).otherwise(0).alias("games"),
        pl.when(pl.col("result_class")==-1).then(1).otherwise(0).alias("wins"),
        pl.when(pl.col("result_class")==0).then(1).otherwise(0).alias("draws"),
        pl.when(pl.col("result_class")==1).then(1).otherwise(0).alias("losses"),
        pl.col("goals_team_2").alias("goals_scored"),
        pl.col("goals_team_1").alias("goals_conceded"),
        (-1*pl.col("goals_diff")).alias("goals_diff"),
        pl.col("points_team_2").alias("points")
    )  # fmt: skip

    # Create table
    pl.concat([match_results_team1, match_results_team2], how="vertical") \
    .select(
        pl.col("league_id"),
        pl.col("league_name"),
        pl.col("season_name"),
        pl.col("match_day"),
        pl.col("team_id"),
        pl.col("team_name"),
        pl.col("games").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("games"),
        pl.col("wins").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("wins"),
        pl.col("draws").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("draws"),
        pl.col("losses").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("losses"),
        pl.col("goals_scored").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("goals_scored"),
        pl.col("goals_conceded").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("goals_conceded"),
        pl.col("goals_diff").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("goals_diff"),
        pl.col("points").cum_sum().over(["league_id", "team_id"], order_by="match_day").alias("points"),
    ) \
    .with_columns(
        pl.struct(["points", "goals_diff", "goals_scored"]) \
        .rank(descending=True, method="min") \
        .over(["league_id", "match_day"]) \
        .alias("rank")
    ) \
    .collect().write_parquet(standings_data_path)  # fmt: skip
