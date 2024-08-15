import polars as pl


def _goals_team_1():
    return pl.col("pointsTeam1").alias("goals_team_1")


def _goals_team_2():
    return pl.col("pointsTeam2").alias("goals_team_2")


def _goals_diff():
    return (pl.col("pointsTeam1") - pl.col("pointsTeam2")).alias("goals_team_2")


def _league_name_raw():
    return pl.col("leagueName").str.head(-10).alias("league_name_raw")


def _points_team_1():
    return (
        pl.when(pl.col("pointsTeam1") > pl.col("pointsTeam2"))
        .then(3)
        .when(pl.col("pointsTeam1") == pl.col("pointsTeam2"))
        .then(1)
        .when(pl.col("pointsTeam1") < pl.col("pointsTeam2"))
        .then(0)
        .otherwise(None)
        .alias("points_team_1")  # fmt: ignore
    )


def _points_team_2():
    return (
        pl.when(pl.col("pointsTeam1") > pl.col("pointsTeam2"))
        .then(0)
        .when(pl.col("pointsTeam1") == pl.col("pointsTeam2"))
        .then(1)
        .when(pl.col("pointsTeam1") < pl.col("pointsTeam2"))
        .then(3)
        .otherwise(None)
        .alias("points_team_2")  # fmt: ignore
    )


def _result_class():
    return (
        pl.when(pl.col("pointsTeam1") > pl.col("pointsTeam2"))
        .then(1)
        .when(pl.col("pointsTeam1") == pl.col("pointsTeam2"))
        .then(0)
        .when(pl.col("pointsTeam1") < pl.col("pointsTeam2"))
        .then(-1)
        .otherwise(None)
        .alias("result_class")  # fmt: ignore
    )


def _season_name():
    return pl.col("leagueName").str.tail(9).alias("season_name")
