import polars as pl


def _goals_team_1():
    return pl.coalesce(pl.col("pointsTeam1"), 0).alias("goals_team_1")


def _goals_team_2():
    return pl.coalesce(pl.col("pointsTeam2"), 0).alias("goals_team_2")


def _goals_diff():
    return pl.coalesce(pl.col("pointsTeam1") - pl.col("pointsTeam2"), 0).alias(
        "goals_diff"
    )


def _league_id():
    return pl.col("leagueId").alias("league_id")


def _league_name():
    return pl.col("league_name_unique").alias("league_name")


def _league_name_raw():
    return pl.col("leagueName").str.head(-10).alias("league_name_raw")


def _match_day():
    return pl.col("group.groupOrderID").alias("match_day")


def _match_day_name():
    return pl.col("group.groupName").alias("match_day_name")


def _match_id():
    return pl.col("matchID").alias("match_id")


def _points_team_1():
    return (
        pl.when(pl.col("pointsTeam1") > pl.col("pointsTeam2"))
        .then(3)
        .when(pl.col("pointsTeam1") == pl.col("pointsTeam2"))
        .then(1)
        .when(pl.col("pointsTeam1") < pl.col("pointsTeam2"))
        .then(0)
        .otherwise(None)
        .alias("points_team_1")  # fmt: skip
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
        .alias("points_team_2")  # fmt: skip
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
        .alias("result_class")  # fmt: skip
    )


def _result_name():
    return pl.col("resultName").alias("result_name")


def _season_name():
    return pl.col("leagueName").str.tail(9).alias("season_name")


def _team_id_1():
    return pl.col("team_id_unique_1").alias("team_id_1")


def _team_id_2():
    return pl.col("team_id_unique_2").alias("team_id_2")


def _team_name_1():
    return pl.col("team1.teamName").alias("team_name_1")


def _team_name_2():
    return pl.col("team2.teamName").alias("team_name_2")
