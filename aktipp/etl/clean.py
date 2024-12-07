from importlib import resources
import os
import polars as pl

from . import feature_store
from . import mapper


def _check_season_openligadb_exists(
    league: str, season: str, data_path: str, records: str
) -> bool:
    """Check if a league season combination as available as json.

    Parameters
    ----------
    league : str
        String identifier from the league, e.g. 'bl1' for 1. Bundesliga. A complete
        list can be retrieved from https://api.openligadb.de/getavailableleagues.
    season : int
        Year indicating the start of a season, e.g. 2023 for the 2023/2024 season.
    data_path : str
        Path where the data should be available as json.

    Returns
    -------
    result : bool
        True if league season combination is available.
    """
    return os.path.isfile(
        f"{data_path}/normalized_data/{league}_{season}_{records}.parquet"
    )


def _build_features(features: list[str]) -> list[pl.Expr]:
    """Build multiple features based on a passed list.

    Parameters
    ----------
    features : list[str]
        List of feature names.

    Returns
    -------
    built_features : list[pl.Expr]
        List of polars expressions to build the features.
    """
    built_features = []
    for feature in features:
        fname = f"_{feature}"
        if hasattr(feature_store, fname):
            built_features.append(getattr(feature_store, fname)())
        else:
            print(f"There is no aggregation for {feature}.")
    return built_features


DEFAULT_FEATURES = [
    "match_id",
    "league_id",
    "league_name",
    "season_name",
    "match_day",
    "match_day_name",
    "team_id_1",
    "team_id_2",
    "team_name_1",
    "team_name_2",
    "goals_team_1",
    "goals_team_2",
    "goals_diff",
    "result_class",
    "result_name",
    "points_team_1",
    "points_team_2",
]


def clean_season_openligadb(
    league: str,
    season: str,
    data_path: str,
    records: str = "matchResults",
    features: list[str] = DEFAULT_FEATURES,
) -> None:
    """Clean up openligadb season files for one type of record data as parquet
    file. Clean up consists of:
    - Resolving ambigious entities (teams and leagues)
    - Renaming to fit a lowercase naming scheme
    - Renaming to resolve ambigious termionology (points and goals)

    Parameters
    ----------
    league : str
        String identifier from the league, e.g. 'bl1' for 1. Bundesliga. A complete
        list can be retrieved from https://api.openligadb.de/getavailableleagues.
    season : int
        Year indicating the start of a season, e.g. 2023 for the 2023/2024 season.
    data_path : str
        Path where the data should be read from json and dumped as normalized parquet.
    records : str, default="matchResults"
        List of the records to be normalized.
    features : list[str], default=DEFAULT_FEATURES
        List of features to be used in cleaned up data set.
    """

    # Lazy load data
    records_data = pl.scan_parquet(
        f"{data_path}/normalized_data/{league}_{season}_{records}.parquet"
    )

    # Lazy load mappers
    league_mapper_path = resources.files(mapper) / "league_mapper.csv"
    league_mapper = pl.scan_csv(league_mapper_path)

    team_mapper_path = resources.files(mapper) / "team_mapper.csv"
    team_mapper = pl.scan_csv(team_mapper_path)

    records_data.with_columns(feature_store._league_name_raw()) \
    .join(other=league_mapper, on="league_name_raw", how="left") \
    .join(
        other=team_mapper.select(["team_id_raw", "team_id_unique"]).rename(
            {"team_id_unique": "team_id_unique_1"}
        ),
        left_on="team1.teamId",
        right_on="team_id_raw",
        how="left",
    ) \
    .join(
        other=team_mapper.select(["team_id_raw", "team_id_unique"]).rename(
            {"team_id_unique": "team_id_unique_2"}
        ),
        left_on="team2.teamId",
        right_on="team_id_raw",
        how="left",
    ) \
    .select(*_build_features(features)) \
    .sink_parquet(f"{data_path}/cleaned_data/{league}_{season}_{records}_clean.parquet")  # fmt: skip


def clean_many_seasons_openligadb(
    leagues: list[str],
    seasons: list[int],
    data_path: str,
    records: str = "matchResults",
) -> None:
    """Clean many seasons for one type of record data as parquet
    file. Clean up consists of:
    - Resolving ambigious entities (teams and leagues)
    - Renaming to fit a lowercase naming scheme
    - Renaming to resolve ambigious termionology (points and goals)

    Parameters
    ----------
    leagues: list[str]
        List of string identifiers, e.g. ['bl1', 'bl2']. A complete list of possible
        values can be retrieved from https://api.openligadb.de/getavailableleagues.
    seasons: list[int]
        List of years for multiple seasons.
    data_path : str
        Path where the data should be read from json and dumped as normalized parquet.
    records : str, default="matchResults"
        List of the records to be normalized.
    meta : str | list[str], default="all"
        Meta data to be used in normalization. "all" indicates all available meta data.
        Otherwise a list, e.g. ["matchID"] with desired meta data can be passed.
    """

    for league in leagues:
        for season in seasons:
            if _check_season_openligadb_exists(league, season, data_path, records):
                clean_season_openligadb(league, season, data_path, records)
                print(f"{league} {season} has been normalized.")
            else:
                print(f"{league} {season} is not available and will be skipped.")
