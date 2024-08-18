from importlib import resources
import polars as pl

from . import feature_store
from . import mapper


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


def clean_openligadb(
    data_path: str, records: str, features: list[str] = DEFAULT_FEATURES
) -> None:
    """Clean up all openligadb files for one type of record data into a single parquet
    file. Clean up consists of:
    - Resolving ambigious entities (teams and leagues)
    - Renaming to fit a lowercase naming scheme
    - Renaming to resolve ambigious termionology (points and goals)

    Parameters
    ----------
    data_path : str
        Path where the data should be read from json and dumped as normalized parquet.
    records : str, default="matchResults"
        List of the records to be normalized.
    features : list[str], default=DEFAULT_FEATURES
        List of features to be used in cleaned up data set.
    """

    # Lazy load data
    # leagues 50 & 4570 are incomplete and should be disregarded
    records_data = pl.scan_parquet(data_path + f"*{records}.parquet").filter(
        ~pl.col("leagueId").is_in([50, 4570])
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
    .sink_parquet(data_path + f"{records}_clean.parquet")  # fmt: skip
