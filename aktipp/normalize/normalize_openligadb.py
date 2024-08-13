import json
import os
import polars as pl


def _check_season_openligadb_exists(league: str, season: str, data_path: str) -> bool:
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
    return os.path.isfile(data_path + f"{league}_{season}.json")


def normalize_season_openligadb(
    league: str,
    season: str,
    data_path: str,
    records: str = "matchResults",
    meta: str | list[str] = "all",
) -> None:
    """Normalize a season from json into a relational table and dump it as parquet.
    The openligadb json files currently contain two seperate lists of records. One
    list for 'matchResults' and one list for 'goals'. Both record lists can be
    normalized individually. By default the normalization includes all available
    meta data, but can be reduced to a subset.

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
    meta : str | list[str], default="all"
        Meta data to be used in normalization. "all" indicates all available meta data.
        Otherwise a list, e.g. ["matchID"] with desired meta data can be passed.
    """

    # validate records
    valid_records = ["matchResults", "goals"]
    if records not in valid_records:
        raise ValueError(f"{records} is not in {valid_records}.")

    # validate meta data
    valid_meta = [
        "matchID",
        "matchDateTime",
        "timeZoneID",
        "leagueId",
        "leagueName",
        "leagueSeason",
        "leagueShortcut",
        "matchDateTimeUTC",
        "group.groupName",
        "group.groupOrderID",
        "group.groupID",
        "team1.teamId",
        "team1.teamName",
        "team1.shortName",
        "team1.teamIconUrl",
        "team1.teamGroupName",
        "team2.teamId",
        "team2.teamName",
        "team2.shortName",
        "team2.teamIconUrl",
        "team2.teamGroupName",
        "lastUpdateDateTime",
        "matchIsFinished",
        "location",
        "numberOfViewers",
    ]

    if isinstance(meta, str):
        if meta == "all":
            meta = valid_meta
        else:
            raise ValueError("meta should be 'all' or subset of {valid_meta}")
    elif isinstance(meta, list):
        elements_valid = [element in valid_meta for element in meta]
        if not all(elements_valid):
            elements_invalid = meta[~elements_valid]
            raise ValueError(f"{elements_invalid} are not in {valid_meta}")
    else:
        raise ValueError("meta should be 'all' or subset of {valid_meta}")

    # read json data
    with open(data_path + f"{league}_{season}.json", "r") as file:
        data = json.load(file)

    # normalize data and dump as parquet file
    schema = {
        "matchID": pl.Int64,
        "matchDateTime": pl.String,
        "timeZoneID": pl.String,
        "leagueId": pl.Int64,
        "leagueName": pl.String,
        "leagueSeason": pl.Int64,
        "leagueShortcut": pl.String,
        "matchDateTimeUTC": pl.String,
        "group.groupName": pl.String,
        "group.groupOrderID": pl.Int64,
        "group.groupID": pl.Int64,
        "team1.teamId": pl.Int64,
        "team1.teamName": pl.String,
        "team1.shortName": pl.String,
        "team1.teamIconUrl": pl.String,
        "team1.teamGroupName": pl.String,
        "team2.teamId": pl.Int64,
        "team2.teamName": pl.String,
        "team2.shortName": pl.String,
        "team2.teamIconUrl": pl.String,
        "team2.teamGroupName": pl.String,
        "lastUpdateDateTime": pl.String,
        "matchIsFinished": pl.Int64,
        "location": pl.String,
        "numberOfViewers": pl.Int64,
        "matchResults": pl.List,
        "goals": pl.Unknown,
    }
    df = pl.json_normalize(data=data, schema=schema)

    if isinstance(df[records].explode().dtype, pl.Null):
        print(f"{league} {season} has no {records}. Only meta data.")
        df.select(meta).write_parquet(
            data_path + f"{league}_{season}_{records}.parquet"
        )
    else:
        record_keys = list(df[records].explode().dtype.to_schema().keys())
        df.explode(records).unnest(records).select(meta + record_keys).write_parquet(
            data_path + f"{league}_{season}_{records}.parquet"
        )


def normalize_many_seasons_openligadb(
    leagues: list[str],
    seasons: list[int],
    data_path: str,
    records: str = "matchResults",
    meta: str | list[str] = "all",
) -> None:
    """Normalize many seasons from json into a relational table and dump them as
    parquet.

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
            if _check_season_openligadb_exists(league, season, data_path):
                normalize_season_openligadb(league, season, data_path, records, meta)
                print(f"{league} {season} has been normalized.")
            else:
                print(f"{league} {season} is not available and will be skipped.")
