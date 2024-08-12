import json
import urllib.request


def _check_season_openligadb_exists(league: str, season: int) -> bool:
    """Check if a league season combination as available at openligadb.

    Parameters
    ----------
    league : str
        String identifier from the league, e.g. 'bl1' for 1. Bundesliga. A complete
        list can be retrieved from https://api.openligadb.de/getavailableleagues.
    season : int
        Year indicating the start of a season, e.g. 2023 for the 2023/2024 season.

    Returns
    -------
    result : bool
        True if league season combination is available.
    """
    # retrieve all available leagues
    contents = urllib.request.urlopen(
        "https://api.openligadb.de/getavailableleagues"
    ).read()
    available_leagues = json.loads(contents)

    # parse league season combinations into a list of tuples (league, season)
    league_season_list = []
    for league_season in available_leagues:
        league_season_list.append(
            (league_season["leagueShortcut"], int(league_season["leagueSeason"]))
        )

    # check if league season combination is available
    return (league, season) in league_season_list


def scrape_season_openligadb(league: str, season: int, data_path: str) -> None:
    """Load all games from one season of a league from openligadb and dump it as json.
    The dumped file will named 'league_season.json'.

    Parameters
    ----------
    league : str
        String identifier from the league, e.g. 'bl1' for 1. Bundesliga. A complete
        list can be retrieved from https://api.openligadb.de/getavailableleagues.
    season : int
        Year indicating the start of a season, e.g. 2023 for the 2023/2024 season.
    data_path : str
        Path where the data should be dumped as json.
    """

    # read data from openligadb and parse as json
    contents = urllib.request.urlopen(
        f"http://www.openligadb.de/api/getmatchdata/{league}/{season}"
    ).read()
    data = json.loads(contents)

    # dump data as json
    with open(f"{data_path}{league}_{season}.json", "w") as file:
        json.dump(data, file)


def scrape_many_seasons_openligadb(
    leagues: list[str], seasons: list[int], data_path: str
) -> None:
    """Load all games from many seasons of many leagues from openligadb. Dump the
    individual combinations of league and season as json named like
    'league_season.json'.

    Parameters
    ----------
    leagues: list[str]
        List of string identifiers, e.g. ['bl1', 'bl2']. A complete list of possible
        values can be retrieved from https://api.openligadb.de/getavailableleagues.
    seasons: list[int]
        List of years for multiple seasons.
    data_path : str
        Path where the data should be dumped as json.
    """

    for league in leagues:
        for season in seasons:
            if _check_season_openligadb_exists(league, season):
                scrape_season_openligadb(league, season, data_path)
                print(f"{league} {season} has been loaded.")
            else:
                print(f"{league} {season} is not available and will be skipped.")
