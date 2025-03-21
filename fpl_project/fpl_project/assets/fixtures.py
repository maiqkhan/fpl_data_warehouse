from dagster import asset, AssetExecutionContext
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime as dt, date


def generate_season_str(first_game: dt, last_game: dt) -> str:
    """Generate a season string based on the first and last game years."""
    if first_game.year == last_game.year:
        return str(first_game.year)

    else:
        return f"{first_game.year}-{str(last_game.year)[2:]}"


def generate_event_type(
    gameweek: Optional[int],
    kickoff_time: Optional[dt],
) -> str:
    """Determine the event type based on gameweek and kickoff time."""
    if gameweek == 38:
        return "Final Day"
    elif kickoff_time is None:
        return "Not Scheduled"
    else:
        match kickoff_time:
            case fixture_dt if fixture_dt.day == 26 and fixture_dt.month == 12:
                return "Boxing Day"
            case fixture_dt if fixture_dt.weekday() == 0:
                return "Monday"
            case fixture_dt if fixture_dt.weekday() in [1, 2, 3]:
                return "Midweek"
            case fixture_dt if fixture_dt.weekday() == 4:
                return "Friday"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [
                11,
                12,
            ]:
                return "Saturday Lunch Hour"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [
                14,
                15,
            ]:
                return "Saturday 3:00 PM"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour > 15:
                return "Saturday Late Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour in [
                15,
                16,
            ]:
                return "Sunday Prime Time Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour < 15:
                return "Sunday Early Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour > 16:
                return "Sunday Late Game"
            case _:
                return "Other"


@asset(
    group_name="FIXTURES",
    description="""Pandas Dataframe with fixture data.""",
    kinds={"python", "pandas"},
)
def raw_fixture_df(raw_fixtures: List[Dict]) -> pd.DataFrame:
    """Convert raw fixture data into a Pandas DataFrame with parsed datetime fields."""
    df = pd.DataFrame.from_records(raw_fixtures)

    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], format="%Y-%m-%dT%H:%M:%SZ")

    return df


@asset(
    group_name="FIXTURES",
    description="""EPL season of the current fixture list.""",
    kinds={"python", "pandas"},
)
def epl_season(context: AssetExecutionContext, raw_fixture_df: pd.DataFrame) -> str:
    """Determine the EPL season string based on the first and last scheduled game."""
    scheduled_games = raw_fixture_df.query("kickoff_time.notnull()", engine="python")

    first_game = scheduled_games["kickoff_time"].min()
    last_game = scheduled_games["kickoff_time"].max()

    season = generate_season_str(first_game, last_game)
    context.log.info(f"{season}")

    return season

@asset(
    group_name="FIXTURES",
    description="""Pandas dataframe with derived features.""",
    kinds={"python", "pandas"},
)
def fixtures(
    context: AssetExecutionContext, raw_fixture_df: pd.DataFrame, epl_season: str
) -> pd.DataFrame:
    """Transform raw fixture data by adding event type, season, and derived keys."""
    fixture_df = raw_fixture_df.copy()

    fixture_df["fixture_type"] = fixture_df.apply(
        lambda x: generate_event_type(x["event"], x["kickoff_time"]), axis=1
    )

    fixture_df["event"] = fixture_df["event"].fillna(0)

    fixture_df["kickoff_time"] = fixture_df["kickoff_time"].fillna(
        dt(year=1900, month=1, day=1)
    )

    fixture_df["season"] = epl_season

    fixture_df = fixture_df.rename(columns={"id": "fixture_id"})

    fixture_df["fixture_key"] = fixture_df.apply(
        lambda x: int(f"{x['season'][:4]}{x['season'][5:]}{x['fixture_id']}"), axis=1
    )

    fixture_df["extract_dt"] = dt.today().date()

    context.log.info(fixture_df.isnull().any())

    return fixture_df[
        [
            "fixture_key",
            "fixture_id",
            "season",
            "event",
            "finished",
            "team_h",
            "team_a",
            "kickoff_time",
            "fixture_type",
            "extract_dt",
        ]
    ]
