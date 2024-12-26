from dagster import asset, AssetExecutionContext
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime as dt, date


def generate_season_str(first_game: dt, last_game: dt) -> str:
    if first_game.year == last_game.year:
        return str(first_game.year)

    else:
        return f"{first_game.year}-{str(last_game.year)[2:]}"


def generate_event_type(
    gameweek: Optional[int],
    kickoff_time: Optional[dt],
) -> str:
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
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def raw_fixture_df(raw_fixtures: List[Dict]) -> pd.DataFrame:

    df = pd.DataFrame.from_records(raw_fixtures)

    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], format="%Y-%m-%dT%H:%M:%SZ")

    return df


@asset(
    group_name="FIXTURES",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def epl_season(context: AssetExecutionContext, raw_fixture_df: pd.DataFrame) -> str:

    scheduled_games = raw_fixture_df.query("kickoff_time.notnull()", engine="python")

    first_game = scheduled_games["kickoff_time"].min()
    last_game = scheduled_games["kickoff_time"].max()

    season = generate_season_str(first_game, last_game)
    context.log.info(f"{season}")

    return season


@asset(
    group_name="FIXTURES",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def first_fixture_date(raw_fixture_df: pd.DataFrame) -> date:

    return (
        raw_fixture_df.query("kickoff_time.notnull()", engine="python")["kickoff_time"]
        .min()
        .date()
    )


@asset(
    group_name="FIXTURES",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def fixtures(
    context: AssetExecutionContext, raw_fixture_df: pd.DataFrame, epl_season: str
) -> pd.DataFrame:

    fixture_df = raw_fixture_df.copy()

    fixture_df["fixture_type"] = fixture_df.apply(
        lambda x: generate_event_type(x["event"], x["kickoff_time"]), axis=1
    )

    fixture_df["season"] = epl_season

    fixture_df["extract_dt"] = dt.today().date()

    context.log.info(fixture_df["fixture_type"].unique())
    context.log.info(fixture_df.columns)

    fixture_df.to_csv(
        rf"C:\Users\khanm375\Documents\fpl_data_warehouse\data\fixtures_{dt.now().strftime('Y-%m-%d %H-%M-%S')}.csv",
        index=False,
    )

    return fixture_df[
        [
            "id",
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
