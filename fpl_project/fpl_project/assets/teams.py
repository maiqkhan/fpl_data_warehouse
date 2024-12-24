from dagster import asset, AssetExecutionContext
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime as dt


@asset(
    group_name="TEAMS",
    description="""Game data from FPL api bootstrap-static endpoint""",
)
def raw_teams_df(context: AssetExecutionContext, raw_teams: List[Dict]) -> pd.DataFrame:

    teams_df = pd.DataFrame.from_records(raw_teams)

    context.log.info(teams_df.columns)

    return teams_df


@asset(
    group_name="TEAMS",
    description="""Game data from FPL api bootstrap-static endpoint""",
)
def teams(raw_teams_df: pd.DataFrame, epl_season: str) -> pd.DataFrame:

    teams_output = raw_teams_df[
        [
            "id",
            "name",
            "short_name",
            "strength",
            "strength_overall_home",
            "strength_overall_away",
            "strength_attack_home",
            "strength_attack_away",
            "strength_defence_home",
            "strength_defence_away",
        ]
    ]

    teams_output["season"] = epl_season

    teams_output["extract_dt"] = dt.today().date()

    teams_output.to_csv(
        rf"C:\Users\khanm375\Documents\fpl_data_warehouse\data\teams_{dt.now().strftime('Y-%m-%d %H-%M-%S')}.csv",
        index=False,
    )

    return teams_output
