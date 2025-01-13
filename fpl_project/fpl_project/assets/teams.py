from dagster import asset, AssetExecutionContext
from typing import Dict, List
import pandas as pd
from datetime import datetime as dt


@asset(
    group_name="TEAMS",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def raw_teams_df(context: AssetExecutionContext, raw_teams: List[Dict]) -> pd.DataFrame:

    teams_df = pd.DataFrame.from_records(raw_teams)

    context.log.info(teams_df.columns)

    return teams_df


@asset(
    group_name="TEAMS",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python", "pandas"},
)
def teams(
    context: AssetExecutionContext, raw_teams_df: pd.DataFrame, epl_season: str
) -> pd.DataFrame:

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

    teams_output = teams_output.rename(columns={"id": "team_id"})

    teams_output["season"] = epl_season

    teams_output["team_key"] = teams_output.apply(
        lambda x: int(f"{x['season'][:4]}{x['season'][5:]}{x['team_id']}"), axis=1
    )

    teams_output["extract_dt"] = dt.today().date()

    return teams_output
