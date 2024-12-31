from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
)
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from typing import Dict, List
import pandas as pd


@asset(
    group_name="INITIAL_LOAD",
    description="""All match statistics for a given player""",
    kinds={"python", "pandas"},
)
def raw_matches_df(
    context: AssetExecutionContext, fpl_api: FplAPI, players: pd.DataFrame
) -> pd.DataFrame:

    player_match_lst = []
    for player in players["id"].unique():
        context.log.info(player)

        payload = fpl_api.get_request(endpoint=f"element-summary/{player}/").json()

        player_matches_df = pd.DataFrame.from_records(payload["history"])

        player_match_lst.append(player_matches_df)

    match_stats_history = pd.concat(player_match_lst)

    return match_stats_history


@asset(
    group_name="INITIAL_LOAD",
    description="""All match statistics for a given player""",
    kinds={"python", "pandas"},
)
def matches_df(
    context: AssetExecutionContext,
    raw_matches_df: pd.DataFrame,
    fixtures: pd.DataFrame,
) -> pd.DataFrame:

    # context.log.info(raw_matches_df.columns)

    drop_cols = [
        "fixture_key",
        "fixture",
        "opponent_team",
        "team_h_score",
        "team_a_score",
        "round",
        "modified",
        "season",
        "event",
        "finished",
        "fixture_type",
        "extract_dt",
    ]

    match_stats = (
        raw_matches_df.drop("kickoff_time", axis=1)
        .merge(
            fixtures,
            how="left",
            left_on=["fixture"],
            right_on=["fixture_id"],
            validate="m:1",
        )
        .query("finished == True")
        .drop(drop_cols, axis=1)
    )

    match_stats["team_id"] = match_stats.apply(
        lambda x: x["team_h"] if x["was_home"] else x["team_a"], axis=1
    )

    match_stats = match_stats.drop(columns=["team_h", "team_a"], axis=1)

    return match_stats
