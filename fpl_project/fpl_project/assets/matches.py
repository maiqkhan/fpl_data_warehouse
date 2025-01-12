from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
    ConfigurableResource,
)
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from fpl_project.fpl_project.resources.postgres import PostgresResource
from fpl_project.fpl_project.assets.staging import table_exists
from fpl_project.fpl_project.assets.models import dim_fixture, dim_player
from typing import Dict, List
import pandas as pd
from sqlalchemy import inspect, func, select, orm, text
import numpy as np


def get_recent_completed_matches(
    api_fixtures: List[int], db_fixtures: List[int]
) -> List[int]:
    if np.setdiff1d(db_fixtures, api_fixtures).size:
        raise Exception(
            f"There are finished fixtures in dim_fixture that are not flagged as finished in FPL: {np.setdiff1d(db_fixtures, api_fixtures)}"
        )
    else:
        return np.setdiff1d(api_fixtures, db_fixtures)


def get_player_match_history(
    player_id_lst: List[int], api_resource: FplAPI, context: AssetExecutionContext
) -> pd.DataFrame:
    player_match_lst = []
    for player in player_id_lst:
        context.log.info(f"Getting historical data for: {player}")
        payload = api_resource.get_request(endpoint=f"element-summary/{player}/").json()

        player_matches_df = pd.DataFrame.from_records(payload["history"])

        player_match_lst.append(player_matches_df)

    return pd.concat(player_match_lst)


@asset(
    group_name="MATCH",
    description="""All match statistics for a given player""",
    kinds={"python", "pandas"},
)
def incremental_finished_matches(
    context: AssetExecutionContext,
    fpl_api: FplAPI,
    players: pd.DataFrame,
    fpl_server: PostgresResource,
    fixtures: pd.DataFrame,
) -> pd.DataFrame:

    engine = fpl_server.connect_to_engine()
    api_fixtures_finished = fixtures.query("finished == True")

    if table_exists(engine, "fact_match_stats", "fpl"):
        context.log.info("The table already exists")

        with fpl_server.get_session() as session:
            fixture_query = select(dim_fixture.fixture_key).where(
                dim_fixture.finished_ind == True
            )

            db_fixtures_finished = [
                fixture for fixture in session.execute(fixture_query).scalars()
            ]

        recent_completed_fixtures = get_recent_completed_matches(
            api_fixtures_finished["fixture_key"].values, db_fixtures_finished
        )

        recent_completed_fixtures_df = api_fixtures_finished.query(
            "fixture_key in @recent_completed_fixtures"
        )

        fixtures_recently_played = recent_completed_fixtures_df[
            "fixture_id"
        ].values.tolist()

        teams_recently_played = (
            recent_completed_fixtures_df["team_h"].values.tolist()
            + recent_completed_fixtures_df["team_a"].values.tolist()
        )

        player_lst = players.query("team in @teams_recently_played")[
            "player_id"
        ].values.tolist()

        context.log.info(recent_completed_fixtures_df)
        context.log.info(teams_recently_played)
        context.log.info(player_lst)

        raw_match_stats_history = get_player_match_history(
            player_lst, fpl_api, context
        ).query("fixture in @fixtures_recently_played")

    else:
        context.log.info("The table does not exist")

        context.log.info(players.columns)

        player_match_lst = []
        for player in players["player_id"].unique():
            context.log.info(player)

            payload = fpl_api.get_request(endpoint=f"element-summary/{player}/").json()

            player_matches_df = pd.DataFrame.from_records(payload["history"])

            player_match_lst.append(player_matches_df)

        raw_match_stats_history = pd.concat(player_match_lst)

    match_stats_history = (
        raw_match_stats_history.drop("kickoff_time", axis=1)
        .merge(
            api_fixtures_finished,
            how="inner",
            left_on=["fixture"],
            right_on=["fixture_id"],
            validate="m:1",
        )
        .merge(
            players[["player_id", "first_name", "last_name", "web_name", "position"]],
            how="inner",
            left_on=["element"],
            right_on=["player_id"],
            validate="m:1",
        )
    )

    context.log.info(match_stats_history.columns)

    return match_stats_history


@asset(
    group_name="MATCH",
    description="""All match statistics for a given player""",
    kinds={"python", "pandas"},
)
def matches_df(
    context: AssetExecutionContext,
    incremental_finished_matches: pd.DataFrame,
) -> pd.DataFrame:

    drop_cols = [
        "fixture_key",
        "fixture",
        "opponent_team",
        "team_h_score",
        "team_a_score",
        "round",
        "modified",
        "event",
        "finished",
        "fixture_type",
        "element",
    ]

    match_stats = incremental_finished_matches.drop(drop_cols, axis=1)

    match_stats["team_id"] = match_stats.apply(
        lambda x: x["team_h"] if x["was_home"] else x["team_a"], axis=1
    )

    match_stats = match_stats.drop(columns=["team_h", "team_a"], axis=1)

    for col in [
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
    ]:
        match_stats[col] = match_stats[col].apply(float)

    context.log.info(match_stats.fixture_id.unique())

    return match_stats
