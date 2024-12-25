from dagster import asset, AssetExecutionContext
import pandas as pd
from fpl_project.fpl_project.resources.postgres import PostgresResource
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from fpl_project.fpl_project.assets.raw import raw_bootstrap
from typing import Dict, List
from datetime import datetime as dt, timedelta as tmdelta


@asset(
    group_name="PLAYER",
    description="Player payload from ",
    kinds={"python", "pandas"},
)
def raw_player_df(
    context: AssetExecutionContext, raw_players: List[Dict]
) -> pd.DataFrame:

    raw_df = pd.DataFrame.from_records(raw_players)

    keep_cols = [
        "id",
        "first_name",
        "second_name",
        "web_name",
        "element_type",
        "now_cost",
        "team",
    ]

    return raw_df[keep_cols]


@asset(
    group_name="PLAYER",
    description="Player payload from ",
    kinds={"python", "pandas"},
)
def players(
    context: AssetExecutionContext, raw_player_df: pd.DataFrame, epl_season: str
) -> pd.DataFrame:

    player_dict = {1: "Goalkeeper", 2: "Defender", 3: "Midfielder", 4: "Forward"}

    raw_player_df["position"] = raw_player_df["element_type"].apply(player_dict.get)

    raw_player_df = raw_player_df.drop("element_type", axis=1)

    raw_player_df["season"] = epl_season

    context.log.info(raw_player_df.columns)

    raw_player_df.to_csv(
        rf"C:\Users\khanm375\Documents\fpl_data_warehouse\data\players_{dt.now().strftime('Y-%m-%d %H-%M-%S')}.csv",
        index=False,
    )

    return raw_player_df


@asset(
    group_name="PLAYER_CHECK",
    description="Current player data from fpl.dim_player table",
)
def db_player_data(fpl_server: PostgresResource) -> pd.DataFrame:

    engine = fpl_server.connect_to_engine()

    with engine.connect() as connection:
        current_players_df = pd.read_sql(
            """select player_id
                    ,d_player.season
                    ,d_player.first_name
                    ,d_player.last_name
                    ,d_player.web_name
                    ,d_player."position"
                    ,d_player.price
                    ,d_team.team_id
                    ,d_player.effective_dt
                    ,d_player.expiry_dt
                    ,d_player.current_ind
                from fpl.dim_player as d_player 
                left join fpl.dim_team as d_team on d_player.team_key = d_team.team_key
                   where d_player.current_ind = True""",
            connection,
        )

    return current_players_df


@asset(
    group_name="PLAYER_CHECK",
    description="""Current player data from FPL api bootstrap-static endpoint""",
)
def api_player_data(raw_bootstrap: Dict) -> pd.DataFrame:

    raw_api_player_data = pd.DataFrame.from_records(raw_bootstrap["elements"])

    player_dict = {1: "Goalkeeper", 2: "Defender", 3: "Midfielder", 4: "Forward"}

    # keep only relevant columns from api endpoint payload
    player_data_df = raw_api_player_data[
        [
            "id",
            "first_name",
            "second_name",
            "web_name",
            "element_type",
            "now_cost",
            "team",
        ]
    ]

    # remap element type to valid position name
    player_data_df["position"] = player_data_df["element_type"].apply(player_dict.get)

    # remap price to float value
    player_data_df["price"] = player_data_df["now_cost"].apply(lambda x: x / 10)

    # Drop element type column
    player_data_df = player_data_df.drop(["element_type", "now_cost"], axis=1)

    # remap column names to match fpl.dim_player table column names
    player_data_df = player_data_df.rename(
        columns={
            "id": "player_id",
            "second_name": "last_name",
            "team": "team_id",
        }
    )

    return player_data_df


@asset(
    group_name="PLAYER_CHECK",
    description="""Players with new values for dimension columns""",
)
def existing_players_new_data(
    context: AssetExecutionContext,
    api_player_data: pd.DataFrame,
    db_player_data: pd.DataFrame,
) -> pd.DataFrame:

    player_data_comp = db_player_data.merge(
        api_player_data,
        how="left",
        left_on=["player_id"],
        right_on=["player_id"],
        suffixes=("_db", "_api"),
        validate="1:1",
    )

    existing_player_updates = player_data_comp.query(
        "first_name_db != first_name_api or last_name_db != last_name_api or web_name_db != web_name_api or price_db != price_api or team_id_db != team_id_api"
    )

    context.log.info(f"{existing_player_updates}")

    return existing_player_updates


@asset(
    group_name="PLAYER_CHECK",
    description="""Players that are not in the dim_player table""",
)
def new_player_data(
    context: AssetExecutionContext,
    api_player_data: pd.DataFrame,
    db_player_data: pd.DataFrame,
):
    new_players_api = api_player_data.merge(
        db_player_data,
        how="left",
        left_on=["player_id"],
        right_on=["player_id"],
        suffixes=("_api", "_db"),
        validate="1:1",
    )

    new_players = new_players_api.query("team_id_db.isnull()", engine="python")

    context.log.info(f"{new_players}")

    return new_players


@asset(
    group_name="PLAYER_CHECK", description="""Staging table for new player updates"""
)
def staging_player_updates(
    context: AssetExecutionContext,
    existing_players_new_data: pd.DataFrame,
    new_player_data: pd.DataFrame,
    fpl_server: PostgresResource,
) -> None:

    stg_tbl_cols = [
        "player_id",
        "first_name_api",
        "last_name_api",
        "web_name_api",
        "position_api",
        "team_id_api",
        "price_api",
    ]

    player_updates = pd.concat(
        [existing_players_new_data[stg_tbl_cols], new_player_data[stg_tbl_cols]]
    ).sort_values(by=["player_id"])

    player_updates.columns = [col.replace("_api", "") for col in player_updates.columns]

    player_updates["extract_dt"] = dt.today().date() - tmdelta(days=1)

    context.log.info(f"{player_updates}")

    engine = fpl_server.connect_to_engine()

    with engine.connect() as connection:
        player_updates.to_sql(
            name="player_updates",
            schema="stg",
            con=connection,
            index=False,
            if_exists="append",
        )
