from dagster import asset, AssetExecutionContext
import pandas as pd
from fpl_project.fpl_project.resources.postgres import PostgresResource
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from typing import Dict


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
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
)
def raw_bootstrap_data(context: AssetExecutionContext, fpl_api: FplAPI) -> Dict:

    payload = fpl_api.get_request(endpoint="bootstrap-static/").json()

    # raw_player_data_df = pd.DataFrame.from_records(payload)

    return payload


@asset(
    group_name="PLAYER_CHECK",
    description="""Current player data from FPL api bootstrap-static endpoint""",
)
def api_player_data(raw_bootstrap_data: Dict) -> pd.DataFrame:

    raw_api_player_data = pd.DataFrame.from_records(raw_bootstrap_data["elements"])

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
def new_player_values(
    context: AssetExecutionContext,
    api_player_data: pd.DataFrame,
    db_player_data: pd.DataFrame,
) -> None:

    player_data_comp = db_player_data.merge(
        api_player_data,
        how="left",
        left_on=["player_id"],
        right_on=["player_id"],
        suffixes=("_db", "_api"),
        validate="1:1",
    )

    player_updates = player_data_comp.query(
        "first_name_db != first_name_api or last_name_db != last_name_api or web_name_db != web_name_api or price_db != price_api or team_id_db != team_id_api"
    )

    player_updates.to_csv(
        r"C:\Users\khanm375\Documents\fpl_data_warehouse\updates.csv", index=False
    )

    context.log.info(f"{player_updates}")
