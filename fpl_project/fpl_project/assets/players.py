from typing import Dict, List
from datetime import datetime as dt, timedelta as tmdelta
from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
)
import pandas as pd
from ..resources.postgres import PostgresResource
from .models import dim_player, Base


def generate_expiry_date(
    eff_dt, is_last_record, default_expiry=dt(year=2261, month=12, day=31).date()
):
    if is_last_record:
        return default_expiry
    else:
        return eff_dt - tmdelta(days=1)


@asset(
    group_name="PLAYER",
    description="Player payload from ",
    kinds={"python", "pandas"},
)
def raw_player_df(raw_players: List[Dict]) -> pd.DataFrame:

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
def players(raw_player_df: pd.DataFrame, epl_season: str) -> pd.DataFrame:

    player_dict = {1: "Goalkeeper", 2: "Defender", 3: "Midfielder", 4: "Forward"}

    player_df = raw_player_df.copy()

    player_df["position"] = player_df["element_type"].apply(player_dict.get)

    player_df = player_df.drop("element_type", axis=1)

    player_df["season"] = epl_season

    player_df = player_df.rename(
        columns={"id": "player_id", "second_name": "last_name", "now_cost": "price"}
    )

    player_df["extract_dt"] = dt.today().date()

    return player_df


@asset_check(
    asset=players,
    blocking=True,
    description="Check that player ID is unique across players dataframe.",
)
def unique_player_check(
    player_df: pd.DataFrame,
) -> AssetCheckResult:

    if player_df.drop_duplicates(subset=["player_id"]).shape[0] != player_df.shape[0]:
        return AssetCheckResult(
            passed=False,
            metadata={
                "dataframe_row_count": player_df.shape[0],
                "unique_players": len(player_df["player_id"].unique()),
            },
        )
    else:
        return AssetCheckResult(passed=True)


@asset(
    group_name="INITIAL_LOAD",
    description="Apply Type 2 Slowly Changing Dimension logic to historical player data",
    kinds={"python", "pandas"},
)
def player_scd_type_2_df(
    matches_df: pd.DataFrame,
) -> pd.DataFrame:

    player_hist_df = matches_df[
        [
            "player_id",
            "season",
            "first_name",
            "last_name",
            "web_name",
            "position",
            "team_id",
            "value",
            "kickoff_time",
        ]
    ].sort_values(by=["player_id", "kickoff_time"])

    for col in ["player_id", "team_id", "value"]:
        player_hist_df[f"{col}_change"] = (
            player_hist_df[f"{col}"] != player_hist_df[f"{col}"].shift()
        )

    player_hist_df["new_record"] = (
        player_hist_df["player_id_change"]
        | player_hist_df["value_change"]
        | player_hist_df["team_id_change"]
    )

    player_hist_df["eff_group"] = player_hist_df["new_record"].cumsum()

    scd_player_history = (
        player_hist_df.groupby("eff_group")
        .agg(
            player_id=("player_id", "first"),
            season=("season", "first"),
            first_name=("first_name", "first"),
            last_name=("last_name", "first"),
            web_name=("web_name", "first"),
            position=("position", "first"),
            price=("value", "first"),
            team_id=("team_id", "first"),
            effective_dt=("kickoff_time", "first"),
        )
        .reset_index(drop=True)
    )

    scd_player_history["effective_dt"] = scd_player_history["effective_dt"].apply(
        lambda x: x.date()
    )

    scd_player_history["is_last_record"] = (
        scd_player_history.groupby("player_id")["effective_dt"].transform("max")
        == scd_player_history["effective_dt"]
    )

    scd_player_history["next_effective_dt"] = scd_player_history.groupby("player_id")[
        "effective_dt"
    ].shift(-1)

    scd_player_history["expiry_dt"] = scd_player_history.apply(
        lambda x: generate_expiry_date(x["next_effective_dt"], x["is_last_record"]),
        axis=1,
    )

    scd_player_history["current_ind"] = scd_player_history["is_last_record"].apply(
        lambda x: 1 if x else 0
    )

    scd_player_history = scd_player_history.drop(
        ["is_last_record", "next_effective_dt"], axis=1
    )

    return scd_player_history


@asset(
    group_name="INITIAL_LOAD",
    description="Apply Type 2 Slowly Changing Dimension logic to historical player data",
    kinds={"python", "pandas"},
)
def initial_dim_player(
    context: AssetExecutionContext,
    player_scd_type_2_df: pd.DataFrame,
    fpl_server: PostgresResource,
):

    table_name = dim_player.__tablename__
    schema_name = dim_player.__table_args__["schema"]
    table_inst = dim_player.__table__

    player_scd_type_2_df["team_key"] = player_scd_type_2_df.apply(
        lambda x: int(f"{x['season'][:4]}{x['season'][5:]}{x['team_id']}"), axis=1
    )

    player_scd_type_2_df = player_scd_type_2_df.drop("team_id", axis=1)

    context.log.info(player_scd_type_2_df.columns)

    engine = fpl_server.connect_to_engine()

    Base.metadata.create_all(engine, tables=[table_inst])

    player_scd_type_2_df.sort_values(by=["player_id", "effective_dt"]).to_sql(
        name=table_name,
        schema=schema_name,
        if_exists="append",
        chunksize=220,
        index=False,
        con=engine,
    )
