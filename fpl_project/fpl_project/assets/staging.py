from dagster import (
    asset, AssetOut, AssetExecutionContext
)
from ..resources.postgres import PostgresResource
from .models import (
    Base,
    stg_dates,
    stg_teams,
    stg_fixtures,
    stg_players,
    stg_matches,
    dim_player
)
import pandas as pd
from typing import List, Tuple
from sqlalchemy import inspect, func, select, orm, text
from sqlalchemy.orm import Session


def table_exists(engine, table_name: str, schema: str = "dbo"):
    """Checks if a table exists in the specified schema."""
    return inspect(engine).has_table(table_name=table_name, schema=schema)


def truncate_table(session: orm.Session, table_name: str, schema_name: str) -> None:
    """Truncates the specified table in the given schema."""
    session.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name};"))
    session.commit()

def reload_staging_table(
    extract_df: pd.DataFrame, 
    session: Session,
    table_name: str,
    sort_keys: List[str],
    insert_chunk_size: int,    
    schema_name: str = "stg",
) -> None:
    "Truncate and load pandas dataframe into specified staging table."        
    truncate_table(
            session=session,
            table_name=table_name,
            schema_name=schema_name,
        )

    extract_df.sort_values(by=sort_keys).to_sql(
        name=table_name,
        schema=schema_name,
        con=session.bind,
        if_exists="append",
        index=False,
        chunksize=insert_chunk_size,
        )


@asset(
    group_name="STAGING",
    description="Staging table for dim_date model.",
    kinds={"python", "postgres", "table"},
)
def staging_dates_table(
    dates_df: pd.DataFrame, fpl_server: PostgresResource
) -> None:
    """Loads the dim_date staging table with date information."""
    with fpl_server.get_session() as session:
        reload_staging_table(
            extract_df= dates_df,
            session= session,
            table_name= "dates",
            sort_keys= ["date_id"],
            insert_chunk_size=1
        )


@asset(
    group_name="STAGING",
    description="Staging table for dim_team model.",
    kinds={"python", "postgres", "table"},
)
def staging_teams_table(teams: pd.DataFrame, fpl_server: PostgresResource) -> None:
    """Loads the dim_team staging table with team information."""
    with fpl_server.get_session() as session:
        reload_staging_table(
            extract_df= teams,
            session= session,
            table_name= "teams",
            sort_keys= ["team_key"],
            insert_chunk_size=20
        )

@asset(
    group_name="STAGING",
    description="Staging table for dim_fixture model.",
    kinds={"python", "postgres", "table"},
)
def staging_fixtures_table(
    fixtures: pd.DataFrame, fpl_server: PostgresResource
) -> None:
    """Loads the dim_fixtures staging table with fixture information."""
    with fpl_server.get_session() as session:
        reload_staging_table(
            extract_df= fixtures,
            session= session,
            table_name= "fixtures",
            sort_keys= ["fixture_key"],
            insert_chunk_size=380
        )


@asset(
    group_name="STAGING",
    kinds={"python", "postgres", "table"},
    description="Staging table for dim_player table",
)
def staging_player_table(
    context: AssetExecutionContext, players: Tuple[pd.DataFrame, bool], fpl_server: PostgresResource
) -> None:
    """Loads the dim_player staging table with player information."""
    player_df, dim_player_exists = players

    staging_table = "players" if dim_player_exists else "players_initial"

    context.log.info(staging_table)
    
    with fpl_server.get_session() as session:
        reload_staging_table(
            extract_df= player_df,
            session= session,
            table_name= staging_table,
            sort_keys= ["player_id"],
            insert_chunk_size=380
        )
    
@asset(
    group_name="STAGING",
    description="Staging table for dim_date model.",
    kinds={"python", "postgres", "table"},
)
def staging_matches_table(
    matches_df: pd.DataFrame,
    fpl_server: PostgresResource,
) -> None:
    """Loads the fact_matches staging table with match information."""
    drop_cols = [
        "was_home",
        "kickoff_time",
        "team_id",
        "first_name",
        "last_name",
        "web_name",
        "position",
    ]

    matches_df = matches_df.drop(drop_cols, axis=1)
    
    with fpl_server.get_session() as session:
        reload_staging_table(
            extract_df= matches_df,
            session= session,
            table_name= "matches",
            sort_keys= ["player_id", "fixture_id"],
            insert_chunk_size=380
        )
        