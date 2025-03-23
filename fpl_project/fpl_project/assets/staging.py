from dagster import (
    asset,
)
from ..resources.postgres import PostgresResource
from .models import (
    Base,
    stg_dates,
    stg_teams,
    stg_fixtures,
    stg_players,
    stg_matches,
)
from .dates import generate_date_fields_array
import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, func, select, orm, text


def table_exists(engine, table_name: str, schema: str = "dbo"):
    """Checks if a table exists in the specified schema."""
    return inspect(engine).has_table(table_name=table_name, schema=schema)


def truncate_table(session: orm.Session, table_name: str, schema_name: str) -> None:
    """Truncates the specified table in the given schema."""
    session.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name};"))
    session.commit()


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
        dates_df.to_sql(
                name=stg_dates.__tablename__,
                schema=stg_dates.__table_args__["schema"],
                con=session.bind,
                if_exists="append",
                index=False
            )


@asset(
    group_name="STAGING",
    description="Staging table for dim_team model.",
    kinds={"python", "postgres", "table"},
)
def staging_teams_table(teams: pd.DataFrame, fpl_server: PostgresResource) -> None:
    """Loads the dim_team staging table with team information."""
    engine = fpl_server.connect_to_engine()

    table_name = stg_teams.__tablename__
    schema_name = stg_teams.__table_args__["schema"]
    table_inst = stg_teams.__table__

    if inspect(engine).has_table(table_name, schema=schema_name):
        with fpl_server.get_session() as session:
            truncate_table(
                session=session,
                table_name=table_name,
                schema_name=schema_name,
            )

    else:
        Base.metadata.create_all(engine, tables=[table_inst])

    teams.sort_values(by=["team_key"]).to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=20,
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
    engine = fpl_server.connect_to_engine()

    table_name = stg_fixtures.__tablename__
    schema_name = stg_fixtures.__table_args__["schema"]
    table_inst = stg_fixtures.__table__

    if inspect(engine).has_table(table_name, schema=schema_name):
        with fpl_server.get_session() as session:
            truncate_table(
                session=session,
                table_name=table_name,
                schema_name=schema_name,
            )

    else:
        Base.metadata.create_all(engine, tables=[table_inst])

    fixtures.sort_values(by=["fixture_key"]).to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=380,
    )


@asset(
    group_name="STAGING",
    kinds={"python", "postgres", "table"},
    description="Staging table for dim_player table",
)
def staging_player_table(
    players: pd.DataFrame, fpl_server: PostgresResource
) -> None:
    """Loads the dim_player staging table with player information."""
    engine = fpl_server.connect_to_engine()

    table_name = stg_players.__tablename__
    schema_name = stg_players.__table_args__["schema"]
    table_inst = stg_players.__table__

    if inspect(engine).has_table(table_name, schema=schema_name):
        with fpl_server.get_session() as session:
            truncate_table(
                session=session,
                table_name=table_name,
                schema_name=schema_name,
            )

    else:
        Base.metadata.create_all(engine, tables=[table_inst])

    players.rename(columns={"team": "team_id"}).to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=380,
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
    engine = fpl_server.connect_to_engine()

    table_name = stg_matches.__tablename__
    schema_name = stg_matches.__table_args__["schema"]
    table_inst = stg_matches.__table__

    if inspect(engine).has_table(table_name, schema=schema_name):
        with fpl_server.get_session() as session:
            truncate_table(
                session=session,
                table_name=table_name,
                schema_name=schema_name,
            )

    else:
        Base.metadata.create_all(engine, tables=[table_inst])

    drop_cols = [
        "was_home",
        "kickoff_time",
        "team_id",
        "first_name",
        "last_name",
        "web_name",
        "position",
    ]

    matches_df.drop(drop_cols, axis=1).to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=380,
    )
