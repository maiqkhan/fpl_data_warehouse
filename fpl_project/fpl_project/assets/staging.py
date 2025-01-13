from dagster import (
    asset,
    AssetExecutionContext,
)
from ..resources.postgres import PostgresResource
from .models import (
    Base,
    fpl_dates,
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
    return inspect(engine).has_table(table_name=table_name, schema=schema)


def truncate_table(session: orm.Session, table_name: str, schema_name: str) -> None:
    session.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name};"))
    session.commit()


@asset(
    group_name="STAGING",
    description="Staging table for dim_date model.",
    kinds={"python", "postgres", "table"},
)
def staging_dates_table(
    context: AssetExecutionContext, dates_df: pd.DataFrame, fpl_server: PostgresResource
) -> None:

    engine = fpl_server.connect_to_engine()

    if inspect(engine).has_table(
        fpl_dates.__tablename__, schema=fpl_dates.__table_args__["schema"]
    ):
        with fpl_server.get_session() as session:
            max_stg_date = session.execute(
                select(func.max(fpl_dates.date_id))
            ).scalar_one_or_none()

        if datetime.today().date() > max_stg_date:
            truncate_table(
                session=session,
                table_name=fpl_dates.__tablename__,
                schema_name=fpl_dates.__table_args__["schema"],
            )

            today_dt_array = generate_date_fields_array([datetime.today()])

            date_ingest = pd.DataFrame.from_records(today_dt_array)

            date_ingest.to_sql(
                name=fpl_dates.__tablename__,
                schema=fpl_dates.__table_args__["schema"],
                con=engine,
                if_exists="append",
                index=False,
                chunksize=365,
            )

        else:
            pass

    else:
        Base.metadata.create_all(engine, tables=[fpl_dates.__table__])
        context.log.info("table doesnt exists")

        dates_df.sort_values(by=["date_id"]).to_sql(
            name=fpl_dates.__tablename__,
            schema=fpl_dates.__table_args__["schema"],
            con=engine,
            if_exists="append",
            index=False,
            chunksize=365,
        )


@asset(
    group_name="STAGING",
    description="Staging table for dim_team model.",
    kinds={"python", "postgres", "table"},
)
def staging_teams_table(teams: pd.DataFrame, fpl_server: PostgresResource) -> None:
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
    context: AssetExecutionContext, players: pd.DataFrame, fpl_server: PostgresResource
) -> None:

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
    context: AssetExecutionContext,
    matches_df: pd.DataFrame,
    fpl_server: PostgresResource,
) -> None:

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
