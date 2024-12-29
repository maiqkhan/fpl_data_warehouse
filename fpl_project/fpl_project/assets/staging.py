from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
)
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from fpl_project.fpl_project.resources.postgres import PostgresResource
from fpl_project.fpl_project.assets.models import Base, fpl_dates
from fpl_project.fpl_project.assets.dates import generate_date_fields_array
from typing import Dict, List
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import inspect, func, select, orm, text


def truncate_table(session: orm.Session, table_name: str) -> None:
    session.execute(text(f"TRUNCATE TABLE {table_name};"))
    session.commit()


@asset(
    group_name="STAGING",
    description="Staging tables for data model.",
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
            context.log.info(f"add {datetime.today} to table")

            truncate_table(session=session, table_name=fpl_dates.__tablename__)

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
