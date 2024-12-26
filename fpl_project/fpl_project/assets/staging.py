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
from typing import Dict, List
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


@asset(
    group_name="STAGING",
    description="Staging tables for data model.",
    kinds={"python", "postgres", "table"},
)
def staging_dates_table(
    context: AssetExecutionContext, dates_df: pd.DataFrame, fpl_server: PostgresResource
) -> None:

    engine = fpl_server.connect_to_engine()

    Base.metadata.create_all(engine, tables=[fpl_dates.__table__])

    dates_df.sort_values(by=["date_id"]).to_sql(
        name=fpl_dates.__tablename__,
        schema=fpl_dates.__table_args__["schema"],
        con=engine,
        if_exists="append",
        index=False,
        chunksize=365,
    )
