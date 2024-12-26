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
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


@asset(
    group_name="DATE",
    description="""Date Table ranging 5 years from the first fixture of the season""",
    kinds={"python", "pandas"},
)
def dates_df(context: AssetExecutionContext, first_fixture_date: date) -> pd.DataFrame:

    month_dict = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }

    day_dict = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
        7: "Sunday",
    }

    five_years_later = first_fixture_date + relativedelta(years=5)

    date_list = []
    current_date = first_fixture_date

    while current_date <= five_years_later:

        date_dict = {
            "date_key": int(datetime.strftime(current_date, "%Y%m%d")),
            "date_id": current_date,
            "year": current_date.year,
            "month_num": current_date.month,
            "month_name": month_dict.get(current_date.month),
            "day_of_month": current_date.day,
            "day_of_week": current_date.weekday() + 1,
            "day_name": day_dict.get(current_date.weekday() + 1),
        }

        date_list.append(date_dict)

        current_date += relativedelta(days=1)

    date_df = pd.DataFrame.from_records(date_list)

    return date_df
