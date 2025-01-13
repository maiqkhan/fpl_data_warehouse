from dagster import (
    asset,
    AssetExecutionContext,
)
from typing import Dict, List
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


def generate_date_fields_array(dt_lst: List[datetime]) -> List[Dict]:

    dt_dict_array = []

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

    for dt in dt_lst:
        date_dict = {
            "date_key": int(datetime.strftime(dt, "%Y%m%d")),
            "date_id": dt,
            "year": dt.year,
            "month_num": dt.month,
            "month_name": month_dict.get(dt.month),
            "day_of_month": dt.day,
            "day_of_week": dt.weekday() + 1,
            "day_name": day_dict.get(dt.weekday() + 1),
        }

        dt_dict_array.append(date_dict)

    return dt_dict_array


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

    five_year_dt_lst = [
        first_fixture_date + relativedelta(days=x)
        for x in range((five_years_later - first_fixture_date).days)
    ]

    five_year_dt_dict = generate_date_fields_array(five_year_dt_lst)

    date_df = pd.DataFrame.from_records(five_year_dt_dict)

    return date_df
