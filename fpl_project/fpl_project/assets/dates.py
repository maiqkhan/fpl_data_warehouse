from dagster import (
    asset,
)
from typing import Dict, List
import pandas as pd
from datetime import date, datetime

def generate_date_fields_array(dt_lst: List[datetime]) -> List[Dict]:
    """Generate an array of dictionaries containing various date-related fields 
    for each datetime object in the provided list.
    """
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
    description="""Date record for today's extraction of FPL data""",
    kinds={"python", "pandas"},
)
def dates_df() -> pd.DataFrame:
    """Generate a DataFrame containing date-related fields for today's extraction."""
    today_extract = datetime.today().date()

    today_dt_dict = generate_date_fields_array([today_extract])

    date_df = pd.DataFrame.from_records(today_dt_dict)

    return date_df
