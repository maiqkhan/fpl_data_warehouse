from typing import Optional, List, Dict
from datetime import datetime as dt, timezone as tz
from sqlalchemy.orm.decl_api import DeclarativeMeta
import pandas as pd


def to_lower_camel_case(snake_str):
    """Capitalize the first letter of each component except the first one
    with the 'capitalize' method and join them together."""

    camel_string = "".join(x.capitalize() for x in snake_str.lower().split("_"))

    return snake_str[0].lower() + camel_string[1:]


def generate_fixture_type(utc_str: str, gameweek: Optional[int]) -> str:
    """Generating the classification of the fixture based on time and day of fixture"""

    if gameweek == 38:
        return "Final Day"

    elif utc_str is None:
        return "Not Scheduled"
    else:
        utc_dtime = dt.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

        match utc_dtime:
            case fixture_dt if fixture_dt.day == 26 and fixture_dt.month == 12:
                return "Boxing Day"
            case fixture_dt if fixture_dt.weekday() == 0:
                return "Monday"
            case fixture_dt if fixture_dt.weekday() in [1, 2, 3]:
                return "Midweek"
            case fixture_dt if fixture_dt.weekday() == 4:
                return "Friday"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [
                11,
                12,
            ]:
                return "Saturday Lunch Hour"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [
                14,
                15,
            ]:
                return "Saturday 3:00 PM"
            case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour > 15:
                return "Saturday Late Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour in [
                15,
                16,
            ]:
                return "Sunday Prime Time Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour < 15:
                return "Sunday Early Game"
            case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour > 16:
                return "Sunday Late Game"
            case _:
                return "Other"


def generate_local_fixture_time(utc_str: str) -> dt:
    """Convert string timestamp of utc time into local datetime value"""
    if utc_str is None:
        return None

    utc_dtime = dt.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

    utc_dtime = utc_dtime.replace(tzinfo=tz.utc)

    local_dtime = utc_dtime.astimezone()

    local_dtime = local_dtime.replace(tzinfo=None)

    return local_dtime


def dict_to_model_inst(
    dict_lst: List[Dict[str, any]],
    model: DeclarativeMeta,
) -> List[DeclarativeMeta]:
    """Converts a list of dictionaries into a list of SQLAlchemy model instances."""
    model_insts = [model(**data_dict) for data_dict in dict_lst]

    return model_insts


def generate_season(min_fix_dt: dt) -> str:
    """Generate season indicator for the extraction"""
    min_yr = min_fix_dt.year
    max_yr = min_yr + 1

    season = str(min_yr) + "-" + str(max_yr)[-2:]

    return season
