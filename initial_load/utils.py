from typing import Optional
from datetime import datetime as dt, timezone as tz


def to_lower_camel_case(snake_str):
    """Capitalize the first letter of each component except the first one
    with the 'capitalize' method and join them together."""

    camel_string = "".join(x.capitalize() for x in snake_str.lower().split("_"))

    return snake_str[0].lower() + camel_string[1:]


def generate_fixture_type(utc_str: str, gameweek: Optional[int]) -> str:
    """Generating the classification of the fixture based on time and day of fixture"""

    utc_dtime = dt.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

    if gameweek == 38:
        return "Final Day"

    match utc_dtime:
        case fixture_dt if fixture_dt.day == 26 and fixture_dt.month == 12:
            return "Boxing Day"
        case fixture_dt if fixture_dt.weekday() == 0:
            return "Monday"
        case fixture_dt if fixture_dt.weekday() in [1, 2, 3]:
            return "Midweek"
        case fixture_dt if fixture_dt.weekday() == 4:
            return "Friday"
        case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [11, 12]:
            return "Saturday Lunch Hour"
        case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour in [14, 15]:
            return "Saturday 3:00 PM"
        case fixture_dt if fixture_dt.weekday() == 5 and fixture_dt.hour > 15:
            return "Saturday Late Game"
        case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour in [15, 16]:
            return "Sunday Prime Time Game"
        case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour < 15:
            return "Sunday Early Game"
        case fixture_dt if fixture_dt.weekday() == 6 and fixture_dt.hour > 16:
            return "Sunday Late Game"
        case _:
            return "Other"


def generate_local_fixture_time(utc_str: str) -> dt:
    """Convert string timestamp of utc time into local datetime value"""

    utc_dtime = dt.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")

    utc_dtime = utc_dtime.replace(tzinfo=tz.utc)

    local_dtime = utc_dtime.astimezone()

    local_dtime = local_dtime.replace(tzinfo=None)

    return local_dtime
