from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
)
from fpl_project.fpl_project.resources.fpl_api import FplAPI
from typing import Dict, List


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_bootstrap(fpl_api: FplAPI) -> Dict:

    payload = fpl_api.get_request(endpoint="bootstrap-static/").json()

    # raw_player_data_df = pd.DataFrame.from_records(payload)

    return payload


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_players(context: AssetExecutionContext, raw_bootstrap: Dict) -> List[Dict]:

    context.log.info(raw_bootstrap["elements"])

    return raw_bootstrap["elements"]


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_teams(context: AssetExecutionContext, raw_bootstrap: Dict) -> List[Dict]:

    context.log.info(raw_bootstrap["teams"])

    return raw_bootstrap["teams"]


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_fixtures(context: AssetExecutionContext, fpl_api: FplAPI) -> List[Dict]:

    payload = fpl_api.get_request(endpoint="fixtures/").json()

    return payload
