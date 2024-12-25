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


@asset_check(asset=raw_bootstrap, blocking=True, description=""" """)
def raw_bootstrap_valid_keys(context: AssetCheckExecutionContext, bootstrap_dict: Dict):
    valid_key_set = set(["elements", "teams"])

    if valid_key_set.issubset(bootstrap_dict.keys()):
        return AssetCheckResult(passed=True)
    else:
        return AssetCheckResult(
            passed=False, metadata={"bootstrap_keys": bootstrap_dict.keys()}
        )


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_players(context: AssetExecutionContext, raw_bootstrap: Dict) -> List[Dict]:

    return raw_bootstrap["elements"]


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_teams(context: AssetExecutionContext, raw_bootstrap: Dict) -> List[Dict]:

    context.log.info(raw_bootstrap["teams"])

    return raw_bootstrap["teams"]


@asset_check(
    asset=raw_teams,
    description="""Check that the requested data from the bootstrap-static/teams endpoint only contains 20 elements, as only 20 teams can partake in an epl season.""",
    blocking=True,
    compute_kind="python",
)
def raw_teams_20_elements_check(
    context: AssetCheckExecutionContext, team_lst: List[Dict]
):

    if len(team_lst) != 20:
        return AssetCheckResult(
            passed=False, metadata={"number_of_teams": len(team_lst)}
        )

    else:
        return AssetCheckResult(passed=True)


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_fixtures(context: AssetExecutionContext, fpl_api: FplAPI) -> List[Dict]:

    payload = fpl_api.get_request(endpoint="fixtures/").json()

    return payload


@asset_check(
    asset=raw_fixtures,
    description="""Check that the requested data from the fixtures endpoint only contains 380 elements, as only 380 fixtures can take place in an epl season.""",
    blocking=True,
    compute_kind="python",
)
def raw_fixtures_380_elements_check(
    context: AssetCheckExecutionContext, fixture_lst: List[Dict]
):
    if len(fixture_lst) != 380:
        return AssetCheckResult(
            passed=False, metadata={"number_of_fixtures": len(fixture_lst)}
        )

    else:
        return AssetCheckResult(passed=True)
