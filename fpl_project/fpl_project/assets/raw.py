from dagster import (
    asset,
    AssetExecutionContext,
    asset_check,
    AssetCheckResult,
)
from ..resources.fpl_api import FplAPI
from typing import Dict, List


@asset(
    group_name="RAW_DATA",
    description="""Game data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_bootstrap(fpl_api: FplAPI) -> Dict:
    """Fetches the game data from the Fantasy Premier League (FPL) API's bootstrap-static endpoint.
    """
    payload = fpl_api.get_request(endpoint="bootstrap-static/").json()

    return payload


@asset_check(
    asset=raw_bootstrap,
    blocking=True,
    description="""Check that the bootstrap endpoint is returning keys required for donwstream assets""",
)
def raw_bootstrap_valid_keys(bootstrap_dict: Dict):
    """
    Validates that the bootstrap data returned from the FPL API contains the required keys.
    """
    valid_key_set = set(["elements", "teams"])

    if valid_key_set.issubset(bootstrap_dict.keys()):
        return AssetCheckResult(passed=True)
    else:
        return AssetCheckResult(
            passed=False, metadata={"bootstrap_keys": bootstrap_dict.keys()}
        )


@asset(
    group_name="RAW_DATA",
    description="""Player data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_players(raw_bootstrap: Dict) -> List[Dict]:
    """Extracts the player data from the raw bootstrap data returned by the FPL API."""
    return raw_bootstrap["elements"]


@asset(
    group_name="RAW_DATA",
    description="""Team data from FPL api bootstrap-static endpoint""",
    kinds={"python"},
)
def raw_teams(raw_bootstrap: Dict) -> List[Dict]:
    """Extracts the team data from the raw bootstrap data returned by the FPL API."""
    return raw_bootstrap["teams"]


@asset_check(
    asset=raw_teams,
    description="""Check that the requested data from the bootstrap-static/teams endpoint only contains 20 elements, as only 20 teams can partake in an epl season.""",
    blocking=True,
    compute_kind="python",
)
def raw_teams_20_elements_check(team_lst: List[Dict]):
    """Verifies that the data returned from the 'bootstrap-static/teams' endpoint contains exactly 20 teams, 
    as only 20 teams can participate in an EPL season.
    """

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
def raw_fixtures(fpl_api: FplAPI) -> List[Dict]:
    """Fetches the fixture data for the current Fantasy Premier League season from the API."""

    payload = fpl_api.get_request(endpoint="fixtures/").json()

    return payload


@asset_check(
    asset=raw_fixtures,
    description="""Check that the requested data from the fixtures endpoint only contains 380 elements, as only 380 fixtures can take place in an epl season.""",
    blocking=True,
    compute_kind="python",
)
def raw_fixtures_380_elements_check(fixture_lst: List[Dict]):
    """Checks that the number of fixtures returned from the API equals 380, 
    which is the exact number of fixtures for an EPL season.
    """
    if len(fixture_lst) != 380:
        return AssetCheckResult(
            passed=False, metadata={"number_of_fixtures": len(fixture_lst)}
        )

    else:
        return AssetCheckResult(passed=True)
