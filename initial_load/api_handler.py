import calendar
from typing import List, Protocol, Dict
from datetime import datetime as dt
import requests
from utils import (
    to_lower_camel_case,
    generate_fixture_type,
    generate_local_fixture_time,
)


class DerivationStrategy(Protocol):
    """Basic representation of field derivation strategy"""

    def process_data(self, data) -> None:
        raise NotImplementedError("Subclasses should implement this method")


class DefaultDerivation:
    """Default field derivation strategy"""

    def process_data(self, data) -> None:
        return data


class FixtureDerivation:
    """Derivation strategy for football fixture data"""

    def process_data(self, data: List[Dict[str, any]]) -> List[Dict[str, any]]:
        for fixture in data:

            utc_kickoff_time = fixture.pop("kickoffTime")

            fixture["fixtureType"] = generate_fixture_type(
                utc_kickoff_time, fixture["event"]
            )
            fixture["localKickoffTime"] = generate_local_fixture_time(utc_kickoff_time)
            fixture["localKickoffMonth"] = calendar.month_name[
                fixture["localKickoffTime"].month
            ]

        return data


class PlayerDataDerivation:
    """Derivation strategy for player historical data"""

    def process_data(self, data: List[Dict[str, any]]) -> List[Dict[str, any]]:
        for match_stats in data:
            for dict_key in ["element"]:
                match_stats.pop(dict_key, None)

            match_stats["value"] = match_stats["value"] / 10

        return data


DATA_DERIVATION_STRATEGIES = {
    "fixtures": FixtureDerivation(),
    "element-summary": PlayerDataDerivation(),
}


class DerivationStrategyFactory:
    """Factory for getting the deriving strategy based on the api endpoint"""

    @staticmethod
    def get_strategy(endpoint) -> DerivationStrategy:
        return DATA_DERIVATION_STRATEGIES.get(endpoint, DefaultDerivation())


class requestHandler:
    def __init__(
        self,
        base_url: str,
        endpoint: str,
        keep_keys: List[str] = None,
        subset_dict_key: str = None,
    ) -> None:
        self.base_url = base_url
        self.endpoint = endpoint
        self.keep_keys = keep_keys
        self.subset_dict_key = subset_dict_key
        self.data = None
        self.derivation_strategy = None

    def get_api_response(self) -> None:
        """
        Fetch and store data from an API endpoint.

        Sends a GET request to `self.endpoint` and stores the JSON response in `self.data`.
        If `self.subset_dict_key` is specified, stores the value associated with that key.
        Prints an error message if the request fails.
        """
        api_route = self.base_url + "//" + self.endpoint

        try:
            response: requests.models.Response = requests.get(
                api_route, timeout=(3.05, 5)
            )

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

        if self.subset_dict_key is None:
            self.data = response.json()
        else:
            self.data = response.json()[self.subset_dict_key]

    def extract_keys(self) -> None:
        """
        Filter and transform keys in `self.data`.

        Converts the keys specified in `self.keep_keys` to lower camel case
        for each dictionary in `self.data`.
        """

        self.data = [
            {to_lower_camel_case(key): d[key] for key in self.keep_keys if key in d}
            for d in self.data
        ]

    def derive_fields(self) -> None:
        """
        Derive new fields using the strategy based on the endpoint.

        Sets the `derivation_strategy` based on `self.endpoint` and processes
        `self.data` with the selected strategy.
        """

        derive_strategy = self.endpoint.split("/")[0]

        self.derivation_strategy = DerivationStrategyFactory.get_strategy(
            derive_strategy
        )

        self.data = self.derivation_strategy.process_data(self.data)

    def add_extract_dt(self) -> None:
        """
        Add extract_dt to data dictionary
        """

        self.data = [
            {**data_dict, "extract_dt": dt.strftime(dt.today(), "%Y-%m-%d")}
            for data_dict in self.data
        ]

    def extract_transform_data(self) -> List[Dict[str, any]]:
        """
        Extract and transform data from the API response.

        Calls methods to get the API response, extract keys, and derive fields.
        Returns the processed data.

        Returns:
            List[Dict[str, any]]: The transformed data.
        """

        self.get_api_response()
        self.extract_keys()
        self.derive_fields()
        self.add_extract_dt()

        return self.data
