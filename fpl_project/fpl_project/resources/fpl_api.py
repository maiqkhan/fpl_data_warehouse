from dagster import ConfigurableResource
from requests import Response, RequestException
import requests
import backoff


class FplAPI(ConfigurableResource):
    """Resource for sending requests to the FPL API"""

    base_url: str

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_time=60,
        max_tries=3,
    )
    def get_request(self, endpoint: str) -> Response:
        try:
            response: Response = requests.get(
                f"{self.base_url}{endpoint}",
                timeout=(3.05, 10),
            )

            response.raise_for_status()

            return response

        except RequestException as e:
            return e
