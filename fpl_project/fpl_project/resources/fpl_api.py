from dagster import ConfigurableResource
from requests import Response, RequestException
import requests


class FplAPI(ConfigurableResource):
    """Resource for sending requests to the FPL API"""

    base_url: str

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
