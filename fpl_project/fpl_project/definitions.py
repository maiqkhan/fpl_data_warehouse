from dagster import Definitions, load_assets_from_modules, EnvVar

from fpl_project.fpl_project.assets import assets
from fpl_project.fpl_project.resources import postgres, fpl_api  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "fpl_server": postgres.PostgresResource(
            credentials=postgres.CredentialsResource(
                username=EnvVar("DB_USERNAME"),
                password=EnvVar("DB_PASSWORD"),
                server_port=EnvVar("SERVER_PORT"),
                server=EnvVar("SERVER"),
                database=EnvVar("DATABASE"),
            )
        ),
        "fpl_api": fpl_api.FplAPI(base_url="https://fantasy.premierleague.com/api/"),
    },
)
