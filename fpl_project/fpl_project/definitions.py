from dagster import (
    Definitions,
    load_assets_from_modules,
    EnvVar,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    schedule,
    RunRequest,
)

from fpl_project.fpl_project.assets import players, raw, fixtures, teams
from fpl_project.fpl_project.resources import postgres, fpl_api  # noqa: TID252

all_assets = load_assets_from_modules([players, raw, fixtures, teams])
all_assets_job = define_asset_job(
    name="initial_job", selection=["*fixtures", "*players", "*teams"]
)


@schedule(job=all_assets_job, cron_schedule="*/5 * * * *")
def test_schedule():
    return RunRequest(
        run_key=None,
    )


defs = Definitions(
    assets=all_assets,
    jobs=[all_assets_job],
    schedules=[test_schedule],
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
