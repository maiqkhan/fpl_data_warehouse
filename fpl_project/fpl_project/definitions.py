from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    EnvVar,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    schedule,
    RunRequest,
)

from dagster_dbt import DbtCliResource

from fpl_project.fpl_project.assets import (
    dates,
    players,
    raw,
    fixtures,
    teams,
    matches,
    staging,
    dbt_assets,
)
from fpl_project.fpl_project.resources import (
    postgres,
    fpl_api,
    dbt_resource,
)  # noqa: TID252
from pathlib import Path

all_assets = load_assets_from_modules(
    [players, raw, fixtures, teams, matches, dates, staging, dbt_assets]
)
all_asset_checks = load_asset_checks_from_modules([raw, players])
all_assets_job = define_asset_job(
    name="initial_job", selection=["*fixtures", "*players", "*teams"]
)


get_raw_data = define_asset_job(
    name="GET_RAW_API_DATA", selection=AssetSelection.groups("RAW_DATA")
)

table_source_data = define_asset_job(
    name="BUILD_TABLE_SOURCE_DATA",
    selection=AssetSelection.groups("FIXTURES")
    | AssetSelection.groups("TEAMS")
    | AssetSelection.groups("PLAYER")
    | AssetSelection.groups("DATE"),
)

initial_load_job = define_asset_job(name="initial_load", selection=["*matches_df"])

refresh_dimensions = define_asset_job(
    name="REFRESH_DIMENSIONS", selection=["*dim_player", "*dim_fixture", "*dim_date"]
)

refresh_match_stats = define_asset_job(
    name="REFRESH_MATCH_STATS", selection=["*fact_match_stats"]
)


@schedule(job=all_assets_job, cron_schedule="*/5 * * * *")
def test_schedule():
    return RunRequest(
        run_key=None,
    )


defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[
        all_assets_job,
        initial_load_job,
        get_raw_data,
        table_source_data,
        refresh_dimensions,
        refresh_match_stats,
    ],
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
        "dbt": DbtCliResource(
            project_dir=Path(__file__)
            .joinpath("..", "..", "..", "dbt_project")
            .resolve()
        ),
    },
)
