from pathlib import Path
from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    EnvVar,
    define_asset_job,
    schedule,
    RunRequest,
    DefaultScheduleStatus,
)

from dagster_dbt import DbtCliResource

from .assets import (
    dates,
    players,
    raw,
    fixtures,
    teams,
    matches,
    staging,
    dbt_assets,
)
from .resources import (
    postgres,
    fpl_api,
)  # noqa: TID252

all_assets = load_assets_from_modules(
    [players, raw, fixtures, teams, matches, dates, staging, dbt_assets]
)
all_asset_checks = load_asset_checks_from_modules(
    [
        players,
        raw,
        fixtures,
        teams,
        matches,
        dates,
        staging,
    ]
)

create_dim_team_table = define_asset_job(
    name="INITIAL_DIM_TEAM_LOAD", selection=["*dim_team"]
)

create_initial_dim_player_table = define_asset_job(
    name="INITIAL_DIM_PLAYER_LOAD", selection=["*initial_dim_player"]
)


refresh_match_stats = define_asset_job(
    name="REFRESH_MATCH_STATS", selection=["*fact_match_stats"]
)

@schedule(
    job=refresh_match_stats,
    cron_schedule=f"30 0 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_fpl_data_refresh():
    return RunRequest(
        run_key=None,
    )


defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[refresh_match_stats, create_dim_team_table, create_initial_dim_player_table],
    schedules=[daily_fpl_data_refresh],
    resources={
        "fpl_server": postgres.PostgresResource(
            credentials=postgres.CredentialsResource(
                username=EnvVar("POSTGRES_USER"),
                password=EnvVar("POSTGRES_PASSWORD"),
                server_port=EnvVar("POSTGRES_PORT"),
                server="fpl_db",
                database=EnvVar("POSTGRES_DB"),
            )
        ),
        "fpl_api": fpl_api.FplAPI(base_url="https://fantasy.premierleague.com/api/"),
        "dbt": DbtCliResource(
            project_dir="/opt/dagster/app/dbt_project"
        ),
    },
)
