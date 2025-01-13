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

refresh_match_stats = define_asset_job(
    name="REFRESH_MATCH_STATS", selection=["*fact_match_stats"]
)

match_stats_sched_hour = EnvVar("MATCH_STATS_SCHED_HOUR").get_value()
match_stats_sched_min = EnvVar("MATCH_STATS_SCHED_MINUTE").get_value()


@schedule(
    job=refresh_match_stats,
    cron_schedule=f"{match_stats_sched_min} {match_stats_sched_hour} * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_fpl_data_refresh():
    return RunRequest(
        run_key=None,
    )


defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[refresh_match_stats],
    schedules=[daily_fpl_data_refresh],
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
