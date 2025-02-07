from dagster_dbt import DbtProject
from pathlib import Path


dbt_project = DbtProject(
    project_dir=Path("/opt/dagster/app/dbt_project")
)

dbt_project.prepare_if_dev()
