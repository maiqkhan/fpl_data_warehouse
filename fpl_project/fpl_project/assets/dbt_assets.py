from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from fpl_project.fpl_project.resources.dbt_resource import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def fpl_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
