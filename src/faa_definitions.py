from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_duckdb_pandas import duckdb_pandas_io_manager
from faa_assets import build_asset, data_sets


faa_assets = [build_asset(data_set) for data_set in data_sets]

faa_job = define_asset_job("faa_job", selection=faa_assets)

faa_schedule = ScheduleDefinition(
    job=faa_job,
    cron_schedule="0 17 * * MON",
)

defs = Definitions(
    assets=faa_assets,
    schedules=[faa_schedule],
    resources={
        "faa_io_manager": duckdb_pandas_io_manager.configured(
            {
                "database": {"env": "DUCKDB_FAA_DB"},
            }
        ),
    },
)
