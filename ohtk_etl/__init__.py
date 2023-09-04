from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
)

from . import assets
from .ohtk_db import OhtkDatabaseResource
from .postgres_io_manager import PostgreSQLPandasIOManager

all_assets = load_assets_from_modules([assets])

source_db = OhtkDatabaseResource(
    db_user="pphetra",
    db_password="ohtk",
    db_host="localhost",
    db_port="5432",
    db_name="ohtk",
    db_schema="bon",
)

report_job = define_asset_job("report_job", AssetSelection.groups("report_group"))
report_schedule = ScheduleDefinition(job=report_job, cron_schedule="0 0 * * *")


defs = Definitions(
    assets=all_assets,
    schedules=[report_schedule],
    jobs=[report_job],
    resources={
        "ohtk_db": source_db,
        "target_io_manager": PostgreSQLPandasIOManager(
            host="localhost",
            port=5432,
            user="pphetra",
            password="ohtk",
            database="ohtk_summary",
        ),
    },
)
