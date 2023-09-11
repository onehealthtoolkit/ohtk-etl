import os

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
    db_user=os.getenv("SRC_DB_USER") or "pphetra",
    db_password=os.getenv("SRC_DB_PASSWORD") or "ohtk",
    db_host=os.getenv("SRC_DB_HOST") or "localhost",
    db_port=os.getenv("SRC_DB_PORT") or "5432",
    db_name=os.getenv("SRC_DB_NAME") or "ohtk",
    db_schema=os.getenv("SRC_DB_SCHEMA") or "bon,public",
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
            host=os.getenv("TARGET_DB_HOST") or "localhost",
            port=os.getenv("TARGET_DB_PORT") or 5432,
            user=os.getenv("TARGET_DB_USER") or "pphetra",
            password=os.getenv("TARGET_DB_PASSWORD") or "ohtk",
            database=os.getenv("TARGET_DB_NAME") or "ohtk_summary",
        ),
    },
)
