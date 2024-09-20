import pandas as pd
from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset

from .authority_graph import Graph
from .ohtk_db import OhtkDatabaseResource


@asset
def authorities(ohtk_db: OhtkDatabaseResource) -> pd.DataFrame:
    db_engine = ohtk_db.get_engine()
    with db_engine.connect() as conn:
        df = pd.read_sql(
            "SELECT id, name FROM accounts_authority where deleted_at is null", conn
        )
        return df


@asset
def authorities_inherits(ohtk_db: OhtkDatabaseResource) -> pd.DataFrame:
    db_engine = ohtk_db.get_engine()
    with db_engine.connect() as conn:
        df = pd.read_sql(
            "SELECT from_authority_id, to_authority_id FROM accounts_authority_inherits",
            conn,
        )
        return df


@asset
def authorities_id_map(
    context: AssetExecutionContext, authorities_inherits: pd.DataFrame
) -> pd.DataFrame:
    graph = Graph()
    for _, row in authorities_inherits.iterrows():
        graph.addEdge(row["to_authority_id"], row["from_authority_id"])

    data = []
    for node in graph.nodes.values():
        id = node.value
        first_path = node.paths_to_root()[0]
        data.append([id, str.join(".", map(lambda x: str(x.value), first_path))])

    df = pd.DataFrame(data, columns=["id", "path"])
    context.log.info(df.head())
    return df


def _get_date_range(context: AssetExecutionContext) -> (str, str):
    start_date_str = context.asset_partition_key_for_output()
    start_date_str = (
        pd.to_datetime(start_date_str)
        .tz_localize("Asia/Bangkok")
        .strftime("%Y-%m-%d %H:%M:%S %Z")
    )

    context.log.info(f"start_date_str: {start_date_str}")

    # end_date = start_date + 1 month
    end_date_str = (
        pd.to_datetime(start_date_str)
        + pd.DateOffset(months=1)
        - pd.DateOffset(milliseconds=1)
    ).strftime("%Y-%m-%d %H:%M:%S %Z")
    context.log.info(f"end_date_str: {end_date_str}")
    return start_date_str, end_date_str


@asset(
    partitions_def=MonthlyPartitionsDefinition(
        start_date="2023-01-01", timezone="Asia/Bangkok"
    ),
    io_manager_key="target_io_manager",
    group_name="report_group",
)
def zero_reports(
    context: AssetExecutionContext,
    ohtk_db: OhtkDatabaseResource,
    authorities_id_map: pd.DataFrame,
) -> pd.DataFrame:
    start_date_str, end_date_str = _get_date_range(context)

    db_engine = ohtk_db.get_engine()
    with db_engine.connect() as conn:
        df = pd.read_sql(
            """
        select r.id as report_id,
            r.created_at as report_date,
            u.id as user_id,
            u.username,
            a.name as authority_name,
            a.id as authority_id
        from reports_zeroreport r,
            accounts_user u,
            accounts_authority a,
            accounts_authorityuser au
        where r.reported_by_id = u.id
        and u.id = au.user_ptr_id
        and au.authority_id = a.id
        and date(r.created_at) not in  ('2023-11-08', '2023-11-09', '2023-11-29', '2023-11-30', '2024-01-10', '2024-01-11', '2024-03-26', '2024-03-27', '2024-04-03', '2024-04-04', '2024-04-05', '2024-07-31', '2024-08-07', '2024-08-09', '2024-08-15', '2024-08-17', '2024-08-19', '2024-08-20', '2024-08-23')
        and r.created_at >= %(start_date_str)s
        and r.created_at <= %(end_date_str)s
        """,
            conn,
            params={"start_date_str": start_date_str, "end_date_str": end_date_str},
        )
        # add column ids from authorities_id_map.path
        df = df.merge(authorities_id_map, left_on="authority_id", right_on="id")
        context.log.info(df.head())
        return df


@asset(
    partitions_def=MonthlyPartitionsDefinition(
        start_date="2023-01-01", timezone="Asia/Bangkok"
    ),
    io_manager_key="target_io_manager",
    group_name="report_group",
)
def incident_reports(
    context: AssetExecutionContext,
    ohtk_db: OhtkDatabaseResource,
    authorities_id_map: pd.DataFrame,
) -> pd.DataFrame:
    start_date_str, end_date_str = _get_date_range(context)

    db_engine = ohtk_db.get_engine()
    with db_engine.connect() as conn:
        df = pd.read_sql(
            """
        select r.id as report_id,
            r.incident_date,
            rt.name as report_type,
            rc.name as category,
            a.name as authority_name,
            a.id as authority_id,
            u.username,
            u.id as user_id,
            /* latitude and longitude */
            st_x(gps_location::geometry) as longitude,
            st_y(gps_location::geometry) as latitude,
            r.data::JSONB as data
        from reports_incidentreport r,
            reports_reporttype rt,
            reports_category rc,
            accounts_user u,
        accounts_authority a,
        accounts_authorityuser au
        where r.report_type_id = rt.id
        and rt.category_id = rc.id
        and r.reported_by_id = u.id
        and u.id = au.user_ptr_id
        and au.authority_id = a.id
        and r.test_flag = false
        and date(r.created_at) not in  ('2023-11-08', '2023-11-09', '2023-11-29', '2023-11-30', '2024-01-10', '2024-01-11', '2024-03-26', '2024-03-27', '2024-04-03', '2024-04-04', '2024-04-05', '2024-07-31', '2024-08-07', '2024-08-09', '2024-08-15', '2024-08-17', '2024-08-19', '2024-08-20', '2024-08-23')
        and r.incident_date >= %(start_date_str)s
        and r.incident_date <= %(end_date_str)s
        """,
            conn,
            params={"start_date_str": start_date_str, "end_date_str": end_date_str},
        )
        df = df.merge(authorities_id_map, left_on="authority_id", right_on="id")
        context.log.info(df.head())
        return df
