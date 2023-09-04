from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset
import pandas as pd
from .ohtk_db import OhtkDatabaseResource
from .authority_graph import Graph


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
        and au.authority_id = a.id""",
            conn,
        )
        # add column ids from authorities_id_map.path
        df = df.merge(authorities_id_map, left_on="authority_id", right_on="id")
        context.log.info(df.head())
        return df
