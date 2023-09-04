# Copyright 2023 Holger Bruch (hb@mfdz.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
import pandas
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection
from typing import Iterator, Optional, Sequence

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)


@contextmanager
def connect_postgresql(config, schema="public") -> Iterator[Connection]:
    url = URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["database"],
    )
    conn = None
    try:
        conn = create_engine(url).connect()
        yield conn
    finally:
        if conn:
            conn.close()


class PostgreSQLPandasIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas dataframe and store it in postgresql."""

    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    user: Optional[str] = "postgres"
    password: Optional[str]
    database: Optional[str]

    @property
    def _config(self):
        return self.dict()

    def handle_output(self, context: OutputContext, obj: pandas.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if isinstance(obj, pandas.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            # TODO make chunksize configurable
            with connect_postgresql(config=self._config) as con:
                obj.to_sql(
                    con=con,
                    name=table,
                    schema=schema,
                    if_exists="replace",
                    chunksize=500,
                )
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

    def load_input(self, context: InputContext) -> pandas.DataFrame:
        schema, table = self._get_schema_table(context.asset_key)
        with connect_postgresql(config=self._config) as con:
            columns = (context.metadata or {}).get("columns")
            return self._load_input(con, table, schema, columns, context)

    def _load_input(
        self,
        con: Connection,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
        context: InputContext,
    ) -> pandas.DataFrame:
        df = pandas.read_sql(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            con=con,
        )
        return df

    def _get_schema_table(self, asset_key):
        return (
            asset_key.path[-2] if len(asset_key.path) > 1 else "public",
            asset_key.path[-1],
        )

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        col_str = ", ".join(columns) if columns else "*"
        return f"""SELECT {col_str} FROM {schema}.{table}"""
