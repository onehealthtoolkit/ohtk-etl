from dagster import (
    ConfigurableResource,
)
import sqlalchemy


class OhtkDatabaseResource(ConfigurableResource):
    db_user: str
    db_password: str
    db_host: str
    db_port: str
    db_name: str
    db_schema: str

    def get_engine(self) -> sqlalchemy.engine.Engine:
        return sqlalchemy.create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}",
            connect_args={"options": f"-csearch_path={self.db_schema}"},
        )
