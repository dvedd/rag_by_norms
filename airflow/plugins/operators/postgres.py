from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context
    from pandas import DataFrame


class DataFrameToPostgresOverrideOperator(BaseOperator):
    """Replace a PostgreSQL table with the supplied DataFrame."""

    template_fields = ("data",)

    def __init__(
        self,
        table_name: str,
        data: DataFrame,
        connection_uri: str = "postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name
        self.data = data

    def execute(self, context: Context) -> None:
        """Write the DataFrame and dispose the engine even when writing fails."""
        from sqlalchemy import create_engine

        engine = create_engine(url=self.connection_uri)

        try:
            with engine.begin() as connection:
                self.data.to_sql(
                    name=self.table_name, con=connection, if_exists="replace"
                )
        finally:
            engine.dispose()


class PostgresToDataFrameOperator(BaseOperator):
    """Read a configured PostgreSQL table into a DataFrame."""

    def __init__(
        self,
        table_name: str,
        connection_uri: str = "postgresql://postgres:postgres@client-postgres:5432/postgres",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name

    def execute(self, context: Context) -> DataFrame:
        """Read the table and dispose the task-owned engine before returning."""
        import pandas as pd
        from sqlalchemy import create_engine

        engine = create_engine(url=self.connection_uri)

        try:
            with engine.connect() as connection:
                # The table name must remain trusted DAG configuration.
                return pd.read_sql_query(
                    sql=f"select * from {self.table_name}", con=connection
                )
        finally:
            engine.dispose()


class CheckIfTableExistsOperator(BaseOperator):
    """Fail when the configured table is absent from the PostgreSQL catalog."""

    def __init__(
        self,
        table_name: str,
        connection_uri: str = "postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name

    def execute(self, context: Context) -> None:
        """Inspect table names and release the task-owned database resources."""
        import pandas as pd
        from sqlalchemy import create_engine

        engine = create_engine(url=self.connection_uri)

        try:
            with engine.connect() as connection:
                tables = pd.read_sql_query(
                    sql="select * from pg_catalog.pg_tables;", con=connection
                )
        finally:
            engine.dispose()

        if not (tables["tablename"] == self.table_name).any():
            raise AirflowFailException(f"Table {self.table_name} does not exist")
