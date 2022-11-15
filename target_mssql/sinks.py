"""mssql target sink class, which handles writing streams."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import sqlalchemy
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column

from target_mssql.connector import mssqlConnector


class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector

    # Copied purely to help with type hints
    @property
    def connector(self) -> mssqlConnector:
        """The connector object.
        Returns:
            The connector object.
        """
        return self._connector

    @property
    def schema_name(self) -> Optional[str]:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """

        default_target_schema = self.config.get("default_target_schema", None)
        parts = self.stream_name.split("-")

        if default_target_schema:
            return default_target_schema

        if len(parts) in {2, 3}:
            # Stream name is a two-part or three-part identifier.
            # Use the second-to-last part as the schema name.
            stream_schema = self.conform_name(parts[-2], "schema")

            if stream_schema == "public":
                return "dbo"
            else:
                return stream_schema

        # Schema name not detected.
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Process incoming record and return a modified result.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        Returns:
            A new, processed record.
        """
        keys = record.keys()
        for key in keys:
            if type(record[key]) is list:
                record[key] = str(record[key])

        return record

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.
        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.info("Inserting with SQL: %s", insert_sql)

        columns = self.column_representation(schema)

        # temporary fix to ensure missing properties are added
        insert_records = []
        for record in records:
            insert_record = {}
            for column in columns:
                insert_record[column.name] = record.get(column.name)
            insert_records.append(insert_record)

        if self.key_properties:
            self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } ON")

        self.connection.execute(insert_sql, insert_records)

        if self.key_properties:
            self.connection.execute(f"SET IDENTITY_INSERT { full_table_name } OFF")

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def column_representation(
        self,
        schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created

        if self.key_properties:
            self.logger.info(f"Preparing table {self.full_table_name}")
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
            )
            # Create a temp table (Creates from the table above)
            self.logger.info(f"Creating temp table {self.full_table_name}")
            self.connector.create_temp_table_from_table(
                from_table_name=self.full_table_name
            )
            # Insert into temp table
            self.logger.info("Inserting into temp table")
            self.bulk_insert_records(
                full_table_name=f"#{self.full_table_name}",
                schema=self.schema,
                records=context["records"],
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=f"#{self.full_table_name}",
                to_table_name=f"{self.full_table_name}",
                schema=self.schema,
                join_keys=self.key_properties,
            )

        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=self.schema,
                records=context["records"],
            )

    def merge_upsert_from_table(
        self,
        from_table_name: str,
        to_table_name: str,
        schema: dict,
        join_keys: List[str],
    ) -> Optional[int]:
        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22

        join_condition = " and ".join(
            [f"temp.{key} = target.{key}" for key in join_keys]
        )

        update_stmt = ", ".join(
            [
                f"target.{key} = temp.{key}"
                for key in schema["properties"].keys()
                if key not in join_keys
            ]
        )  # noqa

        merge_sql = f"""
            MERGE INTO {to_table_name} AS target
            USING {from_table_name} AS temp
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET
                    { update_stmt }
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(schema["properties"].keys())})
                VALUES ({", ".join([f"temp.{key}" for key in schema["properties"].keys()])});
        """

        if self.key_properties:
            self.connection.execute(f"SET IDENTITY_INSERT { to_table_name } ON")

        self.connection.execute(merge_sql)

        if self.key_properties:
            self.connection.execute(f"SET IDENTITY_INSERT { to_table_name } OFF")

        self.connection.execute("COMMIT")
