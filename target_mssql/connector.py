from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, cast

import sqlalchemy
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.helpers._typing import get_datelike_property_type
from sqlalchemy.dialects import mssql


class mssqlConnector(SQLConnector):
    """The connector for mssql.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def create_table_with_records(
        self,
        full_table_name: Optional[str],
        schema: dict,
        records: Iterable[Dict[str, Any]],
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            records: records to load.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        full_table_name = full_table_name or self.full_table_name
        if primary_keys is None:
            primary_keys = self.key_properties
        partition_keys = partition_keys or None

        self.connector.prepare_table(
            full_table_name=full_table_name,
            primary_keys=primary_keys,
            schema=schema,
            as_temp_table=as_temp_table,
        )
        self.bulk_insert_records(
            full_table_name=full_table_name, schema=schema, records=records
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.
        """

        if config.get("sqlalchemy_url"):
            return config["sqlalchemy_url"]

        connection_url = sqlalchemy.engine.url.URL.create(
            drivername="mssql+pymssql",
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )
        return str(connection_url)

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData()
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys

            columntype = self.to_sql_type(property_jsonschema)

            # In MSSQL, Primary keys can not be more than 900 bytes. Setting at 255
            if isinstance(columntype, sqlalchemy.types.VARCHAR) and is_primary_key:
                columntype = sqlalchemy.types.VARCHAR(255)

            if is_primary_key:
                columns.append(
                    sqlalchemy.Column(
                        property_name, columntype, primary_key=True, autoincrement=False
                    )
                )
            else:
                columns.append(
                    sqlalchemy.Column(property_name, columntype, primary_key=False)
                )

        _ = sqlalchemy.Table(table_name, meta, *columns, schema=schema_name)
        meta.create_all(self._engine)

    def merge_sql_types(  # noqa
        self, sql_types: list[sqlalchemy.types.TypeEngine]
    ) -> sqlalchemy.types.TypeEngine:  # noqa
        """Return a compatible SQL type for the selected type list.
        Args:
            sql_types: List of SQL types.
        Returns:
            A SQL type that is compatible with the input types.
        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        # Gathering Type to match variables
        # sent in _adapt_column_type
        current_type = sql_types[0]
        # sql_type = sql_types[1]

        # Getting the length of each type
        # current_type_len: int = getattr(sql_types[0], "length", 0)
        sql_type_len: int = getattr(sql_types[1], "length", 0)
        if sql_type_len is None:
            sql_type_len = 0

        # Convert the two types given into a sorted list
        # containing the best conversion classes
        sql_types = self._sort_types(sql_types)

        # If greater than two evaluate the first pair then on down the line
        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            if isinstance(generic_type, type):
                if issubclass(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                        (opt_len is None)
                        or (opt_len == 0)
                        or (opt_len >= current_type.length)
                    ):
                        return opt
                elif isinstance(
                    generic_type,
                    (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                        (opt_len is None)
                        or (opt_len == 0)
                        or (opt_len >= current_type.length)
                    ):
                        return opt
                # If best conversion class is equal to current type
                # return the best conversion class
                elif str(opt) == str(current_type):
                    return opt

        raise ValueError(
            f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        )

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.
        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type).split(" ")[0] == str(current_type).split(" ")[0]:
            # Nothing to do
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )
        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) }
                ALTER COLUMN { str(column_name) } { str(compatible_sql_type) }"""
            )
        except Exception as e:
            raise RuntimeError(
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            ) from e

        # self.connection.execute(
        #     sqlalchemy.DDL(
        #         "ALTER TABLE %(table)s ALTER COLUMN %(col_name)s %(col_type)s",
        #         {
        #             "table": full_table_name,
        #             "col_name": column_name,
        #             "col_type": compatible_sql_type,
        #         },
        #     )
        # )

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.
        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                sql_type,
            )
        )

        try:
            self.connection.execute(
                f"""ALTER TABLE { str(full_table_name) }
                ADD { str(create_column_clause) } """
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{create_column_clause}' "
                f"on table '{full_table_name}'."
            ) from e

    def _jsonschema_type_check(
        self, jsonschema_type: dict, type_check: tuple[str]
    ) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
            return True

        return False

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:  # noqa
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(
                        sqlalchemy.types.TypeEngine, sqlalchemy.types.DATETIME()
                    )
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

            maxlength = jsonschema_type.get("maxLength")
            if maxlength is not None:
                if maxlength > 8000:
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TEXT())

            return cast(
                sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(maxlength)
            )

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.BIGINT())

        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            if self.config.get("prefer_float_over_numeric", False):
                return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.FLOAT())
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.NUMERIC(38, 16))

        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, mssql.VARCHAR(1))

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.JSON())

        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR())

    def create_temp_table_from_table(self, from_table_name):
        """Temp table from another table."""

        db_name, schema_name, table_name = self.parse_full_table_name(from_table_name)
        full_table_name = (
            f"{schema_name}.{table_name}" if schema_name else f"{table_name}"
        )
        tmp_full_table_name = (
            f"{schema_name}.#{table_name}" if schema_name else f"#{table_name}"
        )

        # Check if the temporary table exists and drop it if it does
        check_and_drop_table = f"""
        IF OBJECT_ID(N'{tmp_full_table_name}', N'U') IS NOT NULL
        BEGIN
            DROP TABLE {tmp_full_table_name};
        END
        """
        self.connection.execute(check_and_drop_table)

        ddl = f"""
            SELECT TOP 0 *
            into {tmp_full_table_name}
            FROM {full_table_name}
        """  # nosec

        self.connection.execute(ddl)
