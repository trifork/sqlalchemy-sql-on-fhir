"""Apache Superset engine spec for SQL on FHIR servers.

Auto-discovered by Superset via the 'superset.db_engine_specs' entry point.
"""

from __future__ import annotations

from typing import Any

from superset.db_engine_specs.base import BaseEngineSpec


class SqlOnFhirEngineSpec(BaseEngineSpec):
    """Engine spec for querying FHIR data via $sql-run operation."""

    engine = "sqlonfhir"
    engine_name = "SQL on FHIR"
    engine_aliases: set[str] = set()
    drivers = {"rest": "SQL on FHIR REST API"}
    default_driver = "rest"

    sqlalchemy_uri_placeholder = "sqlonfhir://host:port/fhir"

    # Capabilities
    allows_joins = True
    allows_subqueries = True
    allows_alias_in_select = True
    allows_alias_in_orderby = True
    allows_sql_comments = False
    supports_file_upload = False
    disable_ssh_tunneling = True

    # Time grain expressions (Spark SQL syntax — used by e.g. Pathling).
    # Native grains use date_format(date_trunc(...)) — the `from_unixtime(
    # unix_timestamp(...), fmt)` form that Superset's Hive/Spark spec uses
    # is rejected by Pathling's $sql-run during chart-data introspection
    # (its planner errors on the LIMIT-0 form even though the same SQL runs
    # fine in SQL Lab). Sub-hour buckets keep the unix-arithmetic form
    # wrapped in date_format, which avoids the bare from_unixtime call.
    _time_grain_expressions: dict[str | None, str] = {
        None: "{col}",
        "PT1S": "date_format(date_trunc('second', {col}), 'yyyy-MM-dd HH:mm:ss')",  # noqa: E501
        "PT1M": "date_format(date_trunc('minute', {col}), 'yyyy-MM-dd HH:mm:00')",  # noqa: E501
        "PT5M": "date_format(timestamp_seconds(unix_timestamp({col}) - unix_timestamp({col}) % 300), 'yyyy-MM-dd HH:mm:00')",  # noqa: E501
        "PT15M": "date_format(timestamp_seconds(unix_timestamp({col}) - unix_timestamp({col}) % 900), 'yyyy-MM-dd HH:mm:00')",  # noqa: E501
        "PT30M": "date_format(timestamp_seconds(unix_timestamp({col}) - unix_timestamp({col}) % 1800), 'yyyy-MM-dd HH:mm:00')",  # noqa: E501
        "PT1H": "date_format(date_trunc('hour', {col}), 'yyyy-MM-dd HH:00:00')",
        "PT6H": "date_format(timestamp_seconds(unix_timestamp({col}) - unix_timestamp({col}) % 21600), 'yyyy-MM-dd HH:00:00')",  # noqa: E501
        "P1D": "date_format(date_trunc('day', {col}), 'yyyy-MM-dd 00:00:00')",
        "P1W": "date_format(date_trunc('week', {col}), 'yyyy-MM-dd 00:00:00')",
        "P1M": "date_format(date_trunc('month', {col}), 'yyyy-MM-01 00:00:00')",
        "P3M": "date_format(add_months(trunc({col}, 'MM'), -(month({col}) - 1) % 3), 'yyyy-MM-dd 00:00:00')",  # noqa: E501
        "P1Y": "date_format(date_trunc('year', {col}), 'yyyy-01-01 00:00:00')",
    }

    @classmethod
    def get_dbapi_exception_mapping(cls) -> dict[type[Exception], type[Exception]]:
        from sqlonfhir.dbapi.exceptions import (
            DatabaseError,
            OperationalError,
            ProgrammingError,
        )
        from superset.db_engine_specs.exceptions import (
            SupersetDBAPIDatabaseError,
            SupersetDBAPIOperationalError,
            SupersetDBAPIProgrammingError,
        )

        return {
            DatabaseError: SupersetDBAPIDatabaseError,
            OperationalError: SupersetDBAPIOperationalError,
            ProgrammingError: SupersetDBAPIProgrammingError,
        }

    @classmethod
    def get_datatype(cls, type_code: Any) -> str | None:
        """Map DBAPI type objects to Superset-recognised type strings."""
        from sqlonfhir.dbapi.types import BINARY, BOOLEAN, DATETIME, NUMBER, STRING

        if type_code is STRING:
            return "VARCHAR"
        if type_code is NUMBER:
            return "DOUBLE"
        if type_code is DATETIME:
            return "TIMESTAMP"
        if type_code is BOOLEAN:
            return "BOOLEAN"
        if type_code is BINARY:
            return "BLOB"
        if isinstance(type_code, str):
            return type_code.upper()
        return None

    @classmethod
    def get_allow_cost_estimate(cls, extra: dict[str, Any]) -> bool:
        return False

    @classmethod
    def get_schema_names(cls, inspector: Any) -> set[str]:
        return {"default"}
