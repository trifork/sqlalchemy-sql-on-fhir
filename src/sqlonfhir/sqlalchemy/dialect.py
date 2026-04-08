"""SQLAlchemy dialect for SQL-on-FHIR servers.

Registers as the 'sqlonfhir' dialect, enabling::

    from sqlalchemy import create_engine
    engine = create_engine("sqlonfhir://localhost:8080/fhir")
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import types as sqla_types
from sqlalchemy.engine import default
from sqlalchemy.sql.compiler import IdentifierPreparer


class _SparkIdentifierPreparer(IdentifierPreparer):
    """Use backtick quoting so generated SQL is valid Spark SQL."""

    def __init__(self, dialect: Any) -> None:
        super().__init__(dialect, initial_quote="`", final_quote="`")


# FHIR type string -> SQLAlchemy type
_FHIR_TO_SQLA: dict[str, sqla_types.TypeEngine] = {
    "string": sqla_types.String(),
    "code": sqla_types.String(),
    "id": sqla_types.String(),
    "uri": sqla_types.String(),
    "url": sqla_types.String(),
    "markdown": sqla_types.Text(),
    "base64Binary": sqla_types.LargeBinary(),
    "boolean": sqla_types.Boolean(),
    "integer": sqla_types.Integer(),
    "positiveInt": sqla_types.Integer(),
    "unsignedInt": sqla_types.Integer(),
    "decimal": sqla_types.Numeric(),
    "date": sqla_types.Date(),
    "dateTime": sqla_types.DateTime(),
    "instant": sqla_types.DateTime(),
    "time": sqla_types.Time(),
}


def _fhir_type_to_sqla(fhir_type: str) -> sqla_types.TypeEngine:
    """Map a FHIR type string to a SQLAlchemy TypeEngine."""
    return _FHIR_TO_SQLA.get(fhir_type, sqla_types.String())


class SqlOnFhirDialect(default.DefaultDialect):
    """SQLAlchemy dialect for SQL on FHIR servers."""

    name = "sqlonfhir"
    driver = "rest"
    preparer = _SparkIdentifierPreparer

    supports_alter = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_statement_cache = False
    returns_unicode_strings = True
    postfetch_lastrowid = False
    preexecute_autoincrement_sequences = False

    @classmethod
    def dbapi(cls) -> Any:
        import sqlonfhir.dbapi

        return sqlonfhir.dbapi

    @classmethod
    def import_dbapi(cls) -> Any:
        import sqlonfhir.dbapi

        return sqlonfhir.dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[str, Any]]:
        """Translate a SQLAlchemy URL into DBAPI connect() kwargs.

        URL format: sqlonfhir://[user:password@]host[:port]/path
        Example:    sqlonfhir://localhost:8080/fhir
        """
        kwargs: dict[str, Any] = {
            "host": url.host or "localhost",
            "port": url.port or 8080,
        }

        # The database part of the URL becomes the FHIR base path
        if url.database:
            kwargs["path"] = f"/{url.database}"
        else:
            kwargs["path"] = "/fhir"

        # Determine scheme
        query = dict(url.query) if url.query else {}
        if "scheme" in query:
            kwargs["scheme"] = query.pop("scheme")
        elif url.port == 443:
            kwargs["scheme"] = "https"
        else:
            kwargs["scheme"] = "http"

        if url.username:
            kwargs["username"] = url.username
        if url.password:
            kwargs["password"] = url.password

        if "token" in query:
            kwargs["token"] = query.pop("token")

        if "timeout" in query:
            kwargs["timeout"] = int(query.pop("timeout"))

        if "verify_ssl" in query:
            kwargs["verify_ssl"] = query.pop("verify_ssl").lower() in (
                "true",
                "1",
                "yes",
            )

        return [], kwargs

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Verify the connection is alive by checking the server metadata."""
        try:
            resp = dbapi_connection._session.get(
                f"{dbapi_connection.base_url}/metadata",
                timeout=10,
            )
            return resp.ok
        except Exception:
            return False

    def get_schema_names(self, connection: Any, **kwargs: Any) -> list[str]:
        return ["default"]

    def get_table_names(
        self, connection: Any, schema: str | None = None, **kwargs: Any
    ) -> list[str]:
        """Return ViewDefinition names as table names."""
        raw_conn = connection.connection.dbapi_connection
        return sorted(raw_conn._view_definitions.keys())

    def get_view_names(
        self, connection: Any, schema: str | None = None, **kwargs: Any
    ) -> list[str]:
        return []

    def get_columns(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return column metadata from the ViewDefinition."""
        raw_conn = connection.connection.dbapi_connection
        vd = raw_conn._view_definitions.get(table_name)
        if not vd:
            from sqlalchemy.exc import NoSuchTableError

            raise NoSuchTableError(table_name)

        columns: list[ReflectedColumn] = []
        for col in vd.get("columns", []):
            columns.append(
                {
                    "name": col["name"],
                    "type": _fhir_type_to_sqla(col.get("type", "string")),
                    "nullable": True,
                    "default": None,
                }
            )
        return columns

    def get_pk_constraint(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return []

    def get_indexes(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return []

    def has_table(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> bool:
        raw_conn = connection.connection.dbapi_connection
        return table_name in raw_conn._view_definitions
