"""PEP-249 DBAPI 2.0 driver for SQL on FHIR servers.

Usage::

    import sqlonfhir.dbapi
    conn = sqlonfhir.dbapi.connect(host="localhost", port=8080)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM patients LIMIT 10")
    rows = cursor.fetchall()
"""

from sqlonfhir.dbapi.connection import Connection
from sqlonfhir.dbapi.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from sqlonfhir.dbapi.types import BINARY, BOOLEAN, DATETIME, NUMBER, ROWID, STRING

# PEP-249 module-level attributes
apilevel = "2.0"
threadsafety = 1  # threads may share module but not connections
paramstyle = "pyformat"  # %(name)s style


def connect(
    host: str = "localhost",
    port: int = 8080,
    path: str = "/fhir",
    scheme: str = "http",
    token: str | None = None,
    username: str | None = None,
    password: str | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 300,
    verify_ssl: bool = True,
) -> Connection:
    """Create a new DBAPI connection to a SQL on FHIR server."""
    return Connection(
        host=host,
        port=port,
        path=path,
        scheme=scheme,
        token=token,
        username=username,
        password=password,
        headers=headers,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )


__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "BOOLEAN",
    "ROWID",
]
