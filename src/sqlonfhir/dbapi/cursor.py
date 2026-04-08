"""DBAPI 2.0 Cursor for SQL on FHIR servers.

Translates SQL queries into FHIR $sqlquery-run HTTP POST requests,
mapping table names to ViewDefinition references.
"""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

import requests
import sqlglot
from sqlglot import exp

from sqlonfhir.dbapi.exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from sqlonfhir.dbapi.types import FHIR_TYPE_TO_DBAPI, STRING, infer_type_from_value

if TYPE_CHECKING:
    from sqlonfhir.dbapi.connection import Connection


class Cursor:
    """A DBAPI 2.0 cursor that executes SQL via Pathling's $sqlquery-run operation."""

    arraysize: int = 100

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._closed = False
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self.description: list[tuple[Any, ...]] | None = None
        self.rowcount: int = -1
        self._last_operation: str | None = None

    def execute(self, operation: str, parameters: dict[str, Any] | None = None) -> None:
        """Execute a SQL query via the Pathling $sqlquery-run operation.

        1. Parse table names from the SQL using sqlglot.
        2. Map each table name to a ViewDefinition ID from the connection cache.
        3. Build the FHIR Parameters resource with a Library containing the SQL.
        4. POST to $sqlquery-run and parse the response.
        """
        self._check_closed()
        self._rows = []
        self._row_index = 0
        self.description = None
        self.rowcount = -1

        # Transpile to Spark SQL so that Pathling's Spark engine accepts the query.
        # This converts ANSI double-quoted identifiers (e.g. "col") to backtick-quoted
        # identifiers (e.g. `col`) and normalises other dialect differences.
        # Also strips schema prefixes (e.g. `default`.table -> table) since Pathling
        # has no schema concept — the dialect returns "default" only for SQL Lab UX.
        try:
            tree = sqlglot.parse_one(operation, dialect="spark")
            for table in tree.find_all(exp.Table):
                if table.args.get("db"):
                    table.set("db", None)
            operation = tree.sql(dialect="spark")
        except sqlglot.errors.ParseError:
            pass  # Send original SQL and let the server report the error

        self._last_operation = operation
        table_names = self._extract_table_names(operation)
        related_artifacts = self._build_related_artifacts(table_names)
        fhir_params = self._build_fhir_parameters(operation, related_artifacts, parameters)

        url = f"{self._connection.base_url}/$sqlquery-run"
        query_params = {"_format": "json"}

        try:
            resp = self._connection._session.post(
                url,
                json=fhir_params,
                params=query_params,
                headers={"Content-Type": "application/fhir+json"},
                timeout=self._connection.timeout,
            )
        except requests.exceptions.ConnectionError as e:
            raise OperationalError(f"Connection failed: {e}") from e
        except requests.exceptions.Timeout as e:
            raise OperationalError(f"Request timed out: {e}") from e

        if not resp.ok:
            self._handle_error_response(resp)

        self._parse_response(resp)

    def executemany(
        self, operation: str, seq_of_parameters: list[dict[str, Any]]
    ) -> None:
        """Execute a SQL query for each set of parameters."""
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of the result set."""
        self._check_closed()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows."""
        self._check_closed()
        if size is None:
            size = self.arraysize
        end = min(self._row_index + size, len(self._rows))
        rows = self._rows[self._row_index : end]
        self._row_index = end
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of the result set."""
        self._check_closed()
        rows = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return rows

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._rows = []
        self.description = None

    def setinputsizes(self, sizes: Any) -> None:
        """No-op per PEP-249."""

    def setoutputsize(self, size: Any, column: int | None = None) -> None:
        """No-op per PEP-249."""

    @property
    def connection(self) -> Connection:
        return self._connection

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    # -- Internal methods --

    def _extract_projected_columns(self, sql: str) -> list[str] | None:
        """Return the column aliases from the outermost SELECT, or None if unparseable.

        Pathling omits null fields from JSON responses, so the result set may
        contain fewer columns than the SQL projects. This method lets us fill
        the gaps with None so the cursor description is complete.
        """
        try:
            statements = sqlglot.parse(sql, dialect="spark")
            if not statements:
                return None
            stmt = statements[0]
            if not isinstance(stmt, exp.Select):
                return None
            cols: list[str] = []
            for sel in stmt.selects:
                if isinstance(sel, exp.Star):
                    return None  # SELECT * — can't enumerate statically
                alias = sel.alias
                if alias:
                    cols.append(alias)
                elif isinstance(sel, exp.Column):
                    cols.append(sel.name)
                else:
                    # Expression without alias — use the SQL text as the name
                    cols.append(sel.sql(dialect="spark"))
            return cols if cols else None
        except sqlglot.errors.ParseError:
            return None

    def _extract_table_names(self, sql: str) -> set[str]:
        """Extract table names from SQL using sqlglot AST parsing."""
        table_names: set[str] = set()
        try:
            # Use Spark dialect since Pathling uses Spark SQL under the hood
            for statement in sqlglot.parse(sql, dialect="spark"):
                if statement is None:
                    continue
                for table in statement.find_all(exp.Table):
                    name = table.name
                    if name:
                        table_names.add(name)
        except sqlglot.errors.ParseError:
            # If parsing fails, fall back: send the SQL as-is and let the server
            # handle it. We won't know table names, so relatedArtifact will be empty.
            pass
        return table_names

    def _build_related_artifacts(
        self, table_names: set[str]
    ) -> list[dict[str, Any]]:
        """Map table names to FHIR relatedArtifact entries."""
        artifacts: list[dict[str, Any]] = []
        view_defs = self._connection._view_definitions

        for name in table_names:
            if name not in view_defs:
                raise ProgrammingError(
                    f"Table '{name}' not found. Available tables: "
                    f"{', '.join(sorted(view_defs.keys()))}"
                )
            vd = view_defs[name]
            artifacts.append(
                {
                    "type": "depends-on",
                    "label": name,
                    "resource": f"ViewDefinition/{vd['id']}",
                }
            )
        return artifacts

    def _build_fhir_parameters(
        self,
        sql: str,
        related_artifacts: list[dict[str, Any]],
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the FHIR Parameters resource for $sqlquery-run."""
        sql_b64 = base64.b64encode(sql.encode("utf-8")).decode("ascii")

        library: dict[str, Any] = {
            "resourceType": "Library",
            "status": "active",
            "type": {"coding": [{"code": "logic-library"}]},
            "content": [
                {
                    "contentType": "application/sql",
                    "data": sql_b64,
                }
            ],
            "relatedArtifact": related_artifacts,
        }

        params_list: list[dict[str, Any]] = [
            {
                "name": "queryResource",
                "resource": library,
            }
        ]

        # Add query parameter bindings
        if parameters:
            for param_name, param_value in parameters.items():
                params_list.append(
                    {
                        "name": "parameter",
                        "part": [
                            {"name": "name", "valueString": param_name},
                            {"name": "value", "valueString": str(param_value)},
                        ],
                    }
                )

        return {
            "resourceType": "Parameters",
            "parameter": params_list,
        }

    def _handle_error_response(self, resp: requests.Response) -> None:
        """Map HTTP error responses to DBAPI exceptions."""
        try:
            body = resp.json()
            # Try to extract FHIR OperationOutcome diagnostics
            diagnostics = []
            for issue in body.get("issue", []):
                diag = issue.get("diagnostics", "")
                if diag:
                    diagnostics.append(diag)
            message = "; ".join(diagnostics) if diagnostics else resp.text
        except (json.JSONDecodeError, ValueError):
            message = resp.text

        if resp.status_code in (401, 403):
            raise OperationalError(f"Authentication/authorization failed: {message}")
        if resp.status_code == 404:
            raise ProgrammingError(f"Resource not found: {message}")
        if resp.status_code == 400:
            raise ProgrammingError(f"Bad request: {message}")
        raise DatabaseError(f"Server error ({resp.status_code}): {message}")

    def _parse_response(self, resp: requests.Response) -> None:
        """Parse the $sqlquery-run JSON response into rows and description."""
        content_type = resp.headers.get("Content-Type", "")

        if "application/x-ndjson" in content_type or "ndjson" in content_type:
            self._parse_ndjson(resp.text)
        elif "application/json" in content_type or "json" in content_type:
            self._parse_json(resp)
        else:
            # Try JSON first, fall back to NDJSON
            try:
                self._parse_json(resp)
            except (json.JSONDecodeError, ValueError):
                self._parse_ndjson(resp.text)

    def _parse_json(self, resp: requests.Response) -> None:
        """Parse a JSON array response."""
        data = resp.json()

        # Handle both plain JSON array and FHIR-wrapped responses
        if isinstance(data, list):
            rows_data = data
        elif isinstance(data, dict):
            # Could be a FHIR Parameters response or a single-object response
            rows_data = [data]
        else:
            self._rows = []
            self.rowcount = 0
            return

        if not rows_data:
            self._rows = []
            self.rowcount = 0
            self.description = []
            return

        # Pathling omits null fields from JSON, so derive the authoritative column
        # list from the SQL when possible, falling back to the response keys.
        response_keys = list(rows_data[0].keys())
        projected = (
            self._extract_projected_columns(self._last_operation)
            if self._last_operation
            else None
        )
        if projected and len(projected) >= len(response_keys):
            col_names = projected
        else:
            col_names = response_keys

        # Build description from column names and infer types from values
        self.description = []
        for col_name in col_names:
            first_val = rows_data[0].get(col_name)
            type_code = infer_type_from_value(first_val)
            # PEP-249 description: (name, type_code, display_size, internal_size,
            #                       precision, scale, null_ok)
            self.description.append(
                (col_name, type_code, None, None, None, None, True)
            )

        # Convert rows to tuples (None for columns Pathling omitted)
        self._rows = [
            tuple(row.get(col) for col in col_names) for row in rows_data
        ]
        self._row_index = 0
        self.rowcount = len(self._rows)

    def _parse_ndjson(self, text: str) -> None:
        """Parse an NDJSON response (one JSON object per line)."""
        lines = [line.strip() for line in text.strip().split("\n") if line.strip()]
        if not lines:
            self._rows = []
            self.rowcount = 0
            self.description = []
            return

        rows_data = [json.loads(line) for line in lines]

        response_keys = list(rows_data[0].keys())
        projected = (
            self._extract_projected_columns(self._last_operation)
            if self._last_operation
            else None
        )
        col_names = (
            projected
            if projected and len(projected) >= len(response_keys)
            else response_keys
        )

        self.description = []
        for col_name in col_names:
            first_val = rows_data[0].get(col_name)
            type_code = infer_type_from_value(first_val)
            self.description.append(
                (col_name, type_code, None, None, None, None, True)
            )

        self._rows = [
            tuple(row.get(col) for col in col_names) for row in rows_data
        ]
        self._row_index = 0
        self.rowcount = len(self._rows)

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")
