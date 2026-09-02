"""DBAPI 2.0 Cursor for SQL-on-FHIR servers.

Translates SQL queries into FHIR `$sql-run` HTTP POST requests, mapping
table names to ViewDefinition references.
"""

from __future__ import annotations

import base64
import datetime as _dt
import json
from typing import TYPE_CHECKING, Any

import requests
import sqlglot
from sqlglot import exp


def _runtime_parameter_entry(name: str, value: Any) -> dict[str, Any]:
    """Encode a Python parameter value as a FHIR Parameters.parameter entry.

    Maps Python types to the corresponding FHIR primitive value[x] field.
    """
    if isinstance(value, bool):
        return {"name": name, "valueBoolean": value}
    if isinstance(value, int):
        return {"name": name, "valueInteger": value}
    if isinstance(value, float):
        return {"name": name, "valueDecimal": value}
    if isinstance(value, _dt.datetime):
        return {"name": name, "valueDateTime": value.isoformat()}
    if isinstance(value, _dt.date):
        return {"name": name, "valueDate": value.isoformat()}
    if isinstance(value, _dt.time):
        return {"name": name, "valueTime": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {
            "name": name,
            "valueBase64Binary": base64.b64encode(bytes(value)).decode("ascii"),
        }
    return {"name": name, "valueString": str(value)}


def _fhir_parameter_type(value: Any) -> str:
    """Return the FHIR type code for a Library.parameter ParameterDefinition.

    Must match the value[x] field chosen by `_runtime_parameter_entry` above —
    Pathling validates each bound parameter against the Library's declared
    parameter list and rejects a binding whose declared type doesn't match.
    """
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, _dt.datetime):
        return "dateTime"
    if isinstance(value, _dt.date):
        return "date"
    if isinstance(value, _dt.time):
        return "time"
    if isinstance(value, (bytes, bytearray)):
        return "base64Binary"
    return "string"


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
    """A DBAPI 2.0 cursor that executes SQL via the `$sql-run` operation."""

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
        """Execute a SQL query via the `$sql-run` operation.

        1. Parse table names from the SQL using sqlglot.
        2. Map each table name to a ViewDefinition from the connection cache.
        3. Build the FHIR Parameters resource: a Library subject referencing
           each dependency by a synthetic canonical URL, plus one `context`
           parameter per dependency carrying the full ViewDefinition inline
           (Pathling resolves relatedArtifact dependencies by canonical URL,
           not by relative reference).
        4. POST to $sql-run and parse the response.
        """
        self._check_closed()
        self._rows = []
        self._row_index = 0
        self.description = None
        self.rowcount = -1

        # Translate ANSI-style SQL (the input we get from SQLAlchemy /
        # Superset) into Spark SQL (what Pathling's $sql-run requires).
        # Pathling rejects ANSI double-quoted identifiers — `... AS "P" ...
        # ORDER BY "P"` returns PARSE_SYNTAX_ERROR — so the conversion to
        # backticks is mandatory, not cosmetic. Also strips schema prefixes
        # (`default`.table -> table) since SQL-on-FHIR has no schema concept
        # and the dialect only returns "default" for SQL Lab UX.
        #
        # The parser dialect must be ANSI-aware, NOT spark, even though the
        # target *is* Spark. sqlglot's spark parser treats `"X"` as a string
        # literal in expression position; with that parser, `ORDER BY "X"`
        # becomes `ORDER BY 'X'` on emit — a sort by a constant string, which
        # silently corrupts top-N queries (the rows come back in arbitrary
        # order). duckdb is sqlglot's most ANSI-faithful parser and emits
        # nothing exotic when transpiling to spark. Tradeoff: a raw Spark
        # query that uses `"foo"` as a string literal (only valid in Spark
        # default non-ANSI mode) is reinterpreted as a column reference.
        # In practice every caller — SQLAlchemy compilation, Superset SQL
        # Lab — uses single quotes for strings, so this is acceptable.
        try:
            tree = sqlglot.parse_one(operation, dialect="duckdb")
            for table in tree.find_all(exp.Table):
                if table.args.get("db"):
                    table.set("db", None)
            operation = tree.sql(dialect="spark")
        except sqlglot.errors.ParseError:
            pass  # Send original SQL and let the server report the error

        self._last_operation = operation
        table_names = self._extract_table_names(operation)
        related_artifacts, context = self._build_dependencies(table_names)
        fhir_params = self._build_fhir_parameters(
            operation, related_artifacts, context, parameters
        )

        url = f"{self._connection.base_url}/$sql-run"
        query_params = {"_format": "json"}

        try:
            resp = self._connection._request(
                "POST",
                url,
                json=fhir_params,
                params=query_params,
                headers={"Content-Type": "application/fhir+json"},
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

        Some SQL-on-FHIR servers (e.g. Pathling) omit null fields from JSON
        responses, so the result set may contain fewer columns than the SQL
        projects. This method lets us fill the gaps with None so the cursor
        description is complete.
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
                    expanded = self._expand_star(stmt)
                    if expanded is None:
                        return None  # can't expand — fall back to response keys
                    cols.extend(expanded)
                    continue
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

    def _expand_star(self, stmt: exp.Select) -> list[str] | None:
        """Resolve a bare `SELECT *` against the cached ViewDefinition.

        Returns the column list when the FROM is a single known table (no
        joins, no subqueries). Otherwise returns None and the caller falls
        back to whatever keys the response carries.
        """
        if stmt.args.get("joins"):
            return None
        # sqlglot >=30 uses the key "from_"; older versions used "from".
        from_ = stmt.args.get("from_") or stmt.args.get("from")
        if from_ is None:
            return None
        src = from_.this
        if not isinstance(src, exp.Table):
            return None
        vd = self._connection._view_definitions.get(src.name)
        if vd is None:
            return None
        return [c["name"] for c in vd["columns"]]

    @staticmethod
    def _merge_response_keys(rows_data: list[dict[str, Any]]) -> list[str]:
        """Union of keys across all rows, preserving first-seen order.

        Pathling omits null fields from JSON, so the first row's keys alone
        can miss columns present further down the result set.
        """
        seen: dict[str, None] = {}
        for row in rows_data:
            for k in row:
                seen.setdefault(k, None)
        return list(seen)

    def _extract_table_names(self, sql: str) -> set[str]:
        """Extract table names from SQL using sqlglot AST parsing."""
        table_names: set[str] = set()
        try:
            # Use Spark dialect for parsing — matches what servers like Pathling
            # run under the hood, and is harmless for plain ANSI SQL.
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

    def _build_dependencies(
        self, table_names: set[str]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Map table names to relatedArtifact entries and inline `context` resources.

        Pathling resolves a Library's relatedArtifact dependencies by matching
        `relatedArtifact.resource` against a supplied `context` resource's
        `url` — a relative reference like `ViewDefinition/{id}` is rejected as
        not being an absolute canonical URL, and there's no requirement that
        the canonical actually resolve on the server, so a synthetic per-view
        urn is enough as long as the same value is used in both places.
        """
        artifacts: list[dict[str, Any]] = []
        context: list[dict[str, Any]] = []
        view_defs = self._connection._view_definitions

        for name in table_names:
            if name not in view_defs:
                raise ProgrammingError(
                    f"Table '{name}' not found. Available tables: "
                    f"{', '.join(sorted(view_defs.keys()))}"
                )
            vd = view_defs[name]
            canonical_url = f"urn:sqlonfhir:view-definition:{vd['id']}"
            artifacts.append(
                {
                    "type": "depends-on",
                    "label": name,
                    "resource": canonical_url,
                }
            )
            context.append({**vd["resource"], "url": canonical_url})
        return artifacts, context

    def _build_fhir_parameters(
        self,
        sql: str,
        related_artifacts: list[dict[str, Any]],
        context: list[dict[str, Any]],
        parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the FHIR Parameters resource for $sql-run."""
        sql_b64 = base64.b64encode(sql.encode("utf-8")).decode("ascii")

        library: dict[str, Any] = {
            "resourceType": "Library",
            "status": "active",
            "type": {
                "coding": [
                    {
                        "system": "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
                        "code": "sql-query",
                    }
                ]
            },
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
                "name": "subjectResource",
                "resource": library,
            }
        ]
        params_list.extend({"name": "context", "resource": vd} for vd in context)

        # Runtime parameter bindings — wrapped in a single typed Parameters resource
        # so that each value carries its FHIR type rather than being string-coerced.
        # Pathling also requires each bound name to be declared up front on the
        # Library, with a matching type, or it rejects the binding.
        if parameters:
            library["parameter"] = [
                {"name": name, "use": "in", "type": _fhir_parameter_type(value)}
                for name, value in parameters.items()
            ]
            inner_params = [
                _runtime_parameter_entry(name, value)
                for name, value in parameters.items()
            ]
            params_list.append(
                {
                    "name": "parameters",
                    "resource": {
                        "resourceType": "Parameters",
                        "parameter": inner_params,
                    },
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
        if resp.status_code == 422:
            # Pathling 3.0+ reports Spark analysis errors (e.g. a type-mismatch
            # in the query) as 422 with a real diagnostic, rather than the
            # opaque 500 it used to return for the same class of error.
            raise ProgrammingError(f"Query error: {message}")
        raise DatabaseError(f"Server error ({resp.status_code}): {message}")

    def _parse_response(self, resp: requests.Response) -> None:
        """Parse the $sql-run JSON response into rows and description."""
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

        # Some servers (e.g. Pathling) omit null fields from JSON, so derive
        # the authoritative column list from the SQL when possible, falling
        # back to the response keys.
        response_keys = self._merge_response_keys(rows_data)
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

        # Convert rows to tuples (None for omitted columns)
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

        response_keys = self._merge_response_keys(rows_data)
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
