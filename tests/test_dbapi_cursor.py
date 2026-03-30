"""Tests for the SQL on FHIR DBAPI Cursor."""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock

import pytest

from sqlonfhir.dbapi.connection import Connection
from sqlonfhir.dbapi.cursor import Cursor
from sqlonfhir.dbapi.exceptions import InterfaceError, OperationalError, ProgrammingError
from tests.conftest import (
    SAMPLE_NDJSON_RESPONSE,
    SAMPLE_QUERY_RESPONSE,
    _make_mock_response,
)


def test_execute_simple_select(connection: Connection):
    """Execute a simple SELECT against a single table."""
    cursor = connection.cursor()
    cursor.execute("SELECT patient_id, gender FROM patients LIMIT 3")

    assert cursor.description is not None
    assert len(cursor.description) == 3  # columns from response
    assert cursor.rowcount == 3

    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == ("pat-1", "male", "1990-05-15")


def test_execute_builds_correct_fhir_params(connection: Connection):
    """Verify the FHIR Parameters resource sent to the server."""
    cursor = connection.cursor()
    sql = "SELECT patient_id FROM patients"
    cursor.execute(sql)

    # Check the POST call
    call_args = connection._session.post.call_args
    body = call_args.kwargs.get("json") or call_args[1].get("json")

    assert body["resourceType"] == "Parameters"
    params = body["parameter"]
    assert len(params) == 1
    assert params[0]["name"] == "queryResource"

    library = params[0]["resource"]
    assert library["resourceType"] == "Library"
    assert library["content"][0]["contentType"] == "application/sql"

    # Verify SQL is base64-encoded
    decoded_sql = base64.b64decode(library["content"][0]["data"]).decode("utf-8")
    assert decoded_sql == sql

    # Verify relatedArtifact
    artifacts = library["relatedArtifact"]
    assert len(artifacts) == 1
    assert artifacts[0]["label"] == "patients"
    assert artifacts[0]["resource"] == "ViewDefinition/vd-patients-1"


def test_execute_join_multiple_tables(connection: Connection):
    """Execute a JOIN query referencing multiple ViewDefinitions."""
    cursor = connection.cursor()
    sql = (
        "SELECT p.given_name, c.condition_name "
        "FROM patients p "
        "JOIN conditions c ON p.patient_id = c.patient_ref"
    )
    cursor.execute(sql)

    call_args = connection._session.post.call_args
    body = call_args.kwargs.get("json") or call_args[1].get("json")
    library = body["parameter"][0]["resource"]
    artifacts = library["relatedArtifact"]

    labels = {a["label"] for a in artifacts}
    assert labels == {"patients", "conditions"}


def test_execute_unknown_table_raises_error(connection: Connection):
    """ProgrammingError raised for unknown table names."""
    cursor = connection.cursor()
    with pytest.raises(ProgrammingError, match="Table 'nonexistent' not found"):
        cursor.execute("SELECT * FROM nonexistent")


def test_fetchone(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    row1 = cursor.fetchone()
    assert row1 is not None
    assert row1[0] == "pat-1"

    row2 = cursor.fetchone()
    assert row2 is not None
    assert row2[0] == "pat-2"


def test_fetchmany(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    rows = cursor.fetchmany(2)
    assert len(rows) == 2

    remaining = cursor.fetchmany(10)
    assert len(remaining) == 1


def test_fetchall_after_partial_fetch(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    cursor.fetchone()
    remaining = cursor.fetchall()
    assert len(remaining) == 2


def test_cursor_iteration(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    rows = list(cursor)
    assert len(rows) == 3


def test_cursor_close(connection: Connection):
    cursor = connection.cursor()
    cursor.close()
    with pytest.raises(InterfaceError, match="closed"):
        cursor.execute("SELECT * FROM patients")


def test_ndjson_response_parsing(connection: Connection):
    """Cursor correctly parses NDJSON responses."""
    ndjson_resp = _make_mock_response(
        text=SAMPLE_NDJSON_RESPONSE,
        content_type="application/x-ndjson",
    )
    # Override json() to raise since NDJSON is not valid JSON
    ndjson_resp.json.side_effect = ValueError("not json")
    connection._session.post.return_value = ndjson_resp

    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    assert cursor.rowcount == 3
    rows = cursor.fetchall()
    assert rows[0][0] == "pat-1"


def test_error_response_400(connection: Connection):
    """400 responses raise ProgrammingError."""
    error_resp = _make_mock_response(
        json_data={
            "resourceType": "OperationOutcome",
            "issue": [
                {"severity": "error", "diagnostics": "SQL syntax error near 'SELCT'"}
            ],
        },
        status_code=400,
        content_type="application/fhir+json",
    )
    connection._session.post.return_value = error_resp

    cursor = connection.cursor()
    with pytest.raises(ProgrammingError, match="SQL syntax error"):
        cursor.execute("SELCT * FROM patients")


def test_error_response_401(connection: Connection):
    """401 responses raise OperationalError."""
    error_resp = _make_mock_response(
        json_data={
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "error", "diagnostics": "Unauthorized"}],
        },
        status_code=401,
        content_type="application/fhir+json",
    )
    connection._session.post.return_value = error_resp

    cursor = connection.cursor()
    with pytest.raises(OperationalError, match="Authentication"):
        cursor.execute("SELECT * FROM patients")


def test_empty_result(connection: Connection):
    """Empty result sets are handled correctly."""
    empty_resp = _make_mock_response(
        json_data=[],
        content_type="application/json",
    )
    connection._session.post.return_value = empty_resp

    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients WHERE 1=0")

    assert cursor.rowcount == 0
    assert cursor.description == []
    assert cursor.fetchall() == []


def test_execute_with_parameters(connection: Connection):
    """Query parameters are included in the FHIR request."""
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM patients WHERE gender = :gender",
        {"gender": "male"},
    )

    call_args = connection._session.post.call_args
    body = call_args.kwargs.get("json") or call_args[1].get("json")
    params = body["parameter"]

    # Should have queryResource + 1 parameter binding
    assert len(params) == 2
    param_binding = params[1]
    assert param_binding["name"] == "parameter"
    parts = {p["name"]: p.get("valueString") for p in param_binding["part"]}
    assert parts["name"] == "gender"
    assert parts["value"] == "male"


def test_extract_table_names_subquery(connection: Connection):
    """Table extraction handles subqueries."""
    cursor = connection.cursor()
    names = cursor._extract_table_names(
        "SELECT * FROM (SELECT * FROM patients) sub JOIN conditions c ON 1=1"
    )
    assert "patients" in names
    assert "conditions" in names
    assert "sub" not in names  # alias, not a table


def test_extract_table_names_cte(connection: Connection):
    """Table extraction handles CTEs."""
    cursor = connection.cursor()
    names = cursor._extract_table_names(
        "WITH p AS (SELECT * FROM patients) SELECT * FROM p JOIN conditions ON 1=1"
    )
    assert "patients" in names
    assert "conditions" in names


def test_description_column_names(connection: Connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM patients")

    col_names = [d[0] for d in cursor.description]
    assert col_names == ["patient_id", "gender", "birth_date"]
