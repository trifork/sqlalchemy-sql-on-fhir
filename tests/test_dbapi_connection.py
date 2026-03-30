"""Tests for the SQL on FHIR DBAPI Connection."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import sqlonfhir.dbapi
from sqlonfhir.dbapi.connection import Connection
from sqlonfhir.dbapi.exceptions import InterfaceError, OperationalError
from tests.conftest import SAMPLE_VIEW_DEFINITION_BUNDLE, _make_mock_response


def test_connection_loads_view_definitions(connection: Connection):
    """ViewDefinitions are loaded on connection init."""
    assert "patients" in connection._view_definitions
    assert "conditions" in connection._view_definitions


def test_connection_view_definition_metadata(connection: Connection):
    """ViewDefinition cache contains correct metadata."""
    patients = connection._view_definitions["patients"]
    assert patients["id"] == "vd-patients-1"
    assert patients["resource_type"] == "Patient"
    assert len(patients["columns"]) == 5
    col_names = [c["name"] for c in patients["columns"]]
    assert "patient_id" in col_names
    assert "gender" in col_names
    assert "family_name" in col_names


def test_connection_base_url(connection: Connection):
    assert connection.base_url == "http://localhost:8080/fhir"


def test_connection_cursor_returns_cursor(connection: Connection):
    cursor = connection.cursor()
    assert cursor is not None
    assert cursor._connection is connection


def test_connection_close(connection: Connection):
    connection.close()
    with pytest.raises(InterfaceError, match="closed"):
        connection.cursor()


def test_connection_commit_noop(connection: Connection):
    connection.commit()  # should not raise


def test_connection_rollback_noop(connection: Connection):
    connection.rollback()  # should not raise


def test_connection_auth_token():
    """Bearer token is set in session headers."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session
        session.get.return_value = _make_mock_response(
            json_data={"resourceType": "Bundle", "entry": []},
        )

        conn = sqlonfhir.dbapi.connect(host="localhost", token="my-secret-token")
        assert session.headers.__setitem__.call_args_list[0] == (
            ("Accept", "application/fhir+json"),
        )
        # Check Authorization header was set
        auth_calls = [
            call
            for call in session.headers.__setitem__.call_args_list
            if call[0][0] == "Authorization"
        ]
        assert len(auth_calls) == 1
        assert auth_calls[0][0][1] == "Bearer my-secret-token"


def test_connection_auth_basic():
    """Basic auth is set on the session."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session
        session.get.return_value = _make_mock_response(
            json_data={"resourceType": "Bundle", "entry": []},
        )

        conn = sqlonfhir.dbapi.connect(
            host="localhost", username="user", password="pass"
        )
        assert session.auth == ("user", "pass")


def test_connection_error_on_failed_vd_fetch():
    """OperationalError raised when ViewDefinition fetch fails."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session
        resp = MagicMock()
        resp.raise_for_status.side_effect = Exception("HTTP 500")
        resp.ok = False
        resp.status_code = 500
        session.get.return_value = resp

        import requests

        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("500")
        with pytest.raises(OperationalError, match="Failed to fetch"):
            sqlonfhir.dbapi.connect(host="localhost")


def test_connection_pagination():
    """ViewDefinition loading follows pagination links."""
    page1 = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "ViewDefinition",
                    "id": "vd-1",
                    "name": "table1",
                    "resource": "Patient",
                    "select": [{"column": [{"name": "id", "type": "id"}]}],
                }
            }
        ],
        "link": [
            {"relation": "self", "url": "http://localhost:8080/fhir/ViewDefinition"},
            {"relation": "next", "url": "http://localhost:8080/fhir/ViewDefinition?page=2"},
        ],
    }
    page2 = {
        "resourceType": "Bundle",
        "entry": [
            {
                "resource": {
                    "resourceType": "ViewDefinition",
                    "id": "vd-2",
                    "name": "table2",
                    "resource": "Condition",
                    "select": [{"column": [{"name": "id", "type": "id"}]}],
                }
            }
        ],
        "link": [{"relation": "self", "url": "http://localhost:8080/fhir/ViewDefinition?page=2"}],
    }

    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session

        session.get.side_effect = [
            _make_mock_response(json_data=page1),
            _make_mock_response(json_data=page2),
        ]

        conn = sqlonfhir.dbapi.connect(host="localhost")
        assert "table1" in conn._view_definitions
        assert "table2" in conn._view_definitions
        assert session.get.call_count == 2
