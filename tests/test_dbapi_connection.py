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


def test_connection_oauth_fetches_token_on_init():
    """When client_credentials are provided, a token is fetched and applied as Bearer."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls, patch(
        "sqlonfhir.dbapi.connection.requests.post"
    ) as mock_post:
        session = MagicMock()
        mock_cls.return_value = session
        session.get.return_value = _make_mock_response(
            json_data={"resourceType": "Bundle", "entry": []},
        )
        mock_post.return_value = _make_mock_response(
            json_data={"access_token": "fresh-jwt", "expires_in": 300, "token_type": "Bearer"},
        )

        sqlonfhir.dbapi.connect(
            host="localhost",
            client_id="cid",
            client_secret="csec",
            token_url="https://idp.example/token",
            scope="system/*.rs",
        )

        # Token endpoint was hit with client_credentials grant
        assert mock_post.call_count == 1
        call = mock_post.call_args
        assert call.args[0] == "https://idp.example/token"
        assert call.kwargs["data"]["grant_type"] == "client_credentials"
        assert call.kwargs["data"]["client_id"] == "cid"
        assert call.kwargs["data"]["client_secret"] == "csec"
        assert call.kwargs["data"]["scope"] == "system/*.rs"

        # Bearer header was set on the session from the access_token
        auth_calls = [
            c for c in session.headers.__setitem__.call_args_list if c[0][0] == "Authorization"
        ]
        assert auth_calls[-1][0][1] == "Bearer fresh-jwt"


def test_connection_oauth_refreshes_proactively_before_expiry():
    """A second request after the token's lifetime should trigger a refresh."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls, patch(
        "sqlonfhir.dbapi.connection.requests.post"
    ) as mock_post, patch("sqlonfhir.dbapi.connection.time.monotonic") as mock_time:
        session = MagicMock()
        mock_cls.return_value = session
        session.get.return_value = _make_mock_response(
            json_data={"resourceType": "Bundle", "entry": []},
        )
        mock_post.side_effect = [
            _make_mock_response(json_data={"access_token": "tok-1", "expires_in": 60}),
            _make_mock_response(json_data={"access_token": "tok-2", "expires_in": 60}),
        ]
        mock_time.return_value = 1000.0

        conn = sqlonfhir.dbapi.connect(
            host="localhost",
            client_id="cid",
            client_secret="csec",
            token_url="https://idp.example/token",
        )
        assert mock_post.call_count == 1

        # Jump past the proactive-refresh threshold (60s lifetime - skew=15s = 1045)
        mock_time.return_value = 1100.0
        conn._request("GET", "http://localhost:8080/fhir/anything")
        assert mock_post.call_count == 2

        auth_calls = [
            c for c in session.headers.__setitem__.call_args_list if c[0][0] == "Authorization"
        ]
        assert auth_calls[-1][0][1] == "Bearer tok-2"


def test_connection_oauth_retries_on_401():
    """A 401 from the FHIR server triggers a single token refresh + retry."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls, patch(
        "sqlonfhir.dbapi.connection.requests.post"
    ) as mock_post:
        session = MagicMock()
        mock_cls.return_value = session
        session.get.return_value = _make_mock_response(
            json_data={"resourceType": "Bundle", "entry": []},
        )
        mock_post.return_value = _make_mock_response(
            json_data={"access_token": "tok", "expires_in": 3600},
        )

        conn = sqlonfhir.dbapi.connect(
            host="localhost",
            client_id="cid",
            client_secret="csec",
            token_url="https://idp.example/token",
        )
        # 1 token fetch happened during init
        assert mock_post.call_count == 1

        unauthorized = _make_mock_response(status_code=401, json_data={})
        ok = _make_mock_response(status_code=200, json_data={})
        session.post.side_effect = [unauthorized, ok]

        resp = conn._request("POST", "http://localhost:8080/fhir/$something", json={})

        # Token endpoint was called again to refresh after the 401
        assert mock_post.call_count == 2
        assert resp.status_code == 200
        assert session.post.call_count == 2


def test_connection_oauth_token_endpoint_failure_raises():
    """Failure to reach the IdP surfaces as OperationalError."""
    import requests as _requests

    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls, patch(
        "sqlonfhir.dbapi.connection.requests.post"
    ) as mock_post:
        mock_cls.return_value = MagicMock()
        mock_post.side_effect = _requests.exceptions.ConnectionError("dns failure")

        with pytest.raises(OperationalError, match="Failed to fetch OAuth2 token"):
            sqlonfhir.dbapi.connect(
                host="localhost",
                client_id="cid",
                client_secret="csec",
                token_url="https://idp.example/token",
            )


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
