"""Shared test fixtures for the SQL on FHIR DBAPI/dialect tests."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import sqlonfhir.dbapi
from sqlonfhir.dbapi.connection import Connection


# Sample ViewDefinition Bundle returned by GET /fhir/ViewDefinition
SAMPLE_VIEW_DEFINITION_BUNDLE: dict[str, Any] = {
    "resourceType": "Bundle",
    "type": "searchset",
    "total": 2,
    "link": [{"relation": "self", "url": "http://localhost:8080/fhir/ViewDefinition"}],
    "entry": [
        {
            "resource": {
                "resourceType": "ViewDefinition",
                "id": "vd-patients-1",
                "name": "patients",
                "resource": "Patient",
                "select": [
                    {
                        "column": [
                            {"path": "id", "name": "patient_id", "type": "id"},
                            {"path": "gender", "name": "gender", "type": "code"},
                            {"path": "birthDate", "name": "birth_date", "type": "date"},
                        ]
                    },
                    {
                        "forEach": "name.where(use = 'official')",
                        "column": [
                            {"path": "family", "name": "family_name", "type": "string"},
                            {"path": "given.first()", "name": "given_name", "type": "string"},
                        ],
                    },
                ],
            }
        },
        {
            "resource": {
                "resourceType": "ViewDefinition",
                "id": "vd-conditions-1",
                "name": "conditions",
                "resource": "Condition",
                "select": [
                    {
                        "column": [
                            {"path": "id", "name": "condition_id", "type": "id"},
                            {"path": "subject.reference", "name": "patient_ref", "type": "string"},
                            {"path": "code.coding.first().display", "name": "condition_name", "type": "string"},
                            {"path": "onsetDateTime", "name": "onset_date", "type": "dateTime"},
                        ]
                    }
                ],
            }
        },
    ],
}

# Sample $sqlquery-run JSON response
SAMPLE_QUERY_RESPONSE: list[dict[str, Any]] = [
    {"patient_id": "pat-1", "gender": "male", "birth_date": "1990-05-15"},
    {"patient_id": "pat-2", "gender": "female", "birth_date": "1985-11-22"},
    {"patient_id": "pat-3", "gender": "male", "birth_date": "2000-03-08"},
]

# Sample NDJSON response
SAMPLE_NDJSON_RESPONSE = "\n".join(json.dumps(row) for row in SAMPLE_QUERY_RESPONSE)


def _make_mock_response(
    json_data: Any = None,
    text: str = "",
    status_code: int = 200,
    content_type: str = "application/json",
) -> MagicMock:
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.ok = 200 <= status_code < 300
    resp.status_code = status_code
    resp.headers = {"Content-Type": content_type}
    resp.text = text or json.dumps(json_data) if json_data else text
    resp.json.return_value = json_data
    return resp


@pytest.fixture()
def mock_session():
    """Provide a mock requests.Session that returns ViewDefinition bundle and query results."""
    with patch("sqlonfhir.dbapi.connection.requests.Session") as mock_cls:
        session = MagicMock()
        mock_cls.return_value = session

        # Mock GET for ViewDefinition listing
        vd_response = _make_mock_response(
            json_data=SAMPLE_VIEW_DEFINITION_BUNDLE,
            content_type="application/fhir+json",
        )
        session.get.return_value = vd_response

        # Mock POST for $sqlquery-run
        query_response = _make_mock_response(
            json_data=SAMPLE_QUERY_RESPONSE,
            content_type="application/json",
        )
        session.post.return_value = query_response

        yield session


@pytest.fixture()
def connection(mock_session) -> Connection:
    """Provide a Connection with mocked HTTP."""
    return sqlonfhir.dbapi.connect(host="localhost", port=8080)
