"""DBAPI 2.0 Connection for SQL on FHIR servers."""

from __future__ import annotations

from typing import Any

import requests

from sqlonfhir.dbapi.cursor import Cursor
from sqlonfhir.dbapi.exceptions import InterfaceError, OperationalError


class Connection:
    """A DBAPI 2.0 connection to a Pathling FHIR server.

    Manages the HTTP session, authentication, and a cache of ViewDefinition
    metadata used to map SQL table names to FHIR ViewDefinition resources.
    """

    def __init__(
        self,
        host: str,
        port: int = 8080,
        path: str = "/fhir",
        scheme: str = "http",
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 300,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = f"{scheme}://{host}:{port}{path}".rstrip("/")
        self.timeout = timeout
        self._closed = False

        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._session.headers["Accept"] = "application/fhir+json"

        if token:
            bearer = token if token.startswith("Bearer ") else f"Bearer {token}"
            self._session.headers["Authorization"] = bearer
        elif username and password:
            self._session.auth = (username, password)

        if headers:
            self._session.headers.update(headers)

        # Cache: ViewDefinition name -> {id, columns, resource_type}
        self._view_definitions: dict[str, dict[str, Any]] = {}
        self._load_view_definitions()

    def _load_view_definitions(self) -> None:
        """Fetch all ViewDefinitions from the server and build the name->metadata cache."""
        self._view_definitions = {}
        url = f"{self.base_url}/ViewDefinition"
        params: dict[str, str] = {"_count": "500"}

        while url:
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
            except requests.exceptions.ConnectionError as e:
                raise OperationalError(
                    f"Failed to connect to Pathling server at {self.base_url}: {e}"
                ) from e
            except requests.exceptions.HTTPError as e:
                raise OperationalError(
                    f"Failed to fetch ViewDefinitions: {e}"
                ) from e

            bundle = resp.json()
            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                name = resource.get("name")
                vid = resource.get("id")
                if not name or not vid:
                    continue

                columns = self._extract_columns(resource)
                resource_type = resource.get("resource", "")

                self._view_definitions[name] = {
                    "id": vid,
                    "columns": columns,
                    "resource_type": resource_type,
                }

            # Follow pagination links
            url = None
            params = {}
            for link in bundle.get("link", []):
                if link.get("relation") == "next":
                    url = link.get("url")
                    break

    def _extract_columns(self, view_definition: dict[str, Any]) -> list[dict[str, str]]:
        """Extract column metadata from a ViewDefinition resource."""
        columns: list[dict[str, str]] = []
        for select in view_definition.get("select", []):
            for col in select.get("column", []):
                col_name = col.get("name", "")
                col_type = col.get("type", "string")
                if col_name:
                    columns.append({"name": col_name, "type": col_type})
        return columns

    def refresh_view_definitions(self) -> None:
        """Re-fetch ViewDefinitions from the server."""
        self._load_view_definitions()

    def cursor(self) -> Cursor:
        """Return a new Cursor object using this connection."""
        self._check_closed()
        return Cursor(self)

    def close(self) -> None:
        """Close the connection and release resources."""
        if not self._closed:
            self._session.close()
            self._closed = True

    def commit(self) -> None:
        """No-op. Pathling is read-only."""

    def rollback(self) -> None:
        """No-op. Pathling is read-only."""

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Connection is closed")
