"""DBAPI 2.0 Connection for SQL on FHIR servers."""

from __future__ import annotations

import time
from typing import Any

import requests

from sqlonfhir.dbapi.cursor import Cursor
from sqlonfhir.dbapi.exceptions import InterfaceError, OperationalError

# Module-level cache: base_url -> {"data": view_defs, "fetched_at": timestamp}
# Shared across all connections to the same server so a dashboard loading many
# charts simultaneously doesn't cause a thundering herd of ViewDefinition requests.
_VIEW_DEF_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes


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

        # ViewDefinitions are shared from the module-level cache when available.
        self._view_definitions: dict[str, dict[str, Any]] = {}
        self._load_view_definitions()

    @property
    def _view_definitions(self) -> dict[str, dict[str, Any]]:
        cached = _VIEW_DEF_CACHE.get(self.base_url)
        if cached:
            return cached["data"]
        return {}

    @_view_definitions.setter
    def _view_definitions(self, value: dict[str, dict[str, Any]]) -> None:
        if value:
            _VIEW_DEF_CACHE[self.base_url] = {"data": value, "fetched_at": time.monotonic()}

    def _load_view_definitions(self) -> None:
        """Fetch all ViewDefinitions from the server, using a module-level cache.

        Skips the network fetch if a valid cached result exists. Retries up to
        3 times with exponential backoff on transient 5xx errors.
        """
        cached = _VIEW_DEF_CACHE.get(self.base_url)
        if cached and (time.monotonic() - cached["fetched_at"]) < _CACHE_TTL_SECONDS:
            return  # Cache is still fresh

        view_defs: dict[str, dict[str, Any]] = {}
        url: str | None = f"{self.base_url}/ViewDefinition"
        params: dict[str, str] = {"_count": "500"}
        max_retries = 3

        while url:
            last_exc: Exception | None = None
            for attempt in range(max_retries):
                try:
                    resp = self._session.get(url, params=params, timeout=self.timeout)
                    resp.raise_for_status()
                    last_exc = None
                    break
                except requests.exceptions.ConnectionError as e:
                    raise OperationalError(
                        f"Failed to connect to Pathling server at {self.base_url}: {e}"
                    ) from e
                except requests.exceptions.HTTPError as e:
                    last_exc = e
                    if resp.status_code < 500 or attempt == max_retries - 1:
                        raise OperationalError(
                            f"Failed to fetch ViewDefinitions: {e}"
                        ) from e
                    time.sleep(2 ** attempt)  # 1s, 2s backoff

            if last_exc:
                raise OperationalError(
                    f"Failed to fetch ViewDefinitions after {max_retries} attempts: {last_exc}"
                ) from last_exc

            bundle = resp.json()
            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                name = resource.get("name")
                vid = resource.get("id")
                if not name or not vid:
                    continue

                columns = self._extract_columns(resource)
                resource_type = resource.get("resource", "")

                view_defs[name] = {
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

        _VIEW_DEF_CACHE[self.base_url] = {"data": view_defs, "fetched_at": time.monotonic()}

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
        """Invalidate the cache and re-fetch ViewDefinitions from the server."""
        _VIEW_DEF_CACHE.pop(self.base_url, None)
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
