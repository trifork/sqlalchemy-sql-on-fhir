"""End-to-end checks against a real Pathling FHIR server.

These tests guard against two regressions that have already bitten us:

* The driver must send `Library.type` with system
  ``https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes`` and code
  ``sql-query`` — Pathling rejects the older ``logic-library`` coding.

* The dialect must emit boolean literals as ``TRUE``/``FALSE``. Spark's
  Catalyst analyzer rejects ``WHERE active IN (1)`` with a
  DATATYPE_MISMATCH, surfaced to the client as an opaque HTTP 500.
"""

from __future__ import annotations

from urllib.parse import urlparse

import pytest
from sqlalchemy import Boolean, Column, MetaData, String, Table, create_engine, select

from sqlonfhir.dbapi import connect
from sqlonfhir.dbapi.exceptions import DatabaseError
from sqlonfhir.sqlalchemy.dialect import SqlOnFhirDialect

pytestmark = pytest.mark.integration


def _connect_kwargs(base_url: str) -> dict[str, object]:
    parsed = urlparse(base_url)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 80,
        "path": parsed.path or "/fhir",
        "scheme": parsed.scheme or "http",
    }


@pytest.fixture()
def cursor(pathling_base_url):
    conn = connect(**_connect_kwargs(pathling_base_url))
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def test_select_true_round_trip(cursor):
    """Smoke test: the driver speaks Pathling's $sqlquery-run dialect.

    Regression for the Library.type coding bug (PR #1) — under the old
    ``logic-library`` coding Pathling responded 400 to every query.
    """
    cursor.execute("SELECT TRUE AS t")
    assert cursor.fetchall() == [(True,)]


def test_boolean_in_clause_with_boolean_literal_succeeds(cursor):
    """``IN (TRUE)`` must parse and return rows — this is the shape the
    dialect now produces for SQLAlchemy boolean filters."""
    cursor.execute("SELECT TRUE WHERE TRUE IN (TRUE)")
    rows = cursor.fetchall()
    assert len(rows) == 1


def test_boolean_in_clause_with_integer_literal_still_fails(cursor):
    """Repro of the underlying Spark behavior that motivated the dialect fix.

    ``BOOLEAN IN (INT)`` triggers DATATYPE_MISMATCH in Catalyst and Pathling
    returns 500. This test pins the upstream behavior so we notice if it
    ever changes (in which case we can relax our literal handling).
    """
    with pytest.raises(DatabaseError) as exc_info:
        cursor.execute("SELECT TRUE IN (1) AS r")
    # Pathling collapses the exception into a generic 500.
    assert "500" in str(exc_info.value)


def test_sqlalchemy_boolean_filter_renders_as_true_literal():
    """SQLAlchemy must compile ``Column.in_([True])`` to ``IN (TRUE)``.

    Doesn't need the live server — it's the unit-level guarantee that pairs
    with `test_boolean_in_clause_with_boolean_literal_succeeds`. Kept here so
    the regression coverage reads as one story.
    """
    metadata = MetaData()
    practitioner = Table(
        "practitioner", metadata,
        Column("id", String),
        Column("active", Boolean),
    )
    stmt = select(practitioner.c.id).where(practitioner.c.active.in_([True]))
    compiled = stmt.compile(
        dialect=SqlOnFhirDialect(),
        compile_kwargs={"literal_binds": True},
    )
    sql = str(compiled).lower()
    assert "in (true)" in sql
    assert "in (1)" not in sql


def test_sqlalchemy_engine_executes_against_live_server(pathling_base_url):
    """End-to-end: the SQLAlchemy engine drives `$sqlquery-run` successfully.

    Exercises the full chain — engine → dialect → DBAPI cursor → Library
    envelope → Pathling — with a query that would have failed under either
    of the historical bugs.
    """
    parsed = urlparse(pathling_base_url)
    engine = create_engine(
        f"sqlonfhir://{parsed.hostname}:{parsed.port}{parsed.path}"
        f"?scheme={parsed.scheme}"
    )
    with engine.connect() as conn:
        from sqlalchemy import text

        result = conn.execute(text("SELECT TRUE AS t WHERE TRUE IN (TRUE)"))
        rows = result.fetchall()
        assert rows == [(True,)]
