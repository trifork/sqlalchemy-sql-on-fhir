"""End-to-end checks against a real Pathling FHIR server.

These tests guard against three regressions that have already bitten us:

* The driver must send `Library.type` with system
  ``https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes`` and code
  ``sql-query`` — Pathling rejects the older ``logic-library`` coding.

* The dialect must emit boolean literals as ``TRUE``/``FALSE``. Spark's
  Catalyst analyzer rejects ``WHERE active IN (1)`` with a
  DATATYPE_MISMATCH, surfaced to the client as an opaque HTTP 500.

* Every Superset time-grain expression must be SQL Pathling actually
  accepts. The previous mapping used ``from_unixtime(unix_timestamp(col),
  fmt)`` for most grains, which Pathling's planner rejects during chart
  introspection (opaque HTTP 500). The current mapping uses
  ``date_format(date_trunc(...))`` / ``date_format(timestamp_seconds(...))``.
"""

from __future__ import annotations

import sys
import types
from urllib.parse import urlparse


def _install_superset_stub() -> None:
    """Allow `import sqlonfhir.superset.engine_spec` without Superset installed.

    The engine spec only needs `BaseEngineSpec` as a parent class for the
    `_time_grain_expressions` map we exercise here, so a minimal stub is
    enough. Done at module top because parametrize() runs at collection.
    """
    if "superset" in sys.modules:
        return
    superset = types.ModuleType("superset")
    db_engine_specs = types.ModuleType("superset.db_engine_specs")
    base = types.ModuleType("superset.db_engine_specs.base")

    class BaseEngineSpec:
        pass

    base.BaseEngineSpec = BaseEngineSpec
    sys.modules["superset"] = superset
    sys.modules["superset.db_engine_specs"] = db_engine_specs
    sys.modules["superset.db_engine_specs.base"] = base


_install_superset_stub()

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


# --- Superset time-grain expressions ----------------------------------------
#
# Regression: Pathling's `$sqlquery-run` rejects the bare
# `from_unixtime(unix_timestamp(col), 'fmt')` form that Superset's stock
# Hive/Spark engine spec maps most grains to. The failure surfaces as an
# opaque HTTP 500 during chart-data introspection (the `WHERE 1 != 1` /
# `LIMIT 0` shape Superset emits to learn result-column types). The same
# SQL runs fine in SQL Lab, which is what made the bug nasty.
#
# The current mapping rewrites every native grain to
# `date_format(date_trunc(<unit>, col), '<fmt>')`, and the sub-hour buckets
# to `date_format(timestamp_seconds(unix_timestamp(col) - unix_timestamp(col)
# % N), '<fmt>')`. These tests run each rendered expression against a real
# Pathling and assert it parses + executes — both as a standalone SELECT
# (the dialect-level guarantee) and inside the chart-introspection wrapper
# that originally tripped the bug.


def _all_grain_ids() -> list[str]:
    from sqlonfhir.superset.engine_spec import SqlOnFhirEngineSpec

    return [g for g in SqlOnFhirEngineSpec._time_grain_expressions if g is not None]


def _render_grain(grain: str, col_sql: str) -> str:
    from sqlonfhir.superset.engine_spec import SqlOnFhirEngineSpec

    template = SqlOnFhirEngineSpec._time_grain_expressions[grain]
    return template.format(col=col_sql)


# A fixed timestamp keeps assertions deterministic. Mid-quarter / mid-month /
# mid-week so the quarter/week/month buckets actually shift the date.
_TS_LITERAL = "TIMESTAMP '2025-05-15 13:47:23'"


@pytest.mark.parametrize("grain", _all_grain_ids())
def test_time_grain_expression_executes_on_pathling(cursor, grain):
    """Every grain expression renders to SQL Pathling accepts.

    Runs the rendered expression as a top-level `SELECT <expr>` (no FROM),
    which is how Pathling exercises the planner without needing a registered
    ViewDefinition. Failure mode under the old mapping was an HTTP 500 from
    `$sqlquery-run`.
    """
    expr = _render_grain(grain, _TS_LITERAL)
    cursor.execute(f"SELECT {expr} AS bucket")
    rows = cursor.fetchall()
    assert len(rows) == 1, f"{grain}: expected one bucket row, got {rows!r}"
    bucket = rows[0][0]
    assert bucket is not None, f"{grain}: bucket value is NULL"
    # Every native grain formats to the leading 'yyyy-' year — the input
    # timestamp is 2025-something, so the result should mention 2025.
    assert "2025" in str(bucket), (
        f"{grain}: expected '2025' in bucket value, got {bucket!r}"
    )


@pytest.mark.parametrize("grain", ["PT1S", "PT1M", "PT1H", "P1D", "P1W", "P1M", "P3M", "P1Y"])
def test_time_grain_in_chart_shaped_query_succeeds(cursor, grain):
    """The grain expression survives the chart-query shape Superset emits.

    Superset's chart endpoint composes:

        SELECT <grain_expr> AS axis, COUNT(...) AS metric
        FROM <view>
        WHERE <temporal_filter>
        GROUP BY <grain_expr>
        ORDER BY metric DESC
        LIMIT <row_limit>

    on top of the underlying dataset. This test mirrors that shape over a
    single-row literal subquery (the integration image starts with no
    ViewDefinitions, so we can't query a real one). The grain expression
    must appear identically in SELECT and GROUP BY — Pathling's planner
    walks both and used to choke on the old `from_unixtime(unix_timestamp(
    ...))` form when wired in this exact shape.
    """
    expr = _render_grain(grain, "ts")
    cursor.execute(
        f"SELECT {expr} AS axis, COUNT(1) AS metric "
        f"FROM (SELECT {_TS_LITERAL} AS ts) t "
        f"GROUP BY {expr} "
        f"ORDER BY metric DESC "
        f"LIMIT 100"
    )
    rows = cursor.fetchall()
    assert len(rows) == 1, f"{grain}: expected one bucketed row, got {rows!r}"
    bucket, metric = rows[0]
    assert metric == 1, f"{grain}: expected metric=1, got {metric!r}"
    assert "2025" in str(bucket), f"{grain}: bucket {bucket!r} missing year"


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
