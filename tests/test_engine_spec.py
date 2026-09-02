"""Tests for the Superset engine spec.

These tests stub out the `superset` package so the module can be imported
without Superset actually installed — the engine spec only inherits from
`BaseEngineSpec` for its class attributes and Superset method protocol, and
we are only checking class-level data here.
"""

from __future__ import annotations

import re
import sys
import types

import pytest


def _install_superset_stub() -> None:
    """Make `from superset.db_engine_specs.base import BaseEngineSpec` work."""
    if "superset" in sys.modules:
        return
    superset = types.ModuleType("superset")
    db_engine_specs = types.ModuleType("superset.db_engine_specs")
    base = types.ModuleType("superset.db_engine_specs.base")

    class BaseEngineSpec:  # minimal stand-in; only used as a parent class
        pass

    base.BaseEngineSpec = BaseEngineSpec
    sys.modules["superset"] = superset
    sys.modules["superset.db_engine_specs"] = db_engine_specs
    sys.modules["superset.db_engine_specs.base"] = base


@pytest.fixture(scope="module")
def grains() -> dict:
    _install_superset_stub()
    from sqlonfhir.superset.engine_spec import SqlOnFhirEngineSpec

    return SqlOnFhirEngineSpec._time_grain_expressions


# The Pathling $sql-run planner rejects `from_unixtime(unix_timestamp(
# col), 'fmt')` during Superset's chart-data introspection (LIMIT-0 form).
# Make sure no grain falls back to that pattern. `date_format(...)` and
# `timestamp_seconds(...)` wrappers are fine; the standalone bare form is
# what regresses.
_BARE_FROM_UNIXTIME = re.compile(
    r"\bfrom_unixtime\s*\(\s*unix_timestamp\s*\(",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    "grain",
    [
        "PT1S", "PT1M", "PT5M", "PT15M", "PT30M",
        "PT1H", "PT6H",
        "P1D", "P1W", "P1M", "P3M", "P1Y",
    ],
)
def test_no_bare_from_unixtime(grains: dict, grain: str) -> None:
    expr = grains[grain]
    assert not _BARE_FROM_UNIXTIME.search(expr), (
        f"{grain!r} maps to {expr!r}, which uses the bare "
        "`from_unixtime(unix_timestamp(...), fmt)` form that Pathling "
        "rejects during Superset chart-data introspection. Wrap with "
        "`date_format(date_trunc(...))` or `date_format(timestamp_seconds("
        "...))` instead."
    )


@pytest.mark.parametrize(
    "grain,unit",
    [
        ("PT1S", "second"),
        ("PT1M", "minute"),
        ("PT1H", "hour"),
        ("P1D", "day"),
        ("P1W", "week"),
        ("P1M", "month"),
        ("P1Y", "year"),
    ],
)
def test_simple_grain_uses_date_trunc(grains: dict, grain: str, unit: str) -> None:
    """Whole-unit grains should bucket via `date_trunc('<unit>', col)`."""
    expr = grains[grain]
    assert f"date_trunc('{unit}'" in expr.lower() or f'date_trunc("{unit}"' in expr.lower(), (
        f"{grain!r} expected to bucket with date_trunc('{unit}', ...); got {expr!r}"
    )


@pytest.mark.parametrize(
    "grain,seconds",
    [
        ("PT5M", 300),
        ("PT15M", 900),
        ("PT30M", 1800),
        ("PT6H", 21600),
    ],
)
def test_sub_unit_grain_uses_modulo(grains: dict, grain: str, seconds: int) -> None:
    """Sub-unit grains keep the unix-arithmetic but wrap in date_format(timestamp_seconds(...))."""
    expr = grains[grain]
    assert f"% {seconds}" in expr, (
        f"{grain!r} should subtract `unix_timestamp(col) % {seconds}`; got {expr!r}"
    )
    assert "timestamp_seconds(" in expr, (
        f"{grain!r} should wrap the modular epoch in `timestamp_seconds(...)` "
        f"so it is rendered through `date_format`; got {expr!r}"
    )
    assert expr.startswith("date_format("), (
        f"{grain!r} expression should start with `date_format(`; got {expr!r}"
    )


def test_none_grain_is_identity(grains: dict) -> None:
    assert grains[None] == "{col}"


def test_all_grains_have_col_placeholder(grains: dict) -> None:
    """Every expression must reference {col} so Superset can substitute the column."""
    for grain, expr in grains.items():
        if grain is None:
            continue
        assert "{col}" in expr, f"{grain!r} expression {expr!r} is missing {{col}}"
