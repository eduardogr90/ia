"""Tests for safeguards preventing fabricated executor results."""
import sys
from pathlib import Path
from types import SimpleNamespace

# Allow importing ``crew.orchestrator`` modules when running tests from repo root.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from crew.orchestrator.orchestrator import CrewOrchestrator


def _make_orchestrator(last_sql: str | None) -> CrewOrchestrator:
    """Create a lightweight orchestrator with a mocked BigQuery tool."""

    orchestrator = CrewOrchestrator.__new__(CrewOrchestrator)
    orchestrator.bigquery_tool = SimpleNamespace(last_sql=last_sql)
    return orchestrator


def test_check_executor_sql_execution_requires_tool_call() -> None:
    orchestrator = _make_orchestrator(last_sql=None)

    message = orchestrator._check_executor_sql_execution("SELECT 1")

    assert message is not None
    assert "no ejecutó" in message.lower()


def test_check_executor_sql_execution_detects_mismatch() -> None:
    orchestrator = _make_orchestrator(last_sql="SELECT foo FROM bar")

    message = orchestrator._check_executor_sql_execution("SELECT baz FROM bar")

    assert message is not None
    assert "distinta" in message.lower()


def test_check_executor_sql_execution_accepts_equivalent_sql() -> None:
    orchestrator = _make_orchestrator(
        last_sql="SELECT foo\nFROM   bar  WHERE   id = 1"
    )

    message = orchestrator._check_executor_sql_execution(
        "  select  foo from bar\nwhere id = 1  "
    )

    assert message is None
