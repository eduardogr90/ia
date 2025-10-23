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

    ok, message = orchestrator._check_executor_sql_execution("SELECT 1")

    assert not ok
    assert message is not None
    assert "no ejecutó" in message.lower()


def test_check_executor_sql_execution_detects_mismatch() -> None:
    orchestrator = _make_orchestrator(last_sql="SELECT foo FROM bar")

    ok, message = orchestrator._check_executor_sql_execution("SELECT baz FROM bar")

    assert not ok
    assert message is not None
    assert "distinta" in message.lower()


def test_check_executor_sql_execution_accepts_equivalent_sql() -> None:
    orchestrator = _make_orchestrator(
        last_sql="SELECT foo\nFROM   bar  WHERE   id = 1"
    )

    ok, message = orchestrator._check_executor_sql_execution(
        "  select  foo from bar\nwhere id = 1  "
    )

    assert ok
    assert message is None


def test_execute_validated_sql_runs_query_and_updates_tool() -> None:
    expected_rows = [{"value": 1}]

    orchestrator = CrewOrchestrator.__new__(CrewOrchestrator)
    orchestrator.bigquery_client = SimpleNamespace(run_query=lambda sql: expected_rows)
    orchestrator.bigquery_tool = SimpleNamespace(
        last_sql=None, last_result=None, last_error="prev"
    )

    rows, error = orchestrator._execute_validated_sql("SELECT 1")

    assert rows == expected_rows
    assert error is None
    assert orchestrator.bigquery_tool.last_sql == "SELECT 1"
    assert orchestrator.bigquery_tool.last_result == expected_rows
    assert orchestrator.bigquery_tool.last_error is None


def test_execute_validated_sql_returns_error_message() -> None:
    def _raise(sql: str) -> None:
        raise RuntimeError("boom")

    orchestrator = CrewOrchestrator.__new__(CrewOrchestrator)
    orchestrator.bigquery_client = SimpleNamespace(run_query=_raise)
    orchestrator.bigquery_tool = SimpleNamespace(
        last_sql="prev_sql", last_result=[{"value": 2}], last_error=None
    )

    rows, error = orchestrator._execute_validated_sql("SELECT 1")

    assert rows is None
    assert error is not None and "boom" in error
    assert orchestrator.bigquery_tool.last_sql == "prev_sql"
    assert orchestrator.bigquery_tool.last_result == [{"value": 2}]
    assert orchestrator.bigquery_tool.last_error == "boom"
