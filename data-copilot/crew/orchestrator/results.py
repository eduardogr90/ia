"""Result objects and error types for the Crew orchestrator."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


class OrchestrationError(RuntimeError):
    """Custom error raised when the Crew orchestration fails."""

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        full_message = message
        if detail:
            detail = detail.strip()
            if detail:
                full_message = f"{message}: {detail}"
        super().__init__(full_message)


@dataclass
class OrchestrationResult:
    """Final outcome of the orchestrated multi-agent run."""

    response: str
    interpreter_output: Dict[str, object]
    sql_output: Dict[str, object]
    validation_output: Dict[str, object]
    analyzer_output: Dict[str, object]
    sql: Optional[str]
    rows: Optional[List[Dict[str, object]]]
    error: Optional[str]
    chart: Optional[Dict[str, object]]
    flow_trace: List[Dict[str, object]]
    total_tokens: int
    total_latency_ms: float
    total_cost_usd: Optional[float]

    def to_dict(self) -> Dict[str, object]:
        return {
            "response": self.response,
            "interpreter_output": self.interpreter_output,
            "sql_output": self.sql_output,
            "validation_output": self.validation_output,
            "analyzer_output": self.analyzer_output,
            "sql": self.sql,
            "rows": self.rows,
            "error": self.error,
            "chart": self.chart,
            "flow_trace": self.flow_trace,
            "total_tokens": self.total_tokens,
            "total_latency_ms": self.total_latency_ms,
            "total_cost_usd": self.total_cost_usd,
        }
