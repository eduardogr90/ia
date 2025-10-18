"""Main Crew orchestrator coordinating the multi-agent workflow."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from crewai import Task

from ..agents import create_interpreter_agent
from .base_orchestrator import BaseCrewOrchestrator
from .prompt_builders import (
    build_analyzer_prompt,
    build_executor_prompt,
    build_interpreter_prompt,
    build_sql_prompt,
    build_validator_prompt,
)
from .results import OrchestrationError, OrchestrationResult
from .runner import _parse_json, _run_task
from .semantics import extract_semantics
from services.bigquery_client import BigQueryClient


class CrewOrchestrator(BaseCrewOrchestrator):
    """Coordinates the CrewAI agents to respond to user questions."""

    def __init__(
        self,
        metadata_dir: Path | None = None,
        bigquery_client: Optional[BigQueryClient] = None,
    ) -> None:
        super().__init__(metadata_dir=metadata_dir, bigquery_client=bigquery_client)

    # ------------------------------------------------------------------
    def handle_message(
        self, user_message: str, history: List[Dict[str, str]]
    ) -> OrchestrationResult:
        """Run the full multi-agent pipeline for a user utterance."""
        self._ensure_llm()

        flow_trace: List[Dict[str, object]] = []
        total_tokens = 0
        total_latency_ms = 0.0
        total_cost_usd = 0.0
        cost_available = False

        def append_trace(entry: Dict[str, object]) -> None:
            nonlocal total_tokens, total_latency_ms, total_cost_usd, cost_available
            flow_trace.append(entry)
            tokens = entry.get("tokens")
            if isinstance(tokens, dict):
                try:
                    total_tokens += int(tokens.get("total") or 0)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    pass
            latency = entry.get("latency_ms")
            if latency is not None:
                try:
                    total_latency_ms += float(latency)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    pass
            cost = entry.get("cost_usd")
            if cost is not None:
                try:
                    total_cost_usd += float(cost)
                    cost_available = True
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    pass

        def finalize_result(
            *,
            response: str,
            interpreter_output: Dict[str, object],
            sql_output: Dict[str, object],
            validation_output: Dict[str, object],
            analyzer_output: Dict[str, object],
            sql: Optional[str],
            rows: Optional[List[Dict[str, object]]],
            error: Optional[str],
            chart: Optional[Dict[str, object]],
        ) -> OrchestrationResult:
            aggregated_latency = round(total_latency_ms, 3)
            aggregated_cost = round(total_cost_usd, 8) if cost_available else None
            return OrchestrationResult(
                response=response,
                interpreter_output=interpreter_output,
                sql_output=sql_output,
                validation_output=validation_output,
                analyzer_output=analyzer_output,
                sql=sql,
                rows=rows,
                error=error,
                chart=chart,
                flow_trace=flow_trace,
                total_tokens=total_tokens,
                total_latency_ms=aggregated_latency,
                total_cost_usd=aggregated_cost,
            )

        try:
            if not all(
                [
                    self.interpreter_agent,
                    self.sql_agent,
                    self.executor_agent,
                    self.validator_agent,
                    self.analyzer_agent,
                    self.analysis_tool,
                ]
            ):
                raise OrchestrationError(
                    "Los agentes de CrewAI no se inicializaron correctamente."
                )

            has_history = any(
                (item.get("content") or "").strip()
                for item in history
                if isinstance(item, dict)
            )
            history_text = self._format_history(history) if has_history else ""
            if has_history:
                self.history_tool.set_history(history_text)
                self.interpreter_agent = create_interpreter_agent(
                    history_tool=self.history_tool, llm=self._llm
                )
            else:
                self.history_tool.set_history("")
                self.interpreter_agent = create_interpreter_agent(llm=self._llm)
            self.metadata_tool.set_metadata(self.metadata)
            self.bigquery_tool.reset()
            self.validation_tool.set_metadata(self.metadata)

            interpreter_prompt = build_interpreter_prompt(
                user_message, history_text, has_history
            )
            interpreter_task = Task(
                description=interpreter_prompt,
                agent=self.interpreter_agent,
                expected_output=(
                    "JSON con requires_sql, reasoning, refined_question y semantics"
                ),
            )
            interpreter_raw, interpreter_trace = _run_task(
                self.interpreter_agent,
                interpreter_task,
                prompt_cost_per_1k=self.prompt_cost_per_1k,
                completion_cost_per_1k=self.completion_cost_per_1k,
                input_context=user_message,
            )
            append_trace(interpreter_trace)
            interpreter_data = _parse_json(interpreter_raw)

            requires_sql = bool(interpreter_data.get("requires_sql", False))
            refined_question = interpreter_data.get("refined_question") or user_message

            question_semantics = extract_semantics(interpreter_data)

            sql_data: Dict[str, object] = {"sql": None, "analysis": ""}
            validation_data: Dict[str, object] = {}
            analyzer_output: Dict[str, object] = {}
            if requires_sql:
                metadata_summary = self.metadata_tool.summary()
                sql_prompt = build_sql_prompt(
                    refined_question,
                    metadata_summary,
                    interpreter_data,
                    question_semantics,
                )
                sql_task = Task(
                    description=sql_prompt,
                    agent=self.sql_agent,
                    expected_output="JSON con sql y analysis",
                )
                sql_raw, sql_trace = _run_task(
                    self.sql_agent,
                    sql_task,
                    prompt_cost_per_1k=self.prompt_cost_per_1k,
                    completion_cost_per_1k=self.completion_cost_per_1k,
                    input_context=refined_question,
                )
                append_trace(sql_trace)
                sql_data = _parse_json(sql_raw)

            sql_text = sql_data.get("sql") if isinstance(sql_data, dict) else None
            if isinstance(sql_text, str) and not sql_text.strip():
                sql_text = None

            sanitized_sql: str | None = None
            if requires_sql and isinstance(sql_text, str):
                self.validation_tool.set_candidate(sql_text, refined_question)
                validator_prompt = build_validator_prompt(
                    sql_text, refined_question
                )
                validator_task = Task(
                    description=validator_prompt,
                    agent=self.validator_agent,
                    expected_output="JSON con valid, message, sanitized_sql, issues, warnings",
                )
                validation_raw, validation_trace = _run_task(
                    self.validator_agent,
                    validator_task,
                    prompt_cost_per_1k=self.prompt_cost_per_1k,
                    completion_cost_per_1k=self.completion_cost_per_1k,
                    extra_metadata={"input_sql": sql_text},
                )
                validation_data = _parse_json(validation_raw)
                is_valid = bool(validation_data.get("valid"))
                sanitized_sql = (
                    validation_data.get("sanitized_sql")
                    if isinstance(validation_data, dict)
                    else None
                )
                if isinstance(sanitized_sql, str) and not sanitized_sql.strip():
                    sanitized_sql = None
                if sanitized_sql:
                    validation_trace["sanitized_sql"] = sanitized_sql
                validation_trace["validation_result"] = (
                    "OK" if is_valid else "RECHAZADA"
                )
                append_trace(validation_trace)
                if not is_valid:
                    message = (
                        validation_data.get("message")
                        or "La consulta fue bloqueada por motivos de seguridad."
                    )
                    return finalize_result(
                        response=str(message).strip(),
                        interpreter_output=interpreter_data,
                        sql_output=sql_data,
                        validation_output=validation_data,
                        analyzer_output={},
                        sql=None,
                        rows=None,
                        error=str(message),
                        chart=None,
                    )

            if requires_sql and not sanitized_sql:
                return finalize_result(
                    response="No se pudo generar una consulta SQL válida.",
                    interpreter_output=interpreter_data,
                    sql_output=sql_data,
                    validation_output=validation_data,
                    analyzer_output={},
                    sql=None,
                    rows=None,
                    error="Consulta SQL vacía tras la validación.",
                    chart=None,
                )

            rows: List[Dict[str, object]] | None = None
            execution_error: Optional[str] = None
            if requires_sql and sanitized_sql:
                executor_prompt = build_executor_prompt(
                    user_message,
                    sanitized_sql,
                    interpreter_data,
                )
                executor_task = Task(
                    description=executor_prompt,
                    agent=self.executor_agent,
                    expected_output="Confirmación de ejecución o error",
                )
                _, executor_trace = _run_task(
                    self.executor_agent,
                    executor_task,
                    prompt_cost_per_1k=self.prompt_cost_per_1k,
                    completion_cost_per_1k=self.completion_cost_per_1k,
                    extra_metadata={"input_sql": sanitized_sql},
                )
                rows = self.bigquery_tool.last_result
                execution_error = self.bigquery_tool.last_error
                executor_trace["rows_returned"] = len(rows or [])
                if execution_error:
                    executor_trace["error"] = execution_error
                append_trace(executor_trace)
                if execution_error:
                    error_message = (
                        f"Error al ejecutar la consulta en BigQuery: {execution_error}"
                    )
                    return finalize_result(
                        response=error_message,
                        interpreter_output=interpreter_data,
                        sql_output=sql_data,
                        validation_output=validation_data,
                        analyzer_output={},
                        sql=sanitized_sql,
                        rows=rows,
                        error=execution_error,
                        chart=None,
                    )

                self.analysis_tool.set_context(
                    question=refined_question,
                    sql=sanitized_sql,
                    results=rows or [],
                )
                analyzer_prompt = build_analyzer_prompt(
                    refined_question,
                    sanitized_sql,
                    rows or [],
                    question_semantics,
                )
                analyzer_task = Task(
                    description=analyzer_prompt,
                    agent=self.analyzer_agent,
                    expected_output="JSON con text y chart",
                )
                analyzer_raw, analyzer_trace = _run_task(
                    self.analyzer_agent,
                    analyzer_task,
                    prompt_cost_per_1k=self.prompt_cost_per_1k,
                    completion_cost_per_1k=self.completion_cost_per_1k,
                    extra_metadata={"input_rows": len(rows or [])},
                )
                analyzer_output = _parse_json(analyzer_raw)
                append_trace(analyzer_trace)
                response_text = analyzer_output.get("text")
                if not isinstance(response_text, str) or not response_text.strip():
                    response_text = str(analyzer_raw).strip()
                chart = (
                    analyzer_output.get("chart")
                    if isinstance(analyzer_output, dict)
                    else None
                )
                chart_payload = chart if isinstance(chart, dict) else None
                return finalize_result(
                    response=response_text.strip(),
                    interpreter_output=interpreter_data,
                    sql_output=sql_data,
                    validation_output=validation_data,
                    analyzer_output=(
                        analyzer_output if isinstance(analyzer_output, dict) else {}
                    ),
                    sql=sanitized_sql,
                    rows=rows,
                    error=None,
                    chart=chart_payload,
                )

            # Caso en que no se requiere SQL: responder con el razonamiento del intérprete.
            fallback_text = (
                interpreter_data.get("reasoning")
                if isinstance(interpreter_data, dict)
                else None
            ) or "La pregunta no requiere ejecutar SQL."
            analyzer_output = {"text": str(fallback_text), "chart": None}
            return finalize_result(
                response=str(fallback_text).strip(),
                interpreter_output=interpreter_data,
                sql_output=sql_data,
                validation_output=validation_data,
                analyzer_output=analyzer_output,
                sql=None,
                rows=None,
                error=None,
                chart=None,
            )
        except OrchestrationError:
            raise
        except Exception as exc:  # pragma: no cover - depends on runtime
            raise OrchestrationError(
                "Fallo inesperado al procesar el mensaje", detail=str(exc)
            ) from exc


_orchestrator: Optional[CrewOrchestrator] = None


def get_orchestrator() -> CrewOrchestrator:
    """Lazy access to a singleton orchestrator instance."""

    global _orchestrator
    if _orchestrator is None:
        try:
            _orchestrator = CrewOrchestrator()
        except OrchestrationError:
            raise
        except Exception as exc:  # pragma: no cover - depends on environment
            raise OrchestrationError(
                "No se pudo inicializar el orquestador de CrewAI.",
                detail=str(exc),
            ) from exc
    return _orchestrator


__all__ = [
    "CrewOrchestrator",
    "OrchestrationResult",
    "OrchestrationError",
    "get_orchestrator",
]
