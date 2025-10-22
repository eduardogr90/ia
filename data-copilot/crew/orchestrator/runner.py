"""Task execution utilities for the Crew orchestrator."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from time import perf_counter
from typing import Dict, Optional, Tuple

from crewai import Agent, Crew, Process, Task
from google.auth.exceptions import DefaultCredentialsError

from .results import OrchestrationError


def _estimate_tokens(text: str | None) -> int:
    """Rudimentarily approximate token usage for logging purposes."""
    if not text:
        return 0
    normalized = str(text).strip()
    if not normalized:
        return 0
    return max(1, math.ceil(len(normalized) / 4))


def _estimate_cost(
    prompt_tokens: int,
    completion_tokens: int,
    prompt_cost_per_1k: float,
    completion_cost_per_1k: float,
) -> Optional[float]:
    """Estimate the USD cost of a model call if pricing metadata exists."""
    cost = 0.0
    has_cost = False
    if prompt_cost_per_1k > 0:
        cost += (prompt_tokens / 1000.0) * prompt_cost_per_1k
        has_cost = True
    if completion_cost_per_1k > 0:
        cost += (completion_tokens / 1000.0) * completion_cost_per_1k
        has_cost = True
    if not has_cost:
        return None
    return round(cost, 8)


def _contains_default_credentials_error(exc: Exception) -> bool:
    """Walk the exception chain looking for DefaultCredentialsError."""

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, DefaultCredentialsError):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


def _parse_json(payload: str) -> Dict[str, object]:
    """Parse JSON produced by agents, tolerating minor formatting issues."""
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        start = payload.find("{")
        end = payload.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(payload[start : end + 1])
            except json.JSONDecodeError:
                return {"raw": payload.strip()}
        return {"raw": payload.strip()}


def _run_task(
    agent: Agent,
    task: Task,
    *,
    prompt_cost_per_1k: float,
    completion_cost_per_1k: float,
    input_context: object | None = None,
    extra_metadata: Optional[Dict[str, object]] = None,
    uses_llm: bool = True,
) -> Tuple[str, Dict[str, object]]:
    """Execute *task* with *agent* and capture telemetry for traceability."""
    agent_role = getattr(agent, "role", agent.__class__.__name__)
    crew = Crew(
        agents=[agent],
        tasks=[task],
        process=Process.sequential,
    )
    start_time = perf_counter()
    try:
        result = crew.kickoff()
    except Exception as exc:  # pragma: no cover - depends on runtime
        if _contains_default_credentials_error(exc):
            raise OrchestrationError(
                f"El agente {agent_role} falló durante la ejecución",
                detail=(
                    "No se encontraron credenciales predeterminadas de Google Cloud."
                    " Define GOOGLE_APPLICATION_CREDENTIALS apuntando al JSON del"
                    " service account o ejecuta `gcloud auth application-default login`."
                ),
            ) from exc
        raise OrchestrationError(
            f"El agente {agent_role} falló durante la ejecución",
            detail=str(exc),
        ) from exc
    latency_ms = (perf_counter() - start_time) * 1000.0
    output = getattr(task, "output", None)
    if isinstance(output, str) and output.strip():
        response_text = output
    if isinstance(result, str):
        response_text = result
    else:
        response_text = str(result)

    trace_entry: Dict[str, object] = {
        "agent": agent_role,
        "prompt_sent": task.description,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latency_ms": round(latency_ms, 3),
    }

    if uses_llm:
        prompt_tokens = _estimate_tokens(task.description)
        completion_tokens = _estimate_tokens(response_text)
        tokens = {
            "prompt": prompt_tokens,
            "completion": completion_tokens,
            "total": prompt_tokens + completion_tokens,
        }
        trace_entry["tokens"] = tokens
        trace_entry["llm_response"] = response_text
        cost = _estimate_cost(
            prompt_tokens,
            completion_tokens,
            prompt_cost_per_1k,
            completion_cost_per_1k,
        )
        if cost is not None:
            trace_entry["cost_usd"] = cost
    else:
        trace_entry["tokens"] = {"prompt": 0, "completion": 0, "total": 0}
        trace_entry["llm_response"] = response_text

    if input_context is not None:
        trace_entry["input"] = input_context
    if extra_metadata:
        trace_entry.update(extra_metadata)

    return response_text, trace_entry
