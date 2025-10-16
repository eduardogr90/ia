"""Crew orchestrator that coordinates the Interpreter, SQL generator and Executor agents."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from crewai import Agent, Crew, Process, Task

from crew.agents import (
    BigQueryQueryTool,
    ConversationHistoryTool,
    SQLMetadataTool,
    create_executor_agent,
    create_interpreter_agent,
    create_sql_generator_agent,
    load_model_metadata,
)
from services.bigquery_client import BigQueryClient
from services.gemini_client import init_gemini_llm, load_vertex_credentials


class OrchestrationError(RuntimeError):
    """Custom error raised when the Crew orchestration fails."""


@dataclass
class OrchestrationResult:
    """Final outcome of the orchestrated multi-agent run."""

    response: str
    interpreter_output: Dict[str, object]
    sql_output: Dict[str, object]
    sql: Optional[str]
    rows: Optional[List[Dict[str, object]]]
    error: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "response": self.response,
            "interpreter_output": self.interpreter_output,
            "sql_output": self.sql_output,
            "sql": self.sql,
            "rows": self.rows,
            "error": self.error,
        }


class CrewOrchestrator:
    """Coordinates the CrewAI agents to respond to user questions."""

    def __init__(
        self,
        metadata_dir: Path | None = None,
        bigquery_client: Optional[BigQueryClient] = None,
    ) -> None:
        self.metadata_dir = metadata_dir or Path(__file__).resolve().parent.parent / "data" / "model"
        self.metadata = load_model_metadata(self.metadata_dir)

        self.history_tool = ConversationHistoryTool()
        self.metadata_tool = SQLMetadataTool(self.metadata)
        try:
            self.bigquery_client = bigquery_client or BigQueryClient()
        except FileNotFoundError as exc:  # pragma: no cover - depends on deployment
            raise OrchestrationError(
                "No se encontró el archivo de credenciales de BigQuery en config/bq_service_account.json."
            ) from exc
        except Exception as exc:  # pragma: no cover - depends on deployment
            raise OrchestrationError("No se pudo inicializar el cliente de BigQuery.") from exc
        self.bigquery_tool = BigQueryQueryTool(self.bigquery_client)

        self.interpreter_agent: Agent | None = None
        self.sql_agent: Agent | None = None
        self.executor_agent: Agent | None = None

        self._llm_ready = False
        self._llm = None

    # ------------------------------------------------------------------
    def _ensure_llm(self) -> None:
        if self._llm_ready:
            return
        project_id = os.environ.get("VERTEX_PROJECT_ID")
        if not project_id:
            raise OrchestrationError(
                "La variable de entorno VERTEX_PROJECT_ID no está configurada."
            )
        location = os.environ.get("VERTEX_LOCATION", "us-central1")
        try:
            credentials_info = load_vertex_credentials()
        except FileNotFoundError as exc:  # pragma: no cover - dependent on deployment
            raise OrchestrationError(
                "No se encontró el archivo de credenciales de Vertex AI." \
                " Verifica config/json_key_vertex.json."
            ) from exc
        try:
            self._llm = init_gemini_llm(
                credentials_info, project_id=project_id, location=location
            )
        except Exception as exc:  # pragma: no cover - depends on environment
            raise OrchestrationError("No se pudo inicializar el modelo Gemini.") from exc
        self.interpreter_agent = create_interpreter_agent(
            self.history_tool, llm=self._llm
        )
        self.sql_agent = create_sql_generator_agent(self.metadata_tool, llm=self._llm)
        self.executor_agent = create_executor_agent(self.bigquery_tool, llm=self._llm)
        self._llm_ready = True

    def _format_history(self, history: List[Dict[str, str]]) -> str:
        lines = []
        for item in history:
            role = item.get("role", "user")
            content = item.get("content", "")
            lines.append(f"[{role}] {content}")
        return "\n".join(lines)

    def _run_task(self, agent: Agent, task: Task) -> str:
        crew = Crew(
            agents=[agent],
            tasks=[task],
            process=Process.sequential,
        )
        result = crew.kickoff()
        output = getattr(task, "output", None)
        if isinstance(output, str) and output.strip():
            return output
        if isinstance(result, str):
            return result
        return str(result)

    def _parse_json(self, payload: str) -> Dict[str, object]:
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

    def _build_interpreter_prompt(self, user_message: str, history_text: str) -> str:
        return (
            "Analiza el mensaje del usuario y determina si necesitas generar una "
            "consulta SQL para responder.\n"
            "Historial:\n"
            f"{history_text or '(sin historial previo)'}\n\n"
            f"Mensaje actual: {user_message}\n\n"
            "Responde exclusivamente en JSON con las claves: \n"
            "- requires_sql: true o false\n"
            "- reasoning: explicación corta\n"
            "- refined_question: reformulación clara de la solicitud\n"
        )

    def _build_sql_prompt(
        self,
        refined_question: str,
        metadata_summary: str,
        interpreter_data: Dict[str, object],
    ) -> str:
        return (
            "Genera una consulta SQL válida para BigQuery que responda la pregunta."\
            " Utiliza solo tablas y columnas disponibles en los metadatos.\n\n"
            f"Pregunta refinada: {refined_question}\n"
            f"Contexto adicional: {interpreter_data.get('reasoning', '')}\n\n"
            "Metadatos disponibles:\n"
            f"{metadata_summary}\n\n"
            "Responde en JSON con las claves:\n"
            "- sql: cadena con la consulta o null si no es necesaria\n"
            "- analysis: explicación breve de la estrategia\n"
        )

    def _build_executor_prompt(
        self,
        user_message: str,
        sql: Optional[str],
        interpreter_data: Dict[str, object],
    ) -> str:
        base = [
            "Eres el agente ejecutor. Si recibes una consulta SQL utilízala para "
            "llamar al tool `bigquery_sql_runner` y obtener los datos."
        ]
        base.append(f"Mensaje original del usuario: {user_message}")
        base.append(f"Análisis del intérprete: {interpreter_data.get('reasoning', '')}")
        if sql:
            base.append("Consulta SQL a ejecutar:")
            base.append(f"```sql\n{sql}\n```")
            base.append(
                "Debes ejecutar la consulta con el tool antes de responder. Luego, "
                "analiza los resultados y devuelve una respuesta final en español."
            )
        else:
            base.append(
                "No hay consulta SQL que ejecutar. Explica la respuesta al usuario "
                "basándote en el análisis previo."
            )
        base.append(
            "Si el tool devuelve un error, informa claramente al usuario y sugiere "
            "acciones para resolverlo."
        )
        base.append("Tu respuesta final debe ser concisa y en formato Markdown.")
        return "\n\n".join(base)

    # ------------------------------------------------------------------
    def handle_message(self, user_message: str, history: List[Dict[str, str]]) -> OrchestrationResult:
        self._ensure_llm()

        if not all([self.interpreter_agent, self.sql_agent, self.executor_agent]):
            raise OrchestrationError(
                "Los agentes de CrewAI no se inicializaron correctamente."
            )

        history_text = self._format_history(history)
        self.history_tool.set_history(history_text)
        self.metadata_tool.set_metadata(self.metadata)
        self.bigquery_tool.reset()

        interpreter_prompt = self._build_interpreter_prompt(user_message, history_text)
        interpreter_task = Task(
            description=interpreter_prompt,
            agent=self.interpreter_agent,
            expected_output="JSON con requires_sql, reasoning y refined_question",
        )
        interpreter_raw = self._run_task(self.interpreter_agent, interpreter_task)
        interpreter_data = self._parse_json(interpreter_raw)

        requires_sql = bool(interpreter_data.get("requires_sql", False))
        refined_question = interpreter_data.get("refined_question") or user_message

        sql_data: Dict[str, object] = {"sql": None, "analysis": ""}
        if requires_sql:
            metadata_summary = self.metadata_tool.summary()
            sql_prompt = self._build_sql_prompt(refined_question, metadata_summary, interpreter_data)
            sql_task = Task(
                description=sql_prompt,
                agent=self.sql_agent,
                expected_output="JSON con sql y analysis",
            )
            sql_raw = self._run_task(self.sql_agent, sql_task)
            sql_data = self._parse_json(sql_raw)

        sql_text = sql_data.get("sql") if isinstance(sql_data, dict) else None
        if isinstance(sql_text, str) and not sql_text.strip():
            sql_text = None

        executor_prompt = self._build_executor_prompt(
            user_message,
            sql_text if isinstance(sql_text, str) else None,
            interpreter_data,
        )
        executor_task = Task(
            description=executor_prompt,
            agent=self.executor_agent,
            expected_output="Respuesta final en texto",
        )
        final_response = self._run_task(self.executor_agent, executor_task)

        return OrchestrationResult(
            response=final_response.strip(),
            interpreter_output=interpreter_data,
            sql_output=sql_data,
            sql=sql_text if isinstance(sql_text, str) else None,
            rows=self.bigquery_tool.last_result,
            error=self.bigquery_tool.last_error,
        )


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
                "No se pudo inicializar el orquestador de CrewAI."
            ) from exc
    return _orchestrator


__all__ = [
    "CrewOrchestrator",
    "OrchestrationResult",
    "OrchestrationError",
    "get_orchestrator",
]
