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
    GeminiAnalysisTool,
    SQLMetadataTool,
    SQLValidationTool,
    create_analyzer_agent,
    create_executor_agent,
    create_interpreter_agent,
    create_sql_generator_agent,
    create_validator_agent,
    load_model_metadata,
)
from google.auth.exceptions import DefaultCredentialsError
from services.bigquery_client import BigQueryClient
from services.gemini_client import (
    DEFAULT_VERTEX_LOCATION,
    GeminiClient,
    init_gemini_llm,
    load_vertex_credentials,
)


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
        # ``SQLMetadataTool`` hereda de ``BaseTool`` (y, por extensión, de ``BaseModel``)
        # por lo que sus argumentos deben pasarse con palabras clave. Usar
        # ``self.metadata`` como argumento posicional provocaba el error
        # ``BaseModel.__init__() takes 1 positional argument but 2 were given`` al
        # inicializar el orquestador. Esto impedía crear los agentes y mostraba el
        # mensaje "No se pudo inicializar el orquestador de CrewAI". Al proporcionar
        # el diccionario de metadatos como argumento nombrado, la inicialización se
        # realiza correctamente con la versión actual de Pydantic/CrewAI.
        self.metadata_tool = SQLMetadataTool(metadata=self.metadata)
        try:
            self.bigquery_client = bigquery_client or BigQueryClient()
        except FileNotFoundError as exc:  # pragma: no cover - depends on deployment
            raise OrchestrationError(
                "No se encontró el archivo de credenciales de BigQuery en config/bq_service_account.json.",
                detail=str(exc),
            ) from exc
        except Exception as exc:  # pragma: no cover - depends on deployment
            raise OrchestrationError(
                "No se pudo inicializar el cliente de BigQuery.",
                detail=str(exc),
            ) from exc
        self.bigquery_tool = BigQueryQueryTool(client=self.bigquery_client)
        self.validation_tool = SQLValidationTool(
            metadata=self.metadata,
            max_limit=getattr(self.bigquery_client, "max_rows", 1000),
        )
        self.analysis_tool: GeminiAnalysisTool | None = None

        self.interpreter_agent: Agent | None = None
        self.sql_agent: Agent | None = None
        self.executor_agent: Agent | None = None
        self.validator_agent: Agent | None = None
        self.analyzer_agent: Agent | None = None

        self._llm_ready = False
        self._llm = None
        self._gemini_client: GeminiClient | None = None

    # ------------------------------------------------------------------
    def _ensure_llm(self) -> None:
        if self._llm_ready:
            return
        location = os.environ.get("VERTEX_LOCATION") or DEFAULT_VERTEX_LOCATION
        try:
            credentials_obj = load_vertex_credentials()
        except FileNotFoundError as exc:  # pragma: no cover - dependent on deployment
            raise OrchestrationError(
                "No se encontró el archivo de credenciales de Vertex AI."
                " Verifica data-copilot/config/json_key_vertex.json.",
                detail=str(exc),
            ) from exc
        try:
            self._llm = init_gemini_llm(
                credentials_obj,
                location=location,
            )
        except ValueError as exc:  # pragma: no cover - depends on deployment
            raise OrchestrationError(
                "No se pudo determinar el ID de proyecto de Vertex AI."
                " Define VERTEX_PROJECT_ID o agrega project_id al JSON de credenciales.",
                detail=str(exc),
            ) from exc
        except Exception as exc:  # pragma: no cover - depends on environment
            raise OrchestrationError(
                "No se pudo inicializar el modelo Gemini.",
                detail=str(exc),
            ) from exc
        self.interpreter_agent = create_interpreter_agent(
            self.history_tool, llm=self._llm
        )
        self.sql_agent = create_sql_generator_agent(self.metadata_tool, llm=self._llm)
        self.executor_agent = create_executor_agent(self.bigquery_tool, llm=self._llm)
        self.validator_agent = create_validator_agent(
            self.validation_tool, llm=self._llm
        )
        if self._gemini_client is None:
            self._gemini_client = GeminiClient(llm=self._llm)
        else:
            self._gemini_client.set_llm(self._llm)
        if self.analysis_tool is None:
            self.analysis_tool = GeminiAnalysisTool(client=self._gemini_client)
        else:
            self.analysis_tool.client = self._gemini_client
        self.analyzer_agent = create_analyzer_agent(self.analysis_tool, llm=self._llm)
        self._llm_ready = True

    def _format_history(self, history: List[Dict[str, str]]) -> str:
        lines = []
        for item in history:
            role = item.get("role", "user")
            content = item.get("content", "")
            lines.append(f"[{role}] {content}")
        return "\n".join(lines)

    def _run_task(self, agent: Agent, task: Task) -> str:
        agent_role = getattr(agent, "role", agent.__class__.__name__)
        crew = Crew(
            agents=[agent],
            tasks=[task],
            process=Process.sequential,
        )
        try:
            result = crew.kickoff()
        except Exception as exc:  # pragma: no cover - depends on runtime
            if self._contains_default_credentials_error(exc):
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
        output = getattr(task, "output", None)
        if isinstance(output, str) and output.strip():
            return output
        if isinstance(result, str):
            return result
        return str(result)

    def _contains_default_credentials_error(self, exc: Exception) -> bool:
        """Walk the exception chain looking for DefaultCredentialsError."""

        seen: set[int] = set()
        current: BaseException | None = exc
        while current is not None and id(current) not in seen:
            if isinstance(current, DefaultCredentialsError):
                return True
            seen.add(id(current))
            current = current.__cause__ or current.__context__
        return False

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
            "Eres el agente ejecutor. Recibiste una consulta SQL que ya fue validada."
            " Debes ejecutarla usando exclusivamente el tool `bigquery_sql_runner`."
        ]
        base.append(f"Mensaje original del usuario: {user_message}")
        base.append(f"Análisis del intérprete: {interpreter_data.get('reasoning', '')}")
        if sql:
            base.append("Consulta SQL a ejecutar:")
            base.append(f"```sql\n{sql}\n```")
            base.append(
                "Ejecuta el tool una sola vez y devuelve un resumen breve del resultado "
                "en formato JSON con las claves status (success/error) y detail."
            )
        else:
            base.append(
                "No hay consulta SQL que ejecutar. Responde con JSON {\"status\": \"skipped\"}."
            )
        base.append(
            "Si el tool devuelve un error, refleja ese error en la clave detail del JSON."
        )
        base.append("No realices interpretaciones ni ofrezcas conclusiones analíticas.")
        return "\n\n".join(base)

    def _build_validator_prompt(
        self,
        sql: str,
        refined_question: str,
    ) -> str:
        return (
            "Evalúa la sentencia SQL propuesta antes de su ejecución. Debes usar el tool"
            " `sql_validation_tool` para verificar que sea segura.\n"
            f"Consulta propuesta:\n```sql\n{sql}\n```\n"
            f"Pregunta del usuario: {refined_question}\n"
            "Responde exclusivamente en JSON con las claves: valid (bool), message,"
            " sanitized_sql, issues (lista) y warnings (lista)."
        )

    def _build_analyzer_prompt(
        self,
        refined_question: str,
        sql: str | None,
        rows: List[Dict[str, object]] | None,
    ) -> str:
        base = [
            "Analiza los datos devueltos por BigQuery y sintetiza los hallazgos en español.",
            "Debes usar el tool `gemini_result_analyzer` para generar el resumen narrativo y,"
            " si aplica, sugerencias de visualización.",
            f"Pregunta a resolver: {refined_question}",
        ]
        if sql:
            base.append("Consulta SQL ejecutada:")
            base.append(f"```sql\n{sql}\n```")
        base.append(
            f"Cantidad de filas disponibles: {len(rows) if rows else 0}."
            " Usa el tool para obtener la respuesta final."
        )
        base.append(
            "El resultado final debe ser JSON con las claves text y chart (esta última puede ser null)."
        )
        return "\n\n".join(base)

    # ------------------------------------------------------------------
    def handle_message(self, user_message: str, history: List[Dict[str, str]]) -> OrchestrationResult:
        self._ensure_llm()

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

            history_text = self._format_history(history)
            self.history_tool.set_history(history_text)
            self.metadata_tool.set_metadata(self.metadata)
            self.bigquery_tool.reset()
            self.validation_tool.set_metadata(self.metadata)

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
            validation_data: Dict[str, object] = {}
            analyzer_output: Dict[str, object] = {}
            if requires_sql:
                metadata_summary = self.metadata_tool.summary()
                sql_prompt = self._build_sql_prompt(
                    refined_question, metadata_summary, interpreter_data
                )
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

            sanitized_sql: str | None = None
            if requires_sql and isinstance(sql_text, str):
                self.validation_tool.set_candidate(sql_text, refined_question)
                validator_prompt = self._build_validator_prompt(
                    sql_text, refined_question
                )
                validator_task = Task(
                    description=validator_prompt,
                    agent=self.validator_agent,
                    expected_output="JSON con valid, message, sanitized_sql, issues, warnings",
                )
                validation_raw = self._run_task(self.validator_agent, validator_task)
                validation_data = self._parse_json(validation_raw)
                is_valid = bool(validation_data.get("valid"))
                sanitized_sql = (
                    validation_data.get("sanitized_sql")
                    if isinstance(validation_data, dict)
                    else None
                )
                if isinstance(sanitized_sql, str) and not sanitized_sql.strip():
                    sanitized_sql = None
                if not is_valid:
                    message = (
                        validation_data.get("message")
                        or "La consulta fue bloqueada por motivos de seguridad."
                    )
                    return OrchestrationResult(
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
                return OrchestrationResult(
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
                executor_prompt = self._build_executor_prompt(
                    user_message,
                    sanitized_sql,
                    interpreter_data,
                )
                executor_task = Task(
                    description=executor_prompt,
                    agent=self.executor_agent,
                    expected_output="Confirmación de ejecución o error",
                )
                _ = self._run_task(self.executor_agent, executor_task)
                rows = self.bigquery_tool.last_result
                execution_error = self.bigquery_tool.last_error
                if execution_error:
                    error_message = (
                        f"Error al ejecutar la consulta en BigQuery: {execution_error}"
                    )
                    return OrchestrationResult(
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
                analyzer_prompt = self._build_analyzer_prompt(
                    refined_question,
                    sanitized_sql,
                    rows or [],
                )
                analyzer_task = Task(
                    description=analyzer_prompt,
                    agent=self.analyzer_agent,
                    expected_output="JSON con text y chart",
                )
                analyzer_raw = self._run_task(self.analyzer_agent, analyzer_task)
                analyzer_output = self._parse_json(analyzer_raw)
                response_text = analyzer_output.get("text")
                if not isinstance(response_text, str) or not response_text.strip():
                    response_text = str(analyzer_raw).strip()
                chart = analyzer_output.get("chart") if isinstance(analyzer_output, dict) else None
                chart_payload = chart if isinstance(chart, dict) else None
                return OrchestrationResult(
                    response=response_text.strip(),
                    interpreter_output=interpreter_data,
                    sql_output=sql_data,
                    validation_output=validation_data,
                    analyzer_output=analyzer_output if isinstance(analyzer_output, dict) else {},
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
            return OrchestrationResult(
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
