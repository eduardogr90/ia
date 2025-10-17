"""Crew orchestrator that coordinates the Interpreter, SQL generator and Executor agents."""
from __future__ import annotations

import json
import math
import os
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
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
from config import settings
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


class CrewOrchestrator:
    """Coordinates the CrewAI agents to respond to user questions."""

    def __init__(
        self,
        metadata_dir: Path | None = None,
        bigquery_client: Optional[BigQueryClient] = None,
    ) -> None:
        self.metadata_dir = metadata_dir or Path(__file__).resolve().parent.parent / "data" / "model"
        self.metadata = load_model_metadata(self.metadata_dir)

        self.prompt_cost_per_1k = settings.GEMINI_PROMPT_COST_PER_1K
        self.completion_cost_per_1k = settings.GEMINI_COMPLETION_COST_PER_1K

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
        self.interpreter_agent = create_interpreter_agent(llm=self._llm)
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

    def _normalize_text(self, text: str | None) -> str:
        if not text:
            return ""
        normalized = unicodedata.normalize("NFD", text)
        without_marks = "".join(
            char for char in normalized if unicodedata.category(char) != "Mn"
        )
        return without_marks.lower()

    def _analyze_question_semantics(self, question: str) -> Dict[str, object]:
        normalized = self._normalize_text(question)
        is_comparative = any(
            keyword in normalized
            for keyword in (
                " vs ",
                "vs.",
                "compar",
                "diferenc",
                "respecto",
                "frente a",
                "variac",
                "evolu",
                "tendenc",
                "increment",
                "disminu",
            )
        )
        wants_visual = any(
            keyword in normalized
            for keyword in ("graf", "visualiz", "chart", "diagrama")
        )

        iteration_patterns = (
            "por mes",
            "por trimestre",
            "por ano",
            "por año",
            "por semana",
            "por dia",
            "por día",
            "mes a mes",
            "trimestre a trimestre",
            "semana a semana",
            "dia a dia",
            "día a día",
            "mensualmente",
            "trimestralmente",
            "semanalmente",
            "diariamente",
        )
        has_iteration = any(pattern in normalized for pattern in iteration_patterns)

        month_names = (
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "setiembre",
            "octubre",
            "noviembre",
            "diciembre",
        )

        period_candidates: List[str] = []
        if not is_comparative and not has_iteration:
            if any(name in normalized for name in month_names) or any(
                keyword in normalized
                for keyword in (
                    " del mes ",
                    "en el mes ",
                    "durante el mes",
                    "ultimo mes",
                    "último mes",
                    "mes pasado",
                )
            ):
                period_candidates.append("monthly")
            if "trimestre" in normalized or "trimestr" in normalized:
                period_candidates.append("quarterly")
            if any(
                keyword in normalized
                for keyword in (
                    " ano ",
                    " año ",
                    " anual",
                    "durante 20",
                    "en 20",
                    "del 20",
                )
            ):
                period_candidates.append("yearly")

        breakdown_blockers = {
            "monthly": ("semana", "semanal", "dia", "día", "diario"),
            "quarterly": ("mes", "mensual", "semana", "semanal"),
            "yearly": ("mes", "mensual", "trimestre", "trimestr", "semana", "semanal"),
        }

        aggregated_period = None
        for candidate in period_candidates:
            blockers = breakdown_blockers.get(candidate, ())
            if any(blocker in normalized for blocker in blockers):
                continue
            aggregated_period = candidate
            break

        period_labels = {
            "monthly": ("mensual", "semanal"),
            "quarterly": ("trimestral", "mensual"),
            "yearly": ("anual", "trimestral"),
        }
        aggregated_label, breakdown_unit = (None, None)
        if aggregated_period:
            aggregated_label, breakdown_unit = period_labels.get(aggregated_period, (None, None))

        return {
            "normalized": normalized,
            "is_comparative": is_comparative,
            "wants_visual": wants_visual,
            "aggregated_period": aggregated_period,
            "aggregated_label": aggregated_label,
            "breakdown_unit": breakdown_unit,
        }

    def _run_task(
        self,
        agent: Agent,
        task: Task,
        *,
        input_context: object | None = None,
        extra_metadata: Optional[Dict[str, object]] = None,
        uses_llm: bool = True,
    ) -> tuple[str, Dict[str, object]]:
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
            prompt_tokens = self._estimate_tokens(task.description)
            completion_tokens = self._estimate_tokens(response_text)
            tokens = {
                "prompt": prompt_tokens,
                "completion": completion_tokens,
                "total": prompt_tokens + completion_tokens,
            }
            trace_entry["tokens"] = tokens
            trace_entry["llm_response"] = response_text
            cost = self._estimate_cost(prompt_tokens, completion_tokens)
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

    def _estimate_tokens(self, text: str | None) -> int:
        if not text:
            return 0
        normalized = str(text).strip()
        if not normalized:
            return 0
        return max(1, math.ceil(len(normalized) / 4))

    def _estimate_cost(self, prompt_tokens: int, completion_tokens: int) -> Optional[float]:
        cost = 0.0
        has_cost = False
        if self.prompt_cost_per_1k > 0:
            cost += (prompt_tokens / 1000.0) * self.prompt_cost_per_1k
            has_cost = True
        if self.completion_cost_per_1k > 0:
            cost += (completion_tokens / 1000.0) * self.completion_cost_per_1k
            has_cost = True
        if not has_cost:
            return None
        return round(cost, 8)

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

    def _build_interpreter_prompt(
        self, user_message: str, history_text: str, has_history: bool
    ) -> str:
        base = [
            "Analiza la intención del usuario y determina si requiere una consulta SQL.",
        ]
        if has_history:
            base.append(
                "Utiliza el historial proporcionado solo cuando aporte contexto relevante y no hagas suposiciones ajenas a él."
            )
            base.append("Historial:")
            base.append(history_text)
        else:
            base.append(
                "Trabaja únicamente con el mensaje actual; no menciones ni supongas mensajes anteriores."
            )
        base.append("")
        base.append(f"Mensaje actual: {user_message}")
        base.append("")
        base.append("Responde exclusivamente en JSON con las claves:")
        base.append("- requires_sql: true o false")
        base.append("- reasoning: explicación corta")
        base.append("- refined_question: reformulación clara de la solicitud")
        return "\n".join(base)

    def _build_sql_prompt(
        self,
        refined_question: str,
        metadata_summary: str,
        interpreter_data: Dict[str, object],
        semantics: Dict[str, object],
    ) -> str:
        base = [
            "Genera una consulta SQL siguiendo el BigQuery Standard SQL que responda la pregunta.",
            "Utiliza solo tablas y columnas disponibles en los metadatos y respeta todos los filtros implícitos en la solicitud.",
            f"Pregunta refinada: {refined_question}",
            f"Contexto adicional: {interpreter_data.get('reasoning', '')}",
            "",
            "Metadatos disponibles:",
            metadata_summary,
            "",
        ]

        if semantics.get("is_comparative"):
            base.append(
                "La pregunta es comparativa o evolutiva. Mantén exactamente la granularidad indicada por el usuario y no añadas desgloses adicionales."
            )
        elif semantics.get("aggregated_period"):
            period_label = semantics.get("aggregated_label") or "solicitado"
            breakdown_unit = semantics.get("breakdown_unit") or "más pequeño"
            base.append(
                "La solicitud pide un agregado "
                f"{period_label}. Además del total requerido, incorpora en la consulta un desglose {breakdown_unit} "
                "del mismo periodo solo con fines analíticos."
            )
            base.append(
                "Puedes usar CTEs, UNION ALL o GROUPING SETS para entregar ambos niveles en un mismo resultado, identificando cada fila con una columna que indique el nivel de agregación (por ejemplo, nivel_agregacion)."
            )
            base.append(
                "No alteres la métrica original ni cambies los filtros solicitados. Si el desglose adicional no es viable con los metadatos disponibles, indícalo con claridad en analysis."
            )
        else:
            base.append(
                "Respeta la granularidad mencionada por el usuario y evita añadir niveles temporales no pedidos."
            )

        base.append("")
        base.append("Responde en JSON con las claves:")
        base.append("- sql: cadena con la consulta o null si no es necesaria")
        base.append(
            "- analysis: explicación breve de la estrategia e indica cualquier decisión sobre granularidad"
        )
        return "\n".join(base)

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
        semantics: Dict[str, object],
    ) -> str:
        base = [
            "Analiza los resultados devueltos por BigQuery y responde en español claro a la pregunta.",
            "Responde primero a la métrica o total solicitado exactamente como lo pidió el usuario.",
            "Si solo hay un valor disponible, limita la respuesta a una frase directa y concreta.",
            "Sé preciso, conciso y basa tus conclusiones únicamente en los resultados mostrados.",
            "Debes usar el tool `gemini_result_analyzer` para generar el resumen narrativo.",
        ]
        if semantics.get("is_comparative"):
            base.append(
                "La solicitud es comparativa o evolutiva; responde siguiendo esa estructura y evita agregar desgloses extra."
            )
        elif semantics.get("aggregated_period"):
            period_label = semantics.get("aggregated_label") or "principal"
            breakdown_unit = semantics.get("breakdown_unit") or "secundario"
            base.append(
                "Presenta el total "
                f"{period_label} primero y, de forma opcional y breve, comenta hallazgos relevantes del desglose {breakdown_unit}."
            )
        base.append(
            "Cuando existan varios registros, asume que la interfaz mostrará una tabla con los totales relevantes; no describas columnas irrelevantes."
        )
        if semantics.get("wants_visual"):
            base.append(
                "El usuario solicitó una visualización. Puedes sugerirla brevemente solo si los datos lo justifican; de lo contrario, mantén chart en null."
            )
        else:
            base.append(
                "El usuario no pidió gráficos; mantén chart en null y no propongas visualizaciones."
            )
        if sql:
            base.append("Consulta SQL ejecutada:")
            base.append(f"```sql\n{sql}\n```")
        base.append(
            f"Cantidad de filas disponibles: {len(rows) if rows else 0}. Usa el tool para obtener la respuesta final."
        )
        base.append(
            "El resultado final debe ser JSON con las claves text y chart (esta última puede ser null)."
        )
        base.append(f"Pregunta a resolver: {refined_question}")
        return "\n\n".join(base)

    # ------------------------------------------------------------------
    def handle_message(self, user_message: str, history: List[Dict[str, str]]) -> OrchestrationResult:
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

            history_text = self._format_history(history)
            has_history = bool(history_text.strip())
            if has_history:
                self.history_tool.set_history(history_text)
                if hasattr(self.interpreter_agent, "tools"):
                    self.interpreter_agent.tools = [self.history_tool]
            else:
                self.history_tool.set_history("")
                if hasattr(self.interpreter_agent, "tools"):
                    self.interpreter_agent.tools = []
            self.metadata_tool.set_metadata(self.metadata)
            self.bigquery_tool.reset()
            self.validation_tool.set_metadata(self.metadata)

            interpreter_prompt = self._build_interpreter_prompt(
                user_message, history_text, has_history
            )
            interpreter_task = Task(
                description=interpreter_prompt,
                agent=self.interpreter_agent,
                expected_output="JSON con requires_sql, reasoning y refined_question",
            )
            interpreter_raw, interpreter_trace = self._run_task(
                self.interpreter_agent,
                interpreter_task,
                input_context=user_message,
            )
            append_trace(interpreter_trace)
            interpreter_data = self._parse_json(interpreter_raw)

            requires_sql = bool(interpreter_data.get("requires_sql", False))
            refined_question = interpreter_data.get("refined_question") or user_message

            question_semantics = self._analyze_question_semantics(refined_question)

            sql_data: Dict[str, object] = {"sql": None, "analysis": ""}
            validation_data: Dict[str, object] = {}
            analyzer_output: Dict[str, object] = {}
            if requires_sql:
                metadata_summary = self.metadata_tool.summary()
                sql_prompt = self._build_sql_prompt(
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
                sql_raw, sql_trace = self._run_task(
                    self.sql_agent,
                    sql_task,
                    input_context=refined_question,
                )
                append_trace(sql_trace)
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
                validation_raw, validation_trace = self._run_task(
                    self.validator_agent,
                    validator_task,
                    extra_metadata={"input_sql": sql_text},
                )
                validation_data = self._parse_json(validation_raw)
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
                validation_trace["validation_result"] = "OK" if is_valid else "RECHAZADA"
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
                _, executor_trace = self._run_task(
                    self.executor_agent,
                    executor_task,
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
                analyzer_prompt = self._build_analyzer_prompt(
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
                analyzer_raw, analyzer_trace = self._run_task(
                    self.analyzer_agent,
                    analyzer_task,
                    extra_metadata={"input_rows": len(rows or [])},
                )
                analyzer_output = self._parse_json(analyzer_raw)
                append_trace(analyzer_trace)
                response_text = analyzer_output.get("text")
                if not isinstance(response_text, str) or not response_text.strip():
                    response_text = str(analyzer_raw).strip()
                chart = analyzer_output.get("chart") if isinstance(analyzer_output, dict) else None
                chart_payload = chart if isinstance(chart, dict) else None
                return finalize_result(
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
