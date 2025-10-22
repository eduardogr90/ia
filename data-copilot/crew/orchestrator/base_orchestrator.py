"""Base orchestration helpers for initializing Crew agents and clients."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

from crewai import Agent

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
from config import settings
from services.bigquery_client import BigQueryClient
from services.gemini_client import (
    DEFAULT_VERTEX_LOCATION,
    GeminiClient,
    init_gemini_llm,
    load_vertex_credentials,
)

from .results import OrchestrationError


class BaseCrewOrchestrator:
    """Shared initialization logic for the Crew orchestrator."""

    def __init__(
        self,
        metadata_dir: Path | None = None,
        bigquery_client: Optional[BigQueryClient] = None,
    ) -> None:
        self.metadata_dir = (
            metadata_dir
            or Path(__file__).resolve().parents[2] / "data" / "model"
        )
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

    def _ensure_llm(self) -> None:
        """Instantiate the shared Vertex AI LLM and the dependent agents."""
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
            llm = init_gemini_llm(
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
        if llm is None:
            raise OrchestrationError(
                "La inicialización del modelo Gemini devolvió un valor vacío.",
                detail="init_gemini_llm regresó None",
            )
        self._llm = llm
        self.interpreter_agent = create_interpreter_agent(
            self.history_tool, llm=self._llm
        )
        self.sql_agent = create_sql_generator_agent(self.metadata_tool, llm=self._llm)
        self.executor_agent = create_executor_agent(self.bigquery_tool, llm=self._llm)
        self.validation_tool.set_llm(self._llm)
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
        """Return a compact textual representation of the chat history."""
        lines = []
        for item in history:
            role = item.get("role", "user")
            content = item.get("content", "")
            lines.append(f"[{role}] {content}")
        return "\n".join(lines)
