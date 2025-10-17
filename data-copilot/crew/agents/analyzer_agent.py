"""Analyzer agent that crafts narrative answers from query results."""
from __future__ import annotations

import json
from typing import Any

from crewai import Agent
from crewai.tools import BaseTool
from pydantic import Field


class GeminiAnalysisTool(BaseTool):
    """Bridges the Analyzer agent with the Gemini client."""

    name: str = "gemini_result_analyzer"
    description: str = (
        "Genera un análisis narrativo en español a partir de los resultados devueltos por BigQuery."
    )
    client: "GeminiClient"
    question: str = Field(default="")
    sql: str = Field(default="")
    results: list[dict[str, Any]] = Field(default_factory=list)

    def set_context(
        self,
        *,
        question: str | None = None,
        sql: str | None = None,
        results: list[dict[str, Any]] | None = None,
    ) -> None:
        self.question = question or ""
        self.sql = sql or ""
        self.results = results or []

    def _run(self, _: str | None = None) -> str:
        analysis = self.client.analyze_results(
            self.results,
            question=self.question or None,
            sql=self.sql or None,
        )
        return json.dumps(analysis, ensure_ascii=False)


def create_analyzer_agent(
    analysis_tool: GeminiAnalysisTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for crafting the narrative answer."""

    return Agent(
        role="AnalyzerAgent",
        goal=(
            "Interpretar los resultados numéricos provenientes de BigQuery y "
            "comunicar hallazgos en español en un lenguaje ejecutivo."
        ),
        backstory=(
            "Eres un analista de inteligencia de negocio que sintetiza datos en "
            "historias claras para la dirección. Utiliza el tool de análisis para "
            "generar texto y sugerencias visuales."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[analysis_tool],
        llm=llm,
    )


__all__ = ["GeminiAnalysisTool", "create_analyzer_agent"]

from services.gemini_client import GeminiClient  # noqa: E402  (import tardío)

GeminiAnalysisTool.model_rebuild()
