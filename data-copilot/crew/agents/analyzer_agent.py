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
        "Convierte los resultados tabulares de BigQuery en una salida estrictamente estructurada:"
        " una única línea que indique si se trata de un valor único o de múltiples valores y,"
        " a continuación, una tabla o matriz en Markdown que contenga únicamente los datos relevantes."
        " No debe añadir comentarios, conclusiones ni descripciones adicionales."
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
            "Interpretar los resultados numéricos de BigQuery y devolverlos sin narrativa,"
            " limitando la respuesta a una sola línea indicativa (valor único vs. múltiples resultados)"
            " seguida exclusivamente por una tabla o matriz en Markdown con los datos solicitados."
        ),
        backstory=(
            "Eres un analista obsesionado con la consistencia tabular."
            " No elaboras conclusiones ni explicaciones:"
            " únicamente clasificas si la respuesta corresponde a un valor único o a múltiples valores"
            " y estructuras los datos en una tabla o matriz con posibles subniveles cuando aplica."
        ),
        allow_delegation=False,
        verbose=False,
        tools=[analysis_tool],
        llm=llm,
    )


__all__ = ["GeminiAnalysisTool", "create_analyzer_agent"]

from services.gemini_client import GeminiClient  # noqa: E402  (import tardío)

GeminiAnalysisTool.model_rebuild()
