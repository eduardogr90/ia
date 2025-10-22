"""Agent dedicated to generating SQL queries from user intent."""
from __future__ import annotations

from typing import Any

from crewai import Agent

from .tools.sql_metadata_tool import SQLMetadataTool


def create_sql_generator_agent(
    metadata_tool: SQLMetadataTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent that converts intents into SQL queries."""

    return Agent(
        role="SQLGeneratorAgent",
        goal=(
            "Transformar preguntas de negocio en consultas SQL válidas basadas en "
            "los metadatos del modelo y en las convenciones de BigQuery."
        ),
        backstory=(
            "Eres un experto en modelado de datos analíticos y puedes combinar "
            "diferentes tablas según las relaciones definidas en los metadatos."
        ),
        allow_delegation=False,
        verbose=False,
        tools=[metadata_tool],
        llm=llm,
    )


__all__ = ["create_sql_generator_agent"]
