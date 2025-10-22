"""Interpreter agent responsible for understanding user intent."""
from __future__ import annotations

from typing import Any

from crewai import Agent

from .tools.conversation_history import ConversationHistoryTool


def create_interpreter_agent(
    history_tool: ConversationHistoryTool | None = None,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for understanding the user's intent."""

    return Agent(
        role="InterpreterAgent",
        goal=(
            "Analizar la intención del usuario, comprender el contexto de la "
            "conversación y determinar si se requiere una consulta SQL,"
            " identificando además si la respuesta esperada es un único valor"
            " o un conjunto de múltiples valores tabulares."
        ),
        backstory=(
            "Eres un analista de datos senior especializado en preparar consultas"
            " para respuestas tabulares estrictas."
            " Tu rol es decidir si hace falta SQL, clarificar la petición"
            " y adelantar señales sobre si la salida deberá ser un valor único"
            " o una matriz de resultados, sin prometer narrativas adicionales."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[history_tool] if history_tool else [],
        llm=llm,
    )


__all__ = ["create_interpreter_agent"]
