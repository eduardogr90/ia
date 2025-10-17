"""Interpreter agent responsible for understanding user intent."""
from __future__ import annotations

from typing import Any

from crewai import Agent

from .tools.conversation_history import ConversationHistoryTool


def create_interpreter_agent(
    history_tool: ConversationHistoryTool,
    llm: Any | None = None,
) -> Agent:
    """Create the agent responsible for understanding the user's intent."""

    return Agent(
        role="InterpreterAgent",
        goal=(
            "Analizar la intención del usuario, comprende el contexto de la "
            "conversación y determinar si se requiere una consulta SQL."
        ),
        backstory=(
            "Eres un analista de datos senior con una gran capacidad para "
            "interpretar preguntas en lenguaje natural y decidir si es necesario "
            "consultar la base de datos para responderlas."
        ),
        allow_delegation=False,
        verbose=True,
        tools=[history_tool],
        llm=llm,
    )


__all__ = ["create_interpreter_agent"]
