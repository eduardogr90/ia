"""Tool providing access to the conversation history."""
from __future__ import annotations

from crewai.tools import BaseTool
from pydantic import Field


class ConversationHistoryTool(BaseTool):
    """Expose the chat history as a CrewAI tool."""

    name: str = "conversation_history"
    description: str = (
        "Proporciona el historial completo de la conversación para ayudar a "
        "interpretar la nueva solicitud del usuario."
    )
    history: str = Field(
        default="",
        description="Historial completo de mensajes previos en la conversación.",
    )

    def set_history(self, history: str) -> None:
        """Update the cached conversation history."""

        self.history = history

    def _run(self) -> str:
        return self.history or "(La conversación inicia con este mensaje)"
