"""Conversation persistence utilities."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from config import settings


@dataclass
class Conversation:
    """Representation of a single chat conversation."""

    id: str
    messages: List[Dict[str, object]]
    title: str
    created_at: str

    @classmethod
    def from_file(cls, path: Path) -> "Conversation":
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        messages = data.get("messages", [])
        if not isinstance(messages, list):
            messages = []
        return cls(
            id=data.get("id", path.stem),
            messages=messages,
            title=data.get("title") or data.get("conversation_title") or data.get("id", path.stem),
            created_at=data.get("created_at") or datetime.now(timezone.utc).isoformat(),
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "id": self.id,
            "title": self.title,
            "created_at": self.created_at,
            "messages": self.messages,
        }


class ConversationService:
    """Handle conversation lifecycle for each user."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    # Internal helpers -------------------------------------------------
    def _user_dir(self, username: str) -> Path:
        path = self.base_dir / username
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _conversation_path(self, username: str, conv_id: str) -> Path:
        return self._user_dir(username) / f"{conv_id}.json"

    # Public API -------------------------------------------------------
    def list_conversations(self, username: str) -> List[Conversation]:
        user_dir = self._user_dir(username)
        conversations = []
        for file in sorted(user_dir.glob("*.json"), reverse=True):
            try:
                conversations.append(Conversation.from_file(file))
            except json.JSONDecodeError:
                # Skip malformed conversations so they don't break the UI
                continue
        return conversations

    def create_conversation(self, username: str) -> Conversation:
        base_id = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        conv_id = base_id
        counter = 1
        # Ensure that conversation IDs are unique even when multiple chats are
        # created within the same second.
        while self._conversation_path(username, conv_id).exists():
            conv_id = f"{base_id}_{counter}"
            counter += 1
        created_at = datetime.now(timezone.utc).isoformat()
        conversation = Conversation(
            id=conv_id,
            messages=[],
            title=f"Conversación {conv_id}",
            created_at=created_at,
        )
        self._save_conversation(username, conversation)
        return conversation

    def load_conversation(self, username: str, conv_id: str) -> Optional[Conversation]:
        path = self._conversation_path(username, conv_id)
        if not path.exists():
            return None
        try:
            return Conversation.from_file(path)
        except json.JSONDecodeError:
            return None

    def append_message(
        self,
        username: str,
        conv_id: str,
        role: str,
        content: str,
        *,
        extra: Optional[Dict[str, object]] = None,
    ) -> Optional[Conversation]:
        conversation = self.load_conversation(username, conv_id)
        if not conversation:
            return None
        timestamp = datetime.now(timezone.utc).isoformat()
        message: Dict[str, object] = {
            "role": role,
            "content": content,
            "timestamp": timestamp,
        }
        if extra:
            message.update(extra)
        conversation.messages.append(message)
        if role == "user":
            preview = content.strip()
            if preview:
                truncated = (preview[:77] + "…") if len(preview) > 78 else preview
                if conversation.title.startswith("Conversación") or not conversation.title.strip():
                    conversation.title = truncated
        self._save_conversation(username, conversation)
        return conversation

    def delete_conversation(self, username: str, conv_id: str) -> bool:
        path = self._conversation_path(username, conv_id)
        if path.exists():
            path.unlink()
            return True
        return False

    # Internal persistence helper ------------------------------------
    def _save_conversation(self, username: str, conversation: Conversation) -> None:
        path = self._conversation_path(username, conversation.id)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(conversation.to_dict(), fh, indent=2, ensure_ascii=False)


conversation_service = ConversationService(settings.CONVERSATIONS_DIR)
