from __future__ import annotations

import json
import os
import shutil
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .utils import safe_slugify

__all__ = [
    "Storage",
    "StorageError",
    "ProjectNotFoundError",
    "FlowNotFoundError",
    "get_storage",
]


class StorageError(RuntimeError):
    """Base error for storage-related failures."""


class ProjectNotFoundError(StorageError):
    """Raised when a project identifier cannot be resolved."""


class FlowNotFoundError(StorageError):
    """Raised when a flow identifier cannot be resolved within a project."""


_LOCKS: Dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()
_STORAGE_INSTANCES: Dict[str, "Storage"] = {}
_STORAGE_INSTANCES_LOCK = threading.Lock()


def _timestamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _lock_for(path: Path) -> threading.RLock:
    key = str(path.resolve())
    with _LOCKS_GUARD:
        if key not in _LOCKS:
            _LOCKS[key] = threading.RLock()
        return _LOCKS[key]


@contextmanager
def _locked(path: Path):
    lock = _lock_for(path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class Storage:
    """Manage project and flow persistence on the local filesystem."""

    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._index_path = self.root / "projects.json"

    # ------------------------------------------------------------------
    # Low-level JSON helpers
    # ------------------------------------------------------------------
    def _read_json(self, path: Path, default: Any = None) -> Any:
        with _locked(path):
            if not path.exists():
                return default
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)

    def _write_json(self, path: Path, data: Any) -> None:
        serialized = json.dumps(data, ensure_ascii=False, indent=2)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        with _locked(path):
            with tmp_path.open("w", encoding="utf-8") as handle:
                handle.write(serialized)
            os.replace(tmp_path, path)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _project_dir(self, project_id: str) -> Path:
        return self.root / project_id

    def _project_file(self, project_id: str) -> Path:
        return self._project_dir(project_id) / "project.json"

    def _flows_dir(self, project_id: str) -> Path:
        return self._project_dir(project_id) / "flows"

    def _flow_file(self, project_id: str, flow_id: str) -> Path:
        return self._flows_dir(project_id) / f"{flow_id}.json"

    def _ensure_index(self) -> Dict[str, Any]:
        data = self._read_json(self._index_path, default=None)
        if data is None:
            data = {"projects": []}
            self._write_json(self._index_path, data)
        elif "projects" not in data or not isinstance(data["projects"], list):
            raise StorageError("Invalid projects index structure.")
        return data

    def _ensure_project_exists(self, project_id: str) -> Dict[str, Any]:
        project_path = self._project_file(project_id)
        project = self._read_json(project_path, default=None)
        if project is None:
            raise ProjectNotFoundError(f"Project '{project_id}' not found.")
        return project

    @staticmethod
    def _unique_slug(value: str, existing: Iterable[str], default: str) -> str:
        base = safe_slugify(value or "", fallback="item")
        if not base:
            base = default
        candidate = base
        suffix = 2
        existing_set = set(existing)
        while candidate in existing_set:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def _update_project_index(self, projects: List[Dict[str, Any]]) -> None:
        payload = {"projects": projects}
        self._write_json(self._index_path, payload)

    def _touch_project(self, project_id: str, timestamp: Optional[str] = None) -> Dict[str, Any]:
        timestamp = timestamp or _timestamp()
        with _locked(self._index_path):
            index_data = self._ensure_index()
            projects = index_data["projects"]
            project_entry: Optional[Dict[str, Any]] = None
            for entry in projects:
                if entry.get("id") == project_id:
                    entry["updatedAt"] = timestamp
                    project_entry = entry
                    break
            if project_entry is None:
                raise ProjectNotFoundError(f"Project '{project_id}' not found.")
            self._update_project_index(projects)

        project_path = self._project_file(project_id)
        with _locked(project_path):
            project_data = self._read_json(project_path)
            if project_data is None:
                raise ProjectNotFoundError(f"Project '{project_id}' not found.")
            project_data["updatedAt"] = timestamp
            self._write_json(project_path, project_data)
        return project_data

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def list_projects(self) -> List[Dict[str, Any]]:
        index_data = self._ensure_index()
        projects = index_data.get("projects", [])
        projects = sorted(projects, key=lambda item: item.get("name", ""))
        return projects

    def get_project(self, project_id: str) -> Dict[str, Any]:
        return self._ensure_project_exists(project_id)

    def create_project(self, name: str) -> Dict[str, Any]:
        with _locked(self._index_path):
            index_data = self._ensure_index()
            projects = index_data["projects"]
            project_id = self._unique_slug(name, (project["id"] for project in projects), "project")
            timestamp = _timestamp()
            project_record = {
                "id": project_id,
                "name": name,
                "createdAt": timestamp,
                "updatedAt": timestamp,
            }
            projects.append(project_record)
            self._update_project_index(projects)

        project_dir = self._project_dir(project_id)
        project_dir.mkdir(parents=True, exist_ok=True)
        self._write_json(self._project_file(project_id), project_record)
        self._flows_dir(project_id).mkdir(parents=True, exist_ok=True)
        return project_record

    def rename_project(self, project_id: str, name: str) -> Dict[str, Any]:
        timestamp = _timestamp()
        with _locked(self._index_path):
            index_data = self._ensure_index()
            projects = index_data["projects"]
            project_entry: Optional[Dict[str, Any]] = None
            for entry in projects:
                if entry.get("id") == project_id:
                    entry["name"] = name
                    entry["updatedAt"] = timestamp
                    project_entry = entry
                    break
            if project_entry is None:
                raise ProjectNotFoundError(f"Project '{project_id}' not found.")
            self._update_project_index(projects)

        project_path = self._project_file(project_id)
        with _locked(project_path):
            project_data = self._read_json(project_path)
            if project_data is None:
                raise ProjectNotFoundError(f"Project '{project_id}' not found.")
            project_data["name"] = name
            project_data["updatedAt"] = timestamp
            self._write_json(project_path, project_data)
        return project_data

    def delete_project(self, project_id: str) -> None:
        self._ensure_project_exists(project_id)
        project_dir = self._project_dir(project_id)
        if project_dir.exists():
            shutil.rmtree(project_dir)

        with _locked(self._index_path):
            index_data = self._ensure_index()
            projects = index_data["projects"]
            new_projects = [project for project in projects if project.get("id") != project_id]
            if len(new_projects) == len(projects):
                raise ProjectNotFoundError(f"Project '{project_id}' not found.")
            self._update_project_index(new_projects)

    def list_flows(self, project_id: str) -> List[Dict[str, Any]]:
        self._ensure_project_exists(project_id)
        flows_dir = self._flows_dir(project_id)
        flows_dir.mkdir(parents=True, exist_ok=True)
        results: List[Dict[str, Any]] = []
        for path in sorted(flows_dir.glob("*.json")):
            flow = self._read_json(path)
            if not isinstance(flow, dict):
                continue
            results.append(
                {
                    "id": flow.get("id", path.stem),
                    "name": flow.get("name", path.stem),
                    "updatedAt": flow.get("updatedAt"),
                }
            )
        return results

    def create_flow(self, project_id: str, name: str) -> Dict[str, Any]:
        self._ensure_project_exists(project_id)
        flows_dir = self._flows_dir(project_id)
        flows_dir.mkdir(parents=True, exist_ok=True)
        existing_ids = [path.stem for path in flows_dir.glob("*.json")]
        flow_id = self._unique_slug(name, existing_ids, "flow")
        timestamp = _timestamp()
        flow_record: Dict[str, Any] = {
            "id": flow_id,
            "name": name,
            "nodes": [],
            "edges": [],
            "metadata": {},
            "createdAt": timestamp,
            "updatedAt": timestamp,
        }
        self._write_json(self._flow_file(project_id, flow_id), flow_record)
        self._touch_project(project_id, timestamp)
        return flow_record

    def load_flow(self, project_id: str, flow_id: str) -> Dict[str, Any]:
        self._ensure_project_exists(project_id)
        flow_path = self._flow_file(project_id, flow_id)
        flow = self._read_json(flow_path, default=None)
        if flow is None:
            raise FlowNotFoundError(f"Flow '{flow_id}' not found in project '{project_id}'.")
        return flow

    def save_flow(self, project_id: str, flow_id: str, flow_data: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_project_exists(project_id)
        flow_path = self._flow_file(project_id, flow_id)
        existing = self._read_json(flow_path, default=None)
        if existing is None:
            raise FlowNotFoundError(f"Flow '{flow_id}' not found in project '{project_id}'.")

        timestamp = _timestamp()
        created_at = existing.get("createdAt", timestamp)
        flow_data = dict(flow_data)
        flow_data["id"] = flow_id
        flow_data.setdefault("metadata", existing.get("metadata", {}))
        flow_data["createdAt"] = created_at
        flow_data["updatedAt"] = timestamp
        self._write_json(flow_path, flow_data)
        self._touch_project(project_id, timestamp)
        return flow_data

    def delete_flow(self, project_id: str, flow_id: str) -> None:
        self._ensure_project_exists(project_id)
        flow_path = self._flow_file(project_id, flow_id)
        if not flow_path.exists():
            raise FlowNotFoundError(f"Flow '{flow_id}' not found in project '{project_id}'.")
        flow_path.unlink()
        self._touch_project(project_id)


def get_storage(root: Optional[str | Path] = None) -> Storage:
    base_path = Path(root).resolve() if root is not None else Path(__file__).resolve().parent.parent / "storage"
    key = str(base_path)
    with _STORAGE_INSTANCES_LOCK:
        if key not in _STORAGE_INSTANCES:
            _STORAGE_INSTANCES[key] = Storage(base_path)
        return _STORAGE_INSTANCES[key]
