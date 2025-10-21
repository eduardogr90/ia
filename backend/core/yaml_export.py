from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from io import StringIO
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional dependency
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap
except Exception:  # pragma: no cover - fallback when ruamel is unavailable
    YAML = None  # type: ignore
    CommentedMap = OrderedDict  # type: ignore

from .validation import FlowEdge, FlowModel, FlowNode, build_graph

__all__ = ["to_yaml"]

if YAML is not None:
    _yaml = YAML()
    _yaml.default_flow_style = False
    _yaml.allow_unicode = True
    _yaml.explicit_start = False
    _yaml.indent(mapping=2, sequence=4, offset=2)
else:  # pragma: no cover - exercised when ruamel is missing
    _yaml = None

_NODE_TYPE_ORDER = {"question": 0, "action": 1, "message": 2}


def _dump_mapping(mapping: Mapping[str, Any], indent: int = 0) -> List[str]:  # pragma: no cover - simple formatter
    lines: List[str] = []
    prefix = "  " * indent
    for key, value in mapping.items():
        entry = f"{prefix}{key}:"
        if isinstance(value, Mapping):
            if value:
                lines.append(entry)
                lines.extend(_dump_mapping(value, indent + 1))
            else:
                lines.append(entry + " {}")
        elif isinstance(value, list):
            if value:
                lines.append(entry)
                for item in value:
                    if isinstance(item, Mapping):
                        lines.append("  " * (indent + 1) + "-")
                        lines.extend(_dump_mapping(item, indent + 2))
                    else:
                        lines.append("  " * (indent + 1) + f"- {item}")
            else:
                lines.append(entry + " []")
        else:
            lines.append(f"{entry} {value}")
    return lines


def _sorted_nodes(nodes: List[FlowNode]) -> List[FlowNode]:
    return sorted(
        nodes,
        key=lambda node: (_NODE_TYPE_ORDER.get(node.type, len(_NODE_TYPE_ORDER)), node.id),
    )


def _sorted_edges(edges: List[FlowEdge]) -> List[FlowEdge]:
    return sorted(edges, key=lambda edge: (edge.source, edge.target, edge.via_label or ""))


def _as_commented_map(data: Dict[str, Any]) -> CommentedMap:
    mapping = CommentedMap()
    for key in sorted(data):
        mapping[key] = data[key]
    return mapping


def _build_next(edges: List[FlowEdge]) -> Optional[Any]:
    if not edges:
        return None
    labelled_edges = [edge for edge in edges if edge.via_label]
    if len(edges) == 1 and not labelled_edges:
        return edges[0].target

    next_map = CommentedMap()
    for edge in _sorted_edges(edges):
        label = edge.via_label or "default"
        next_map[label] = edge.target
    return next_map


def _question_entry(node: FlowNode, edges: List[FlowEdge]) -> CommentedMap:
    entry = CommentedMap()
    entry["type"] = node.type
    data = node.data or {}
    if data.get("question"):
        entry["question"] = data["question"]
    if data.get("check"):
        entry["check"] = data["check"]
    expected = data.get("expectedAnswers")
    if isinstance(expected, list) and expected:
        entry["expected_answers"] = [str(value) for value in expected]
    next_value = _build_next(edges)
    if next_value is not None:
        entry["next"] = next_value
    metadata = data.get("metadata")
    if isinstance(metadata, dict) and metadata:
        entry["metadata"] = _as_commented_map(metadata)
    return entry


def _action_entry(node: FlowNode, edges: List[FlowEdge]) -> CommentedMap:
    entry = CommentedMap()
    entry["type"] = node.type
    data = node.data or {}
    if data.get("action"):
        entry["action"] = data["action"]
    parameters = data.get("parameters")
    if isinstance(parameters, dict) and parameters:
        entry["parameters"] = _as_commented_map(parameters)
    next_value = _build_next(edges)
    if next_value is not None:
        entry["next"] = next_value
    metadata = data.get("metadata")
    if isinstance(metadata, dict) and metadata:
        entry["metadata"] = _as_commented_map(metadata)
    return entry


def _message_entry(node: FlowNode, edges: List[FlowEdge]) -> CommentedMap:
    entry = CommentedMap()
    entry["type"] = node.type
    data = node.data or {}
    if data.get("message"):
        entry["message"] = data["message"]
    if data.get("severity"):
        entry["severity"] = data["severity"]
    metadata = data.get("metadata")
    if isinstance(metadata, dict) and metadata:
        entry["metadata"] = _as_commented_map(metadata)
    next_value = _build_next(edges)
    if next_value is not None:
        entry["next"] = next_value
    return entry


def _node_entry(node: FlowNode, edges: List[FlowEdge]) -> CommentedMap:
    if node.type == "question":
        return _question_entry(node, edges)
    if node.type == "action":
        return _action_entry(node, edges)
    return _message_entry(node, edges)


def to_yaml(model: FlowModel) -> str:
    _, outbound = build_graph(model)
    sorted_nodes = _sorted_nodes(model.nodes)

    flow_map = CommentedMap()
    for node in sorted_nodes:
        flow_map[node.id] = _node_entry(node, outbound.get(node.id, []))

    document = CommentedMap()
    document["id"] = model.id
    document["name"] = model.name
    if model.metadata:
        document["metadata"] = _as_commented_map(model.metadata)
    document["flow"] = flow_map

    if _yaml is None:
        lines = _dump_mapping(document)
        return "\n".join(lines) + ("\n" if lines else "")

    buffer = StringIO()
    _yaml.dump(document, buffer)
    return buffer.getvalue()
