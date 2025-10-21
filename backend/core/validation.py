from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "FlowModel",
    "FlowNode",
    "FlowEdge",
    "validate_flow",
    "enumerate_paths",
    "build_graph",
]

MAX_PATH_DEPTH = 1000


class FlowNode(BaseModel):
    id: str
    type: str
    label: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class FlowEdge(BaseModel):
    id: Optional[str] = None
    source: str
    target: str
    via_label: Optional[str] = Field(default=None, alias="viaLabel")
    data: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class FlowModel(BaseModel):
    id: str
    name: str
    nodes: List[FlowNode]
    edges: List[FlowEdge] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", populate_by_name=True)


def _find_duplicate_ids(values: Iterable[str]) -> Set[str]:
    seen: Set[str] = set()
    duplicates: Set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        else:
            seen.add(value)
    return duplicates


def build_graph(model: FlowModel) -> Tuple[Dict[str, List[FlowEdge]], Dict[str, List[FlowEdge]]]:
    """Return inbound and outbound edge mappings keyed by node identifier."""

    inbound: Dict[str, List[FlowEdge]] = defaultdict(list)
    outbound: Dict[str, List[FlowEdge]] = defaultdict(list)

    for node in model.nodes:
        inbound.setdefault(node.id, [])
        outbound.setdefault(node.id, [])

    for edge in model.edges:
        outbound.setdefault(edge.source, []).append(edge)
        inbound.setdefault(edge.target, []).append(edge)

    return inbound, outbound


def _message_terminals(nodes: Dict[str, FlowNode], outbound: Dict[str, List[FlowEdge]]) -> List[str]:
    return [
        node_id
        for node_id, node in nodes.items()
        if node.type == "message" and len(outbound.get(node_id, [])) == 0
    ]


def _question_expected_answers(node: FlowNode) -> Set[str]:
    expected = node.data.get("expectedAnswers")
    if isinstance(expected, (list, tuple, set)):
        return {str(value) for value in expected}
    return set()


def _format_cycle(path: List[str], repeated: str) -> str:
    start = path.index(repeated)
    cycle = path[start:] + [repeated]
    return " -> ".join(cycle)


def validate_flow(model: FlowModel) -> Tuple[bool, List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    if not model.nodes:
        return False, ["Flow must contain at least one node."], warnings

    node_ids = [node.id for node in model.nodes]
    duplicates = _find_duplicate_ids(node_ids)
    if duplicates:
        errors.append("Duplicate node identifiers detected: " + ", ".join(sorted(duplicates)))

    nodes_by_id: Dict[str, FlowNode] = {node.id: node for node in model.nodes}

    inbound, outbound = build_graph(model)

    seen_edge_signatures: Set[Tuple[str, str, Optional[str]]] = set()
    for edge in model.edges:
        if edge.source not in nodes_by_id:
            errors.append(f"Edge references unknown source node '{edge.source}'.")
        if edge.target not in nodes_by_id:
            errors.append(f"Edge references unknown target node '{edge.target}'.")
        signature = (edge.source, edge.target, edge.via_label)
        if signature in seen_edge_signatures:
            warnings.append(
                f"Duplicate edge detected from '{edge.source}' to '{edge.target}' with label '{edge.via_label or ''}'."
            )
        else:
            seen_edge_signatures.add(signature)

    roots = [node_id for node_id in nodes_by_id if len(inbound.get(node_id, [])) == 0]
    if not roots:
        errors.append("Flow must contain at least one start node (no incoming edges).")
    elif len(roots) > 1:
        warnings.append("Multiple start nodes detected; execution order may be ambiguous.")

    terminals = _message_terminals(nodes_by_id, outbound)
    if not terminals:
        errors.append("Flow must contain at least one terminal message node (message without outgoing edges).")

    for node_id, node in nodes_by_id.items():
        outgoing = outbound.get(node_id, [])
        if node.type == "message" and outgoing:
            warnings.append(f"Message node '{node_id}' has outgoing edges and will not terminate the flow.")
        if node.type == "question":
            expected = _question_expected_answers(node)
            if expected:
                for edge in outgoing:
                    if edge.via_label and edge.via_label not in expected:
                        errors.append(
                            f"Edge from question '{node_id}' uses label '{edge.via_label}' not present in expected answers."
                        )

    colour: Dict[str, int] = {node_id: 0 for node_id in nodes_by_id}  # 0=white,1=gray,2=black
    path_stack: List[str] = []
    cycle_found = False

    def dfs(node_id: str) -> None:
        nonlocal cycle_found
        if cycle_found:
            return
        colour[node_id] = 1
        path_stack.append(node_id)
        for edge in outbound.get(node_id, []):
            target = edge.target
            if target not in nodes_by_id:
                continue
            if colour.get(target, 0) == 1:
                cycle_found = True
                errors.append("Cycle detected: " + _format_cycle(path_stack, target))
                return
            if colour.get(target, 0) == 0:
                dfs(target)
                if cycle_found:
                    return
        path_stack.pop()
        colour[node_id] = 2

    for root in roots:
        if colour.get(root, 0) == 0:
            dfs(root)
            if cycle_found:
                break

    if not cycle_found:
        for node_id in nodes_by_id:
            if colour.get(node_id, 0) == 0:
                dfs(node_id)
                if cycle_found:
                    break

    reachable: Set[str] = set()

    def traverse(start: str) -> None:
        if start in reachable:
            return
        reachable.add(start)
        for edge in outbound.get(start, []):
            if edge.target in nodes_by_id:
                traverse(edge.target)

    for root in roots:
        traverse(root)

    unreachable = [node_id for node_id in nodes_by_id if node_id not in reachable]
    if unreachable:
        warnings.append("Unreachable nodes detected: " + ", ".join(sorted(unreachable)))

    return len(errors) == 0, errors, warnings


def enumerate_paths(model: FlowModel) -> List[List[Dict[str, Any]]]:
    if not model.nodes:
        return []

    inbound, outbound = build_graph(model)
    nodes_by_id: Dict[str, FlowNode] = {node.id: node for node in model.nodes}

    roots = [node_id for node_id in nodes_by_id if len(inbound.get(node_id, [])) == 0]
    terminals = set(_message_terminals(nodes_by_id, outbound))
    if not roots or not terminals:
        return []

    results: List[List[Dict[str, Any]]] = []

    def backtrack(node_id: str, path: List[Dict[str, Any]], depth: int) -> None:
        if depth > MAX_PATH_DEPTH:
            return
        if node_id in terminals:
            results.append([dict(step) for step in path])
        for edge in outbound.get(node_id, []):
            target = edge.target
            if target not in nodes_by_id:
                continue
            if any(step["nodeId"] == target for step in path):
                continue
            step: Dict[str, Any] = {"nodeId": target}
            if edge.via_label:
                step["via"] = edge.via_label
            path.append(step)
            backtrack(target, path, depth + 1)
            path.pop()

    for root in roots:
        if root not in nodes_by_id:
            continue
        backtrack(root, [{"nodeId": root}], 1)

    return results
