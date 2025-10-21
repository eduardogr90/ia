from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from backend.core.validation import (
    FlowEdge,
    FlowModel,
    FlowNode,
    enumerate_paths,
    validate_flow,
)


def build_model(*, nodes, edges) -> FlowModel:
    return FlowModel(id="flow", name="Flow", nodes=list(nodes), edges=list(edges))


def test_validate_flow_detects_cycles() -> None:
    nodes = [
        FlowNode(id="start", type="question", data={"question": "Begin?", "expectedAnswers": ["yes", "no"]}),
        FlowNode(id="loop", type="action", data={"action": "loop"}),
        FlowNode(id="end", type="message", data={"message": "done"}),
    ]
    edges = [
        FlowEdge(source="start", target="loop", viaLabel="yes"),
        FlowEdge(source="loop", target="start"),
        FlowEdge(source="start", target="end", viaLabel="no"),
    ]
    model = build_model(nodes=nodes, edges=edges)

    valid, errors, warnings = validate_flow(model)

    assert not valid
    assert any("Cycle detected" in error for error in errors)
    assert "Flow must contain at least one terminal" not in errors
    assert warnings  # warnings may be present for unreachable nodes


def test_validate_flow_reports_missing_nodes() -> None:
    nodes = [FlowNode(id="only", type="question", data={"question": "Hi?"})]
    edges = [FlowEdge(source="only", target="ghost")]
    model = build_model(nodes=nodes, edges=edges)

    valid, errors, _ = validate_flow(model)

    assert not valid
    assert any("unknown target" in error for error in errors)


def test_validate_flow_validates_expected_answers() -> None:
    nodes = [
        FlowNode(
            id="q1",
            type="question",
            data={"question": "Continue?", "expectedAnswers": ["yes", "no"]},
        ),
        FlowNode(id="m1", type="message", data={"message": "done"}),
    ]
    edges = [
        FlowEdge(source="q1", target="m1", viaLabel="maybe"),
    ]
    model = build_model(nodes=nodes, edges=edges)

    valid, errors, _ = validate_flow(model)

    assert not valid
    assert any("expected answers" in error for error in errors)


def test_validate_flow_warns_on_multiple_roots_and_message_outgoing() -> None:
    nodes = [
        FlowNode(id="first", type="question", data={"question": "Start?"}),
        FlowNode(id="second", type="question", data={"question": "Alt?"}),
        FlowNode(id="message", type="message", data={"message": "done"}),
        FlowNode(id="follow", type="action", data={"action": "next"}),
    ]
    edges = [
        FlowEdge(source="first", target="message"),
        FlowEdge(source="second", target="follow"),
        FlowEdge(source="message", target="follow"),
    ]
    model = build_model(nodes=nodes, edges=edges)

    valid, errors, warnings = validate_flow(model)

    assert not valid
    assert any("terminal" in error for error in errors)
    assert any("Multiple start nodes" in warning for warning in warnings)
    assert any("Message node" in warning for warning in warnings)


def test_enumerate_paths_returns_root_to_terminal_sequences() -> None:
    nodes = [
        FlowNode(id="start", type="question", data={"question": "Start?"}),
        FlowNode(id="action", type="action", data={"action": "work"}),
        FlowNode(id="fail", type="message", data={"message": "fail", "severity": "error"}),
        FlowNode(id="success", type="message", data={"message": "ok"}),
    ]
    edges = [
        FlowEdge(source="start", target="action", viaLabel="go"),
        FlowEdge(source="action", target="success"),
        FlowEdge(source="start", target="fail", viaLabel="stop"),
    ]
    model = build_model(nodes=nodes, edges=edges)

    paths = enumerate_paths(model)

    assert paths
    assert {tuple(step.keys()) for path in paths for step in path} <= {("nodeId",), ("nodeId", "via")}
    assert any(path[-1]["nodeId"] == "success" for path in paths)
    assert any(path[1].get("via") == "go" for path in paths if len(path) > 1)


def test_enumerate_paths_returns_empty_when_no_terminals() -> None:
    nodes = [
        FlowNode(id="start", type="question", data={"question": "Begin?"}),
        FlowNode(id="middle", type="action", data={"action": "work"}),
    ]
    edges = [FlowEdge(source="start", target="middle")]
    model = build_model(nodes=nodes, edges=edges)

    paths = enumerate_paths(model)

    assert paths == []


def test_enumerate_paths_handles_long_chain() -> None:
    nodes = [
        FlowNode(id=f"n{i}", type="question" if i == 0 else "action", data={"question": "?"})
        for i in range(25)
    ]
    nodes.append(FlowNode(id="end", type="message", data={"message": "done"}))
    edges = [FlowEdge(source=f"n{i}", target=f"n{i + 1}") for i in range(24)]
    edges.append(FlowEdge(source="n24", target="end"))
    model = build_model(nodes=nodes, edges=edges)

    paths = enumerate_paths(model)

    assert len(paths) == 1
    assert paths[0][-1]["nodeId"] == "end"


@pytest.mark.parametrize("count", [5, 12, 32])
def test_validate_flow_handles_parallel_paths(count: int) -> None:
    nodes = [
        FlowNode(id="start", type="question", data={"question": "Begin?"}),
        FlowNode(id="terminal", type="message", data={"message": "done"}),
    ]
    edges = []
    for index in range(count):
        node_id = f"branch_{index}"
        nodes.append(FlowNode(id=node_id, type="action", data={"action": f"branch_{index}"}))
        edges.append(FlowEdge(source="start", target=node_id, viaLabel=str(index)))
        edges.append(FlowEdge(source=node_id, target="terminal"))

    model = build_model(nodes=nodes, edges=edges)

    valid, errors, warnings = validate_flow(model)
    paths = enumerate_paths(model)

    assert valid
    assert not errors
    assert warnings == []
    assert len(paths) == count
    assert {path[-1]["nodeId"] for path in paths} == {"terminal"}
