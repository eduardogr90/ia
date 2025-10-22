from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from backend.core.validation import FlowEdge, FlowModel, FlowNode
from backend.core.yaml_export import to_yaml


def test_to_yaml_matches_expected_structure() -> None:
    model = FlowModel(
        id="sample-flow",
        name="Sample flow",
        metadata={"owner": "data-team", "version": 1},
        nodes=[
            FlowNode(
                id="start",
                type="question",
                data={
                    "question": "Where to?",
                    "expectedAnswers": ["yes", "no"],
                    "metadata": {"channel": "inbound"},
                },
            ),
            FlowNode(
                id="action",
                type="action",
                data={"action": "dispatch", "parameters": {"timeout": 30}},
            ),
            FlowNode(
                id="end",
                type="message",
                data={"message": "Completed", "severity": "info"},
            ),
        ],
        edges=[
            FlowEdge(source="start", target="action", viaLabel="yes"),
            FlowEdge(source="start", target="end", viaLabel="no"),
            FlowEdge(source="action", target="end"),
        ],
    )

    yaml_text = to_yaml(model)

    expected = (
        "id: sample-flow\n"
        "name: Sample flow\n"
        "metadata:\n"
        "  owner: data-team\n"
        "  version: 1\n"
        "flow:\n"
        "  start:\n"
        "    type: question\n"
        "    question: Where to?\n"
        "    expected_answers:\n"
        "      - yes\n"
        "      - no\n"
        "    next:\n"
        "      yes: action\n"
        "      no: end\n"
        "    metadata:\n"
        "      channel: inbound\n"
        "  action:\n"
        "    type: action\n"
        "    action: dispatch\n"
        "    parameters:\n"
        "      timeout: 30\n"
        "    next: end\n"
        "  end:\n"
        "    type: message\n"
        "    message: Completed\n"
        "    severity: info\n"
    )

    assert yaml_text == expected
