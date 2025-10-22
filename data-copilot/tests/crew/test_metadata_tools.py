"""Tests for metadata normalization and summarisation utilities."""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow importing ``crew`` package when running tests from repo root.
sys.path.append(str(Path(__file__).resolve().parents[2]))

from crew.agents.agents_utils import load_model_metadata
from crew.agents.tools.sql_metadata_tool import SQLMetadataTool


def test_load_model_metadata_normalizes_spanish_schema(tmp_path) -> None:
    """Metadata JSON files with Spanish keys should be harmonised."""

    payload = {
        "tb_result_energia": {
            "descripcion": [
                "Esta tabla registra cada venta individual.",
                "Nivel de granularidad: 1 fila = 1 venta.",
            ],
            "path": "accom-dw.accom_ventas.tb_result_energia",
            "columns": {
                "acc_estado_accom_con_bajas": {
                    "descripcion": "Estado del contrato en ACCOM incluyendo bajas.",
                    "tipo_dato": "STRING",
                    "sinonimos": ["estado_con_bajas"],
                },
                "acc_producto_servicio": {
                    "descripcion": "Tipo de producto comercializado.",
                    "tipo_dato": "STRING",
                },
            },
        }
    }

    metadata_path = tmp_path / "tb_result_energia.json"
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    metadata = load_model_metadata(tmp_path)
    assert "tb_result_energia" in metadata

    table_entry = metadata["tb_result_energia"]
    assert table_entry["description"].startswith("Esta tabla registra")
    assert table_entry["path"] == "accom-dw.accom_ventas.tb_result_energia"

    columns = table_entry["columns"]
    assert set(columns.keys()) == {
        "acc_estado_accom_con_bajas",
        "acc_producto_servicio",
    }
    assert (
        columns["acc_estado_accom_con_bajas"]["description"]
        == "Estado del contrato en ACCOM incluyendo bajas."
    )
    assert columns["acc_estado_accom_con_bajas"]["type"] == "STRING"
    assert columns["acc_estado_accom_con_bajas"]["synonyms"] == [
        "estado_con_bajas"
    ]


def test_metadata_tool_summary_includes_columns(tmp_path) -> None:
    """The metadata summary should contain table and column details."""

    payload = {
        "tb_result_energia": {
            "descripcion": "Ventas energéticas.",
            "path": "accom-dw.accom_ventas.tb_result_energia",
            "columnas": {
                "acc_estado_accom_con_bajas": {
                    "descripcion": "Estado del contrato.",
                    "tipo_dato": "STRING",
                },
            },
        }
    }

    metadata_path = tmp_path / "tb_result_energia.json"
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    metadata = load_model_metadata(tmp_path)
    tool = SQLMetadataTool(metadata=metadata)

    summary = tool.summary()
    assert "Tabla: tb_result_energia" in summary
    assert "Descripción: Ventas energéticas." in summary
    assert "acc_estado_accom_con_bajas" in summary
    assert "Tipo: STRING" in summary
