from __future__ import annotations

import json

from openpyxl import Workbook

import web_runtime
from web_runtime import WebRunManager, _artifact_preview_kind, _table_preview_from_path


def test_csv_preview_is_bounded_and_marks_remaining_rows(tmp_path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text("地区,数值\n中国,12\n日本,8\n韩国,6\n", encoding="utf-8-sig")

    preview = _table_preview_from_path(path, max_rows=2, max_columns=10)

    assert preview["columns"] == ["地区", "数值"]
    assert preview["rows"] == [["中国", "12"], ["日本", "8"]]
    assert preview["preview_row_count"] == 2
    assert preview["truncated"] is True


def test_json_record_preview_preserves_columns_and_nested_values(tmp_path) -> None:
    path = tmp_path / "events.json"
    path.write_text(
        json.dumps(
            [
                {"事件": "洪水", "范围": {"国家": "中国"}},
                {"事件": "火灾", "范围": {"国家": "澳大利亚"}},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    preview = _table_preview_from_path(path, max_rows=5, max_columns=5)

    assert preview["columns"] == ["事件", "范围"]
    assert preview["rows"][0] == ["洪水", '{"国家": "中国"}']
    assert preview["truncated"] is False


def test_json_table_contract_uses_declared_columns(tmp_path) -> None:
    path = tmp_path / "declared-table.json"
    path.write_text(
        json.dumps({"columns": ["国家", "指数"], "rows": [["中国", 88], ["印度", 74]]}, ensure_ascii=False),
        encoding="utf-8",
    )

    preview = _table_preview_from_path(path, max_rows=5, max_columns=5)

    assert preview["columns"] == ["国家", "指数"]
    assert preview["rows"] == [["中国", "88"], ["印度", "74"]]


def test_xlsx_preview_uses_active_sheet_and_formula_values(tmp_path) -> None:
    path = tmp_path / "metrics.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "指标"
    sheet.append(["国家", "得分"])
    sheet.append(["中国", 91])
    workbook.save(path)
    workbook.close()

    preview = _table_preview_from_path(path, max_rows=5, max_columns=5)

    assert preview["sheet_name"] == "指标"
    assert preview["columns"] == ["国家", "得分"]
    assert preview["rows"] == [["中国", "91"]]


def test_preview_kind_exposes_supported_workspace_formats(tmp_path) -> None:
    assert _artifact_preview_kind(tmp_path / "map.webp") == "image"
    assert _artifact_preview_kind(tmp_path / "table.tsv") == "table"
    assert _artifact_preview_kind(tmp_path / "report.html") == "html"
    assert _artifact_preview_kind(tmp_path / "notes.md") == "markdown"
    assert _artifact_preview_kind(tmp_path / "brief.pdf") == "pdf"
    assert _artifact_preview_kind(tmp_path / "method.py") == "text"
    assert _artifact_preview_kind(tmp_path / "interactive.svg") == ""


def test_workspace_files_keep_inputs_and_outputs_separate(tmp_path, monkeypatch) -> None:
    workspace = tmp_path / "thread-workspace"
    inputs = workspace / "inputs"
    outputs = workspace / "outputs" / "figures"
    inputs.mkdir(parents=True)
    outputs.mkdir(parents=True)
    (inputs / "source.csv").write_text("name,value\nChina,1\n", encoding="utf-8")
    (outputs / "map.html").write_text("<!doctype html><title>Map</title>", encoding="utf-8")
    monkeypatch.setattr(web_runtime.storage_manager, "get_workspace", lambda _thread_id: workspace)

    files = WebRunManager.list_workspace_files("thread-1")

    assert [item["path"] for item in files["inputs"]] == ["source.csv"]
    assert files["inputs"][0]["root"] == "inputs"
    assert [item["path"] for item in files["outputs"]] == ["figures/map.html"]
    assert files["outputs"][0]["preview_kind"] == "html"
