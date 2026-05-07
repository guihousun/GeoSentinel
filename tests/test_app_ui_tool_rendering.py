from __future__ import annotations

import unittest
from unittest import mock
import sys
import tempfile
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

for _name in ("geopandas", "folium", "rasterio", "matplotlib", "matplotlib.cm", "matplotlib.colors"):
    sys.modules.setdefault(_name, mock.MagicMock())
sys.modules.setdefault("streamlit_folium", mock.MagicMock(st_folium=mock.MagicMock()))
sys.modules.setdefault("app_logic", mock.MagicMock())
sys.modules.setdefault("gee_auth", mock.MagicMock())

import app_ui


class AppUiToolRenderingTests(unittest.TestCase):
    def test_streaming_signatures_are_stable_and_change_with_inputs(self) -> None:
        chat = [("user", "show Beijing lights"), ("assistant", "working")]
        logs = [{"messages": [AIMessage(content="Plan", name="NTL_Engineer")]}]

        chat_sig = app_ui._compute_streaming_chat_signature(
            chat,
            is_running=True,
            run_last_terminal_kind="",
        )
        self.assertEqual(
            chat_sig,
            app_ui._compute_streaming_chat_signature(
                list(chat),
                is_running=True,
                run_last_terminal_kind="",
            ),
        )
        self.assertNotEqual(
            chat_sig,
            app_ui._compute_streaming_chat_signature(
                chat + [("assistant", "done")],
                is_running=True,
                run_last_terminal_kind="",
            ),
        )
        self.assertNotEqual(
            chat_sig,
            app_ui._compute_streaming_chat_signature(
                chat,
                is_running=False,
                run_last_terminal_kind="final_answer",
            ),
        )

        lifecycle_sig = app_ui._compute_streaming_lifecycle_signature(
            logs,
            is_running=True,
            run_last_terminal_kind="",
        )
        self.assertEqual(
            lifecycle_sig,
            app_ui._compute_streaming_lifecycle_signature(
                list(logs),
                is_running=True,
                run_last_terminal_kind="",
            ),
        )
        self.assertNotEqual(
            lifecycle_sig,
            app_ui._compute_streaming_lifecycle_signature(
                logs + [{"messages": [AIMessage(content="Run", name="Code_Assistant")]}],
                is_running=True,
                run_last_terminal_kind="",
            ),
        )

        reasoning_sig = app_ui._compute_streaming_reasoning_signature(logs)
        self.assertEqual(reasoning_sig, app_ui._compute_streaming_reasoning_signature(list(logs)))
        self.assertNotEqual(
            reasoning_sig,
            app_ui._compute_streaming_reasoning_signature(
                logs + [{"messages": [AIMessage(content="Summarize", name="NTL_Engineer")]}]
            ),
        )

    def test_streaming_render_flags_skip_when_poll_has_no_changes(self) -> None:
        state = {
            "chat_history": [("user", "question")],
            "analysis_logs": [{"messages": [AIMessage(content="Thinking", name="NTL_Engineer")]}],
            "is_running": True,
            "run_last_terminal_kind": "",
        }
        app_ui._remember_streaming_live_signatures(state)

        flags = app_ui._compute_streaming_render_flags(state, events_consumed=False)

        self.assertEqual(
            flags,
            {
                "chat": False,
                "lifecycle": False,
                "reasoning": False,
                "any": False,
            },
        )

    def test_streaming_render_flags_redraw_only_changed_placeholders(self) -> None:
        state = {
            "chat_history": [("user", "question")],
            "analysis_logs": [{"messages": [AIMessage(content="Thinking", name="NTL_Engineer")]}],
            "is_running": True,
            "run_last_terminal_kind": "",
        }
        app_ui._remember_streaming_live_signatures(state)
        state["analysis_logs"] = state["analysis_logs"] + [
            {"messages": [AIMessage(content="Need data", name="Data_Searcher")]}
        ]

        flags = app_ui._compute_streaming_render_flags(state, events_consumed=True)

        self.assertFalse(flags["chat"])
        self.assertTrue(flags["lifecycle"])
        self.assertTrue(flags["reasoning"])
        self.assertTrue(flags["any"])

    def test_classifies_skill_python_csv_and_image_paths(self) -> None:
        cases = [
            ("/skills/workflow-self-evolution/SKILL.md", "skill"),
            ("/skills/workflow-self-evolution/references/metrics.json", "json"),
            ("outputs/analyze_lights.py", "python"),
            ("outputs/province_stats.csv", "csv"),
            ("outputs/timeline.png", "image"),
        ]

        for path, expected_kind in cases:
            with self.subTest(path=path):
                info = app_ui._classify_file_reference(path)
                self.assertEqual(info["kind"], expected_kind)
                self.assertIn("label", info)

    def test_skill_nested_file_label_preserves_scope_and_extension(self) -> None:
        info = app_ui._classify_file_reference("/skills/workflow-self-evolution/references/metrics.json")

        self.assertEqual(info["kind"], "json")
        self.assertEqual(info["language"], "json")
        self.assertIn("Skill", info["label"])
        self.assertIn("JSON", info["label"])

    def test_read_file_tool_metadata_is_attached_to_reasoning_section(self) -> None:
        events = [
            {
                "messages": [
                    AIMessage(
                        content="",
                        name="NTL_Engineer",
                        tool_calls=[
                            {
                                "name": "read_file",
                                "args": {
                                    "file_path": "/skills/workflow-self-evolution/SKILL.md",
                                },
                                "id": "call_read_skill",
                            }
                        ],
                    ),
                    ToolMessage(
                        content="---\nname: workflow-self-evolution\n",
                        name="read_file",
                        tool_call_id="call_read_skill",
                    ),
                ]
            }
        ]

        grouped = app_ui._build_reasoning_sections(events)

        tool_section = next(item for item in grouped if item["kind"] == "tool")
        meta = tool_section["tool_meta"]["call_read_skill"]
        self.assertEqual(meta["name"], "read_file")
        self.assertEqual(meta["file_refs"][0]["kind"], "skill")
        self.assertEqual(meta["file_refs"][0]["path"], "/skills/workflow-self-evolution/SKILL.md")

    def test_read_files_extracts_multiple_file_references(self) -> None:
        msg = AIMessage(
            content="",
            name="Code_Assistant",
            tool_calls=[
                {
                    "name": "read_files",
                    "args": {
                        "paths": ["outputs/a.csv", "outputs/plot.png"],
                    },
                    "id": "call_read_many",
                }
            ],
        )

        metadata = app_ui._extract_tool_call_metadata(msg)

        refs = metadata["call_read_many"]["file_refs"]
        self.assertEqual([ref["kind"] for ref in refs], ["csv", "image"])

    def test_edit_file_metadata_includes_target_and_replacement_preview(self) -> None:
        msg = AIMessage(
            content="",
            name="NTL_Engineer",
            tool_calls=[
                {
                    "name": "edit_file",
                    "args": {
                        "file_path": "/workflow-self-evolution/references/metrics.json",
                        "old_string": '"usage_count": 1',
                        "new_string": '"usage_count": 2',
                    },
                    "id": "call_edit_metrics",
                }
            ],
        )

        metadata = app_ui._extract_tool_call_metadata(msg)

        meta = metadata["call_edit_metrics"]
        self.assertEqual(meta["name"], "edit_file")
        self.assertEqual(meta["file_refs"][0]["kind"], "json")
        self.assertEqual(meta["change_preview"]["before"], '"usage_count": 1')
        self.assertEqual(meta["change_preview"]["after"], '"usage_count": 2')

    def test_gif_image_artifact_uses_html_img_to_preserve_animation(self) -> None:
        self.assertTrue(app_ui._should_render_image_as_html_gif("outputs/timeline.gif"))
        self.assertFalse(app_ui._should_render_image_as_html_gif("outputs/plot.png"))

    def test_png_image_artifact_uses_inline_html_preview_when_data_uri_is_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            image_path = Path(tmpdir) / "plot.png"
            image_path.write_bytes(b"not-a-real-image-but-exists")
            fake_st = mock.MagicMock()
            with mock.patch.object(app_ui, "st", fake_st), \
                mock.patch.object(app_ui, "_resolve_workspace_artifact_ref", return_value=image_path), \
                mock.patch.object(app_ui, "_artifact_display_ref", return_value="outputs/plot.png"), \
                mock.patch.object(app_ui, "_image_data_uri", return_value="data:image/png;base64,abc"):
                app_ui._render_image_artifact("outputs/plot.png", key_prefix="test")

        markdown_payloads = [str(call.args[0]) for call in fake_st.markdown.call_args_list]
        self.assertTrue(any('<img src="data:image/png;base64,abc"' in payload for payload in markdown_payloads))
        fake_st.image.assert_not_called()

    def test_static_code_block_html_has_language_header_without_line_numbers(self) -> None:
        html = app_ui._static_code_block_html(
            'print("ntl")\nvalue = 1',
            language="python",
            title="china_province_ntl_2020.py",
        )

        self.assertIn("Python script", html)
        self.assertIn("china_province_ntl_2020.py", html)
        self.assertIn("ntl-code-block", html)
        self.assertIn("ntl-code-kind-python", html)
        self.assertNotIn("line-number", html)

    def test_text_code_block_html_has_kind_class_for_wrapping(self) -> None:
        html = app_ui._static_code_block_html(
            "2020 Annual GDP - 34 Province-Level Administrative Regions",
            language="text",
            title="Product",
        )

        self.assertIn("ntl-code-kind-text", html)
        self.assertIn("2020 Annual GDP", html)

    def test_render_static_python_code_uses_streamlit_code_with_full_body(self) -> None:
        code = 'CONFIG = {"items": [{"name": "Beijing"}, {"name": "Shanghai"}]}\n\ndef main():\n    return CONFIG\n'
        fake_st = mock.MagicMock()

        with mock.patch.object(app_ui, "st", fake_st):
            app_ui._render_static_code_block(code, language="python", title="script.py")

        fake_st.code.assert_called_once_with(
            code,
            language="python",
            line_numbers=False,
            wrap_lines=True,
            height="content",
            width="stretch",
        )
        fake_st.markdown.assert_not_called()

    def test_json_block_text_is_pretty_and_sanitized(self) -> None:
        text = app_ui._json_block_text({"status": "success", "path": "D:/NTL-GPT-Clone/user_data/t1/outputs/a.csv"})

        self.assertIn('"status": "success"', text)
        self.assertIn("outputs/a.csv", text)
        self.assertNotIn("D:/NTL-GPT-Clone/user_data", text)

    def test_reasoning_dot_uses_dark_readable_theme(self) -> None:
        dot = app_ui._build_reasoning_dot(
            {
                "nodes": [
                    {"data": {"id": "start", "label": "START", "kind": "system"}},
                    {"data": {"id": "ai", "label": "AI: NTL_Engineer", "kind": "ai"}},
                ],
                "edges": [{"data": {"source": "start", "target": "ai"}, "classes": "flow"}],
            }
        )

        self.assertIn('bgcolor="#071021"', dot)
        self.assertIn('fontcolor="#eaf1ff"', dot)

    def test_reasoning_headers_distinguish_agent_roles(self) -> None:
        engineer = app_ui._reasoning_agent_meta("NTL_Engineer")
        code = app_ui._reasoning_agent_meta("Code_Assistant")
        html = app_ui._reasoning_header_html("AI", engineer["label"], engineer["role"], accent=engineer["accent"])

        self.assertNotEqual(engineer["accent"], code["accent"])
        self.assertIn("ntl-reasoning-header", html)
        self.assertIn("ntl-reasoning-header-plain", html)
        self.assertIn("NTL_Engineer", html)
        self.assertIn(engineer["accent"], html)
        self.assertEqual(engineer["role"], "")

    def test_reasoning_header_with_subtitle_uses_stacked_layout(self) -> None:
        html = app_ui._reasoning_header_html("TOOL", "read_file", "Tool output", accent="#93c5fd")

        self.assertIn("ntl-reasoning-header-stacked", html)
        self.assertIn("Tool output", html)

    def test_subsequent_human_messages_are_agent_instructions(self) -> None:
        grouped = app_ui._build_reasoning_sections(
            [
                {"messages": [HumanMessage(content="Original user query")]},
                {"messages": [AIMessage(content="Route to subagent", name="NTL_Engineer")]},
                {"messages": [HumanMessage(content="Execute the saved script")]},
            ]
        )

        self.assertEqual(grouped[0]["kind"], "human")
        self.assertEqual(grouped[2]["kind"], "instruction")

    def test_reasoning_text_html_escapes_content(self) -> None:
        html = app_ui._reasoning_text_html("<script>alert(1)</script>", accent="#fb7185")

        self.assertIn("ntl-reasoning-text", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<script>alert", html)

    def test_reasoning_rich_text_styles_inline_code_and_tables(self) -> None:
        html = app_ui._reasoning_rich_text_html(
            "## Result Summary\n\n| Item | Value |\n| --- | --- |\n| Status | `success` |\n\nUse **annual** `ee.Reducer.sum()`.",
            accent="#7dd3fc",
        )

        self.assertIn("ntl-reasoning-rich", html)
        self.assertIn("ntl-reasoning-md-heading", html)
        self.assertIn("<table>", html)
        self.assertIn("<code>success</code>", html)
        self.assertIn("<strong>annual</strong>", html)
        self.assertIn("<code>ee.Reducer.sum()</code>", html)

    def test_reasoning_rich_text_escapes_html(self) -> None:
        html = app_ui._reasoning_rich_text_html("Bad <script>alert(1)</script>", accent="#fb7185")

        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<script>alert", html)

    def test_reasoning_rich_text_hides_markdown_image_links(self) -> None:
        html = app_ui._reasoning_rich_text_html(
            "Before\n![China NTL 2010](outputs/ntl_preview/2010.png)\nAfter",
            accent="#5eead4",
        )

        self.assertIn("Before", html)
        self.assertIn("After", html)
        self.assertNotIn("![China NTL 2010]", html)
        self.assertNotIn("outputs/ntl_preview/2010.png", html)

    def test_code_assistant_plain_text_is_not_forced_into_code_block(self) -> None:
        self.assertFalse(app_ui._looks_like_code_assistant_code("Let me verify the output CSV:"))
        self.assertFalse(
            app_ui._looks_like_code_assistant_code(
                "Execution completed successfully.\n\n**Status:** Success"
            )
        )
        self.assertTrue(
            app_ui._looks_like_code_assistant_code(
                "import pandas as pd\n\ndef main():\n    return pd.DataFrame()"
            )
        )


if __name__ == "__main__":
    unittest.main()
