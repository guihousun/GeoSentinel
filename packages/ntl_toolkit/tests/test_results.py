import importlib
import warnings
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

import ntl_toolkit.schemas.jobs as jobs_module
import ntl_toolkit.schemas.results as results_module
from ntl_toolkit.schemas import JobRecord, OutputArtifact, ToolError, ToolResult


def test_succeeded_result_json_dump_preserves_schema_outputs_and_metrics() -> None:
    outputs = [
        OutputArtifact(
            path="outputs/example.tif",
            media_type="image/tiff",
        )
    ]
    metrics = {"width": 256, "height": 128}

    result = ToolResult.succeeded(
        tool="render_map",
        summary="Rendered nighttime light raster.",
        outputs=outputs,
        metrics=metrics,
    )

    payload = result.model_dump(mode="json")

    assert payload["schema"] == "ntl.tool.result.v1"
    assert payload["status"] == "succeeded"
    assert payload["tool"] == "render_map"
    assert payload["summary"] == "Rendered nighttime light raster."
    assert payload["error"] is None
    assert payload["outputs"] == [
        {
            "path": "outputs/example.tif",
            "media_type": "image/tiff",
            "role": "primary",
        }
    ]
    assert payload["metrics"] == {"width": 256, "height": 128}


def test_failed_result_preserves_actionable_error_and_defaults_summary() -> None:
    error = ToolError(
        code="missing_input",
        message="Input raster is required.",
        details={"field": "input_raster", "retryable": False},
        suggestion="Upload a raster and retry.",
    )

    result = ToolResult.failed(
        tool="render_map",
        error=error,
    )

    payload = result.model_dump(mode="json")

    assert payload["status"] == "failed"
    assert payload["summary"] == "Input raster is required."
    assert payload["error"] == {
        "code": "missing_input",
        "message": "Input raster is required.",
        "details": {"field": "input_raster", "retryable": False},
        "suggestion": "Upload a raster and retry.",
    }


def test_failed_result_preserves_explicit_empty_summary() -> None:
    error = ToolError(
        code="missing_input",
        message="Input raster is required.",
    )

    result = ToolResult.failed(
        tool="render_map",
        error=error,
        summary="",
    )

    assert result.summary == ""


def test_job_record_json_serialization_and_defaults() -> None:
    created_at = datetime(2026, 6, 28, 10, 30, tzinfo=timezone.utc)
    updated_at = datetime(2026, 6, 28, 10, 45, tzinfo=timezone.utc)

    job = JobRecord(
        job_id="job-123",
        tool="render_map",
        status="queued",
        created_at=created_at,
        updated_at=updated_at,
    )

    payload = job.model_dump(mode="json")

    assert payload == {
        "schema": "ntl.job.v1",
        "job_id": "job-123",
        "tool": "render_map",
        "status": "queued",
        "created_at": "2026-06-28T10:30:00Z",
        "updated_at": "2026-06-28T10:45:00Z",
        "request": {},
        "outputs": [],
    }


def test_mutable_defaults_are_not_shared_between_instances() -> None:
    first_error = ToolError(code="first", message="First error")
    second_error = ToolError(code="second", message="Second error")
    first_result = ToolResult.succeeded(tool="tool_a", summary="ok")
    second_result = ToolResult.succeeded(tool="tool_b", summary="ok")
    first_job = JobRecord(
        job_id="job-1",
        tool="tool_a",
        status="queued",
        created_at=datetime(2026, 6, 28, tzinfo=timezone.utc),
        updated_at=datetime(2026, 6, 28, tzinfo=timezone.utc),
    )
    second_job = JobRecord(
        job_id="job-2",
        tool="tool_b",
        status="queued",
        created_at=datetime(2026, 6, 28, tzinfo=timezone.utc),
        updated_at=datetime(2026, 6, 28, tzinfo=timezone.utc),
    )

    first_error.details["field"] = "alpha"
    first_result.outputs.append(
        OutputArtifact(path="outputs/first.tif", media_type="image/tiff")
    )
    first_result.metrics["width"] = 512
    first_result.warnings.append("check extent")
    first_job.request["region"] = "tehran"
    first_job.outputs.append("outputs/first.tif")

    assert second_error.details == {}
    assert second_result.outputs == []
    assert second_result.metrics == {}
    assert second_result.warnings == []
    assert second_job.request == {}
    assert second_job.outputs == []


def test_succeeded_builder_defensively_copies_mutable_inputs() -> None:
    outputs = [OutputArtifact(path="outputs/example.tif", media_type="image/tiff")]
    metrics = {"width": 128}
    warnings = ["initial"]

    result = ToolResult.succeeded(
        tool="render_map",
        summary="done",
        outputs=outputs,
        metrics=metrics,
        warnings=warnings,
    )

    outputs.append(OutputArtifact(path="outputs/second.tif", media_type="image/tiff"))
    metrics["height"] = 256
    warnings.append("late warning")

    payload = result.model_dump(mode="json")

    assert payload["outputs"] == [
        {
            "path": "outputs/example.tif",
            "media_type": "image/tiff",
            "role": "primary",
        }
    ]
    assert payload["metrics"] == {"width": 128}
    assert payload["warnings"] == ["initial"]


def test_schema_modules_reload_without_schema_shadow_warnings() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(results_module)
        importlib.reload(jobs_module)

    shadow_warnings = [
        str(warning.message)
        for warning in caught
        if 'Field name "schema"' in str(warning.message)
    ]

    assert shadow_warnings == []


def test_tool_result_schema_alias_population_dump_and_literal_enforcement() -> None:
    via_alias = ToolResult(
        schema="ntl.tool.result.v1",
        status="succeeded",
        tool="render_map",
        summary="done",
    )
    via_name = ToolResult(
        schema_="ntl.tool.result.v1",
        status="succeeded",
        tool="render_map",
        summary="done",
    )

    assert via_alias.schema_ == "ntl.tool.result.v1"
    assert via_name.schema_ == "ntl.tool.result.v1"
    assert via_alias.model_dump()["schema"] == "ntl.tool.result.v1"
    assert via_alias.model_dump(mode="json")["schema"] == "ntl.tool.result.v1"
    assert "schema_" not in via_alias.model_dump()

    with pytest.raises(ValidationError):
        ToolResult(
            schema="wrong",
            status="succeeded",
            tool="render_map",
            summary="done",
        )


def test_job_record_schema_alias_population_dump_and_literal_enforcement() -> None:
    created_at = datetime(2026, 6, 28, 10, 30, tzinfo=timezone.utc)
    updated_at = datetime(2026, 6, 28, 10, 45, tzinfo=timezone.utc)

    via_alias = JobRecord(
        schema="ntl.job.v1",
        job_id="job-123",
        tool="render_map",
        status="queued",
        created_at=created_at,
        updated_at=updated_at,
    )
    via_name = JobRecord(
        schema_="ntl.job.v1",
        job_id="job-124",
        tool="render_map",
        status="queued",
        created_at=created_at,
        updated_at=updated_at,
    )

    assert via_alias.schema_ == "ntl.job.v1"
    assert via_name.schema_ == "ntl.job.v1"
    assert via_alias.model_dump()["schema"] == "ntl.job.v1"
    assert via_alias.model_dump(mode="json")["schema"] == "ntl.job.v1"
    assert "schema_" not in via_alias.model_dump()

    with pytest.raises(ValidationError):
        JobRecord(
            schema_="wrong",
            job_id="job-125",
            tool="render_map",
            status="queued",
            created_at=created_at,
            updated_at=updated_at,
        )
