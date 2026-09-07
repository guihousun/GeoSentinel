"""Safe persistence and verification for hierarchical-agent contracts.

Every managed record is written beneath the current thread's
``outputs/runs/<run_id>/`` tree.  The canonical JSON bytes on disk are exactly
the bytes covered by the returned SHA-256, which makes a ``PackageRef``
independently verifiable by the Engineer and the post-run harness.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, TypeVar

from pydantic import BaseModel

from contracts.agent_packages import (
    AnalysisPackage,
    ArtifactRecord,
    ContractEnvelope,
    EngineerDecision,
    EventContext,
    EvidenceReport,
    HandoffDecision,
    HandoffEnvelope,
    ObservationPackage,
    PackageRef,
    TaskPlan,
    canonical_json,
    contract_sha256,
)
from storage_manager import storage_manager


ContractModel = TaskPlan | EventContext | ObservationPackage | AnalysisPackage | EvidenceReport
TContract = TypeVar("TContract", bound=ContractEnvelope)

CONTRACT_MODELS: dict[str, type[ContractEnvelope]] = {
    "TaskPlan": TaskPlan,
    "EventContext": EventContext,
    "ObservationPackage": ObservationPackage,
    "AnalysisPackage": AnalysisPackage,
    "EvidenceReport": EvidenceReport,
}

_CONTRACT_FILENAMES = {
    "TaskPlan": "task_plan",
    "EventContext": "event_context",
    "ObservationPackage": "observation_package",
    "AnalysisPackage": "analysis_package",
    "EvidenceReport": "evidence_report",
}

_EVALUATOR_ONLY_KEYS = frozenset(
    {
        "gold",
        "gold_answer",
        "gold_answers",
        "gold_contract",
        "judge_packet",
        "judge_prompt",
        "evaluator_prompt",
        "reference_answer",
        "expected_score",
    }
)


class ContractIOError(ValueError):
    """Raised when a contract fails scope, integrity, or persistence checks."""


def _coerce_mapping(payload: dict[str, Any] | str | BaseModel) -> dict[str, Any]:
    if isinstance(payload, BaseModel):
        return payload.model_dump(mode="json", exclude_none=False)
    if isinstance(payload, str):
        try:
            value = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ContractIOError(f"invalid contract JSON: {exc}") from exc
    else:
        value = payload
    if not isinstance(value, dict):
        raise ContractIOError("contract payload must be a JSON object")
    return value


def _assert_no_evaluator_fields(value: Any, *, location: str = "$") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key).strip().casefold().replace("-", "_").replace(" ", "_")
            if normalized in _EVALUATOR_ONLY_KEYS:
                raise ContractIOError(f"evaluator-only field is forbidden inside runtime contracts: {location}.{key}")
            _assert_no_evaluator_fields(nested, location=f"{location}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_no_evaluator_fields(nested, location=f"{location}[{index}]")


def validate_contract_payload(
    payload: dict[str, Any] | str | BaseModel,
    *,
    expected_artifact_type: str | None = None,
) -> ContractModel:
    """Validate one of the five package types without reading or writing files."""

    raw = _coerce_mapping(payload)
    _assert_no_evaluator_fields(raw)
    artifact_type = str(raw.get("artifact_type") or expected_artifact_type or "").strip()
    if expected_artifact_type and artifact_type != expected_artifact_type:
        raise ContractIOError(
            f"expected artifact_type={expected_artifact_type}, received artifact_type={artifact_type or '<missing>'}"
        )
    model = CONTRACT_MODELS.get(artifact_type)
    if model is None:
        raise ContractIOError(f"unsupported or missing artifact_type: {artifact_type or '<missing>'}")
    return model.model_validate(raw)


def _normalize_output_relative_path(path_value: str) -> PurePosixPath:
    raw = str(path_value or "").strip().replace("\\", "/")
    if not raw or "\x00" in raw or PureWindowsPath(raw).is_absolute():
        raise ContractIOError("record path must be a workspace output path")
    if raw.startswith("/data/processed/"):
        raw = raw[len("/data/processed/") :]
    elif raw.startswith("/outputs/"):
        raw = raw[len("/outputs/") :]
    elif raw.startswith("outputs/"):
        raw = raw[len("outputs/") :]
    rel = PurePosixPath(raw)
    if rel.is_absolute() or any(part in {"", ".", ".."} for part in rel.parts):
        raise ContractIOError("record path cannot be absolute or contain traversal segments")
    return rel


def _resolve_output_path(
    path_value: str,
    *,
    thread_id: str,
    require_run_id: str | None = None,
) -> Path:
    rel = _normalize_output_relative_path(path_value)
    if require_run_id is not None:
        if len(rel.parts) < 2 or rel.parts[0] != "runs" or rel.parts[1] != require_run_id:
            raise ContractIOError(f"record path must stay inside outputs/runs/{require_run_id}/")
    return storage_manager.resolve_workspace_relative_path(
        rel.as_posix(),
        thread_id=thread_id,
        default_root="outputs",
        create_parent=False,
        allow_memory=False,
        allowed_roots=("outputs",),
    )


def _resolve_workspace_artifact_path(path_value: str, *, thread_id: str) -> Path:
    """Resolve a declared immutable artifact in this thread's inputs or outputs.

    ObservationPackage artifacts may legitimately be checksum-bound staged
    inputs, while AnalysisPackage and EvidenceReport artifacts are normally
    outputs.  Contract records must name the root explicitly for inputs;
    unprefixed paths retain the historical output-relative meaning.
    """

    raw = str(path_value or "").strip().replace("\\", "/")
    if not raw or "\x00" in raw or PureWindowsPath(raw).is_absolute():
        raise ContractIOError("artifact path must be workspace-relative")
    if raw.startswith("/data/raw/"):
        root = "inputs"
        raw = raw[len("/data/raw/") :]
    elif raw.startswith("/inputs/"):
        root = "inputs"
        raw = raw[len("/inputs/") :]
    elif raw.startswith("inputs/"):
        root = "inputs"
        raw = raw[len("inputs/") :]
    elif raw.startswith("/data/processed/"):
        root = "outputs"
        raw = raw[len("/data/processed/") :]
    elif raw.startswith("/outputs/"):
        root = "outputs"
        raw = raw[len("/outputs/") :]
    elif raw.startswith("outputs/"):
        root = "outputs"
        raw = raw[len("outputs/") :]
    elif raw.startswith("/"):
        raise ContractIOError("artifact path uses an unsupported workspace root")
    else:
        root = "outputs"

    relative = PurePosixPath(raw)
    if (
        not raw
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ContractIOError("artifact path cannot be absolute or contain traversal segments")
    try:
        return storage_manager.resolve_workspace_relative_path(
            relative.as_posix(),
            thread_id=thread_id,
            default_root=root,
            create_parent=False,
            allow_memory=False,
            allowed_roots=(root,),
        )
    except (OSError, PermissionError, ValueError) as exc:
        raise ContractIOError("artifact path escaped its declared workspace root") from exc


def _virtual_output_path(path: Path, *, thread_id: str) -> str:
    outputs = (storage_manager.get_workspace(thread_id) / "outputs").resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(outputs)
    except ValueError as exc:
        raise ContractIOError("persisted record escaped current thread outputs") from exc
    return "/data/processed/" + relative.as_posix()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_create_canonical_json(
    relative_path: str,
    value: BaseModel,
    *,
    thread_id: str,
) -> Path:
    canonical = canonical_json(value)
    target = _resolve_output_path(relative_path, thread_id=thread_id)
    with storage_manager.workspace_write_lock(thread_id):
        if target.exists():
            if target.is_file() and target.read_bytes() == canonical.encode("utf-8"):
                return target
            raise FileExistsError(f"refusing to overwrite a different immutable contract: {relative_path}")
        return storage_manager.atomic_write_text(
            relative_path,
            canonical,
            thread_id=thread_id,
            default_root="outputs",
            allow_memory=False,
        )


def _persisted_result(value: BaseModel, path: Path, *, thread_id: str) -> dict[str, Any]:
    stat = path.stat()
    payload: dict[str, Any] = {
        "status": "success",
        "path": _virtual_output_path(path, thread_id=thread_id),
        "sha256": _file_sha256(path),
        "bytes": int(stat.st_size),
    }
    for field in ("schema_version", "artifact_type", "artifact_id", "run_id", "task_id"):
        if hasattr(value, field):
            payload[field] = getattr(value, field)
    return payload


def save_contract(
    payload: dict[str, Any] | str | BaseModel,
    *,
    thread_id: str,
    expected_artifact_type: str | None = None,
) -> dict[str, Any]:
    """Validate and immutably persist one of the five package types."""

    contract = validate_contract_payload(payload, expected_artifact_type=expected_artifact_type)
    stem = _CONTRACT_FILENAMES[contract.artifact_type]
    relative = f"runs/{contract.run_id}/contracts/{stem}__{contract.artifact_id}.json"
    path = _atomic_create_canonical_json(relative, contract, thread_id=thread_id)
    result = _persisted_result(contract, path, thread_id=thread_id)
    result["package_ref"] = PackageRef(
        artifact_id=contract.artifact_id,
        artifact_type=contract.artifact_type,
        path=result["path"],
        sha256=result["sha256"],
    ).model_dump(mode="json")
    return result


def load_contract(
    path_value: str,
    *,
    thread_id: str,
    expected_artifact_type: str | None = None,
    require_run_id: str | None = None,
) -> ContractModel:
    path = _resolve_output_path(path_value, thread_id=thread_id, require_run_id=require_run_id)
    if not path.is_file():
        raise ContractIOError(f"contract file does not exist: {path_value}")
    if path.stat().st_size > 10 * 1024 * 1024:
        raise ContractIOError("contract file exceeds the 10 MiB safety limit")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractIOError(f"failed to read contract JSON: {exc}") from exc
    return validate_contract_payload(raw, expected_artifact_type=expected_artifact_type)


def inspect_saved_contract(
    path_value: str,
    *,
    thread_id: str,
    expected_artifact_type: str | None = None,
) -> dict[str, Any]:
    """Validate a persisted package and return its identity and actual digest."""

    package = load_contract(
        path_value,
        thread_id=thread_id,
        expected_artifact_type=expected_artifact_type,
    )
    path = _resolve_output_path(path_value, thread_id=thread_id, require_run_id=package.run_id)
    actual_sha = _file_sha256(path)
    if contract_sha256(package) != actual_sha:
        raise ContractIOError("persisted package is not canonical JSON")
    result = _persisted_result(package, path, thread_id=thread_id)
    result["package_ref"] = PackageRef(
        artifact_id=package.artifact_id,
        artifact_type=package.artifact_type,
        path=result["path"],
        sha256=actual_sha,
    ).model_dump(mode="json")
    return result


def verify_package_ref(
    reference: PackageRef | dict[str, Any],
    *,
    thread_id: str,
    run_id: str,
    task_id: str | None = None,
) -> ContractModel:
    ref = reference if isinstance(reference, PackageRef) else PackageRef.model_validate(reference)
    path = _resolve_output_path(ref.path, thread_id=thread_id, require_run_id=run_id)
    if not path.is_file():
        raise ContractIOError(f"referenced package does not exist: {ref.path}")
    actual_sha = _file_sha256(path)
    if actual_sha != ref.sha256:
        raise ContractIOError(f"package checksum mismatch for {ref.path}")
    package = load_contract(
        ref.path,
        thread_id=thread_id,
        expected_artifact_type=ref.artifact_type,
        require_run_id=run_id,
    )
    if package.artifact_id != ref.artifact_id or package.run_id != run_id:
        raise ContractIOError("package reference identity does not match persisted contract")
    if task_id is not None and package.task_id != task_id:
        raise ContractIOError("package reference task_id does not match handoff")
    if contract_sha256(package) != actual_sha:
        raise ContractIOError("persisted package is not canonical JSON")
    return package


def verify_artifact_record(
    record: ArtifactRecord | dict[str, Any],
    *,
    thread_id: str,
) -> Path:
    artifact = record if isinstance(record, ArtifactRecord) else ArtifactRecord.model_validate(record)
    path = _resolve_workspace_artifact_path(artifact.path, thread_id=thread_id)
    if not path.is_file():
        raise ContractIOError(f"artifact does not exist: {artifact.path}")
    actual_bytes = path.stat().st_size
    if actual_bytes != artifact.bytes:
        raise ContractIOError(f"artifact byte-size mismatch: {artifact.path}")
    if _file_sha256(path) != artifact.sha256:
        raise ContractIOError(f"artifact checksum mismatch: {artifact.path}")
    return path


def _package_artifacts(package: ContractModel) -> list[ArtifactRecord]:
    if isinstance(package, ObservationPackage):
        return package.analysis_ready_artifacts
    if isinstance(package, AnalysisPackage):
        return package.artifacts
    if isinstance(package, EvidenceReport):
        return package.representative_artifacts
    return []


def persist_handoff_decision(
    handoff_payload: dict[str, Any] | str | HandoffEnvelope,
    decision_payload: dict[str, Any] | str | EngineerDecision,
    *,
    thread_id: str,
) -> dict[str, Any]:
    """Validate a handoff and atomically persist its Engineer decision records."""

    handoff_raw = _coerce_mapping(handoff_payload)
    decision_raw = dict(_coerce_mapping(decision_payload))
    _assert_no_evaluator_fields(handoff_raw)
    _assert_no_evaluator_fields(decision_raw)
    handoff = HandoffEnvelope.model_validate(handoff_raw)
    # Identity and digest are deterministic system facts, not values the LLM
    # should have to calculate.  Populate them when a decision draft omits
    # them; explicitly conflicting values are rejected by the checks below.
    decision_raw.setdefault("run_id", handoff.run_id)
    decision_raw.setdefault("task_id", handoff.task_id)
    decision_raw.setdefault("assignment_id", handoff.assignment_id)
    decision_raw.setdefault("handoff_id", handoff.handoff_id)
    decision_raw.setdefault("handoff_sha256", contract_sha256(handoff))
    decision_raw.setdefault(
        "package",
        handoff.package.model_dump(mode="json") if handoff.package is not None else None,
    )
    decision = EngineerDecision.model_validate(decision_raw)

    if (decision.run_id, decision.task_id, decision.assignment_id, decision.handoff_id) != (
        handoff.run_id,
        handoff.task_id,
        handoff.assignment_id,
        handoff.handoff_id,
    ):
        raise ContractIOError("EngineerDecision identity does not match HandoffEnvelope")
    handoff_hash = contract_sha256(handoff)
    if decision.handoff_sha256 != handoff_hash:
        raise ContractIOError("EngineerDecision handoff_sha256 does not match canonical handoff")
    if (decision.package is None) != (handoff.package is None):
        raise ContractIOError("EngineerDecision package must mirror the HandoffEnvelope package")
    if decision.package is not None and decision.package != handoff.package:
        raise ContractIOError("EngineerDecision package reference differs from HandoffEnvelope")

    package: ContractModel | None = None
    if handoff.package is not None:
        package = verify_package_ref(
            handoff.package,
            thread_id=thread_id,
            run_id=handoff.run_id,
            task_id=handoff.task_id,
        )
    if decision.decision == HandoffDecision.ACCEPTED:
        if handoff.status != "ready" or handoff.validation_verdict != "passed":
            raise ContractIOError("Engineer cannot accept a non-ready or failed-validation handoff")
        assert package is not None  # already guaranteed by the Pydantic contracts
        for artifact in _package_artifacts(package):
            verify_artifact_record(artifact, thread_id=thread_id)

    handoff_relative = f"runs/{handoff.run_id}/handoffs/handoff__{handoff.handoff_id}.json"
    decision_relative = f"runs/{decision.run_id}/decisions/engineer_decision__{decision.decision_id}.json"
    handoff_path = _atomic_create_canonical_json(handoff_relative, handoff, thread_id=thread_id)
    decision_path = _atomic_create_canonical_json(decision_relative, decision, thread_id=thread_id)
    return {
        "status": "success",
        "handoff": _persisted_result(handoff, handoff_path, thread_id=thread_id),
        "decision": _persisted_result(decision, decision_path, thread_id=thread_id),
    }


def persist_route_transition(
    *,
    run_id: str,
    task_id: str,
    target_status: str,
    reason: str,
    thread_id: str,
    max_revisions: int = 2,
    contract_refs: list[PackageRef | dict[str, Any]] | None = None,
    error_code: str | None = None,
) -> dict[str, Any]:
    """Load, advance, and atomically checkpoint one run's route ledger."""

    # Local import keeps the pure state-machine module independent from storage.
    from contracts.agent_packages import ErrorCode
    from orchestration.route_state import RouteState, RouteStateMachine, RouteStatus

    relative = f"runs/{run_id}/route/route_state.json"
    path = _resolve_output_path(relative, thread_id=thread_id, require_run_id=run_id)
    refs = [ref if isinstance(ref, PackageRef) else PackageRef.model_validate(ref) for ref in (contract_refs or [])]
    for ref in refs:
        verify_package_ref(ref, thread_id=thread_id, run_id=run_id, task_id=task_id)

    with storage_manager.workspace_write_lock(thread_id):
        if path.exists():
            try:
                state = RouteState.model_validate_json(path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise ContractIOError(f"existing route state is invalid: {exc}") from exc
            if state.run_id != run_id or state.task_id != task_id:
                raise ContractIOError("existing route state identity does not match requested run/task")
            if state.max_revisions != max_revisions:
                raise ContractIOError("max_revisions is frozen after route initialization")
        else:
            state = RouteState(run_id=run_id, task_id=task_id, max_revisions=max_revisions)

        machine = RouteStateMachine(state)
        target = RouteStatus(target_status)
        if target == RouteStatus.REVISION_REQUESTED:
            state = machine.request_revision_or_block(
                actor="NTL_Engineer",
                reason=reason,
                contract_refs=refs,
            )
        else:
            if RouteStatus(state.status) == RouteStatus.HANDOFF_VALIDATION and refs:
                for ref in refs:
                    machine.accept_package(ref, specialist=ref.artifact_type)
            state = machine.transition(
                target,
                actor="NTL_Engineer",
                reason=reason,
                contract_refs=refs,
                error_code=ErrorCode(error_code) if error_code else None,
            )
        canonical = canonical_json(state)
        path = storage_manager.atomic_write_text(
            relative,
            canonical,
            thread_id=thread_id,
            default_root="outputs",
            allow_memory=False,
        )
    result = _persisted_result(state, path, thread_id=thread_id)
    result.update(
        {
            "route_status": state.status,
            "revision_count": state.revision_count,
            "terminal": state.terminal,
            "event_count": len(state.events),
            "accepted_packages": {
                key: value.model_dump(mode="json") for key, value in state.accepted_packages.items()
            },
        }
    )
    return result

