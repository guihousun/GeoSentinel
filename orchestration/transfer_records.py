"""System-authored records for native Deep Agents specialist transfers.

The native Deep Agents 0.7.5 ``task`` tool accepts only ``description`` and
``subagent_type`` and returns an ordinary tool response.  This module keeps
that harness unchanged: it derives immutable assignment/handoff records from
trusted task telemetry and persisted typed packages after execution.  It never
parses assignment prose or a specialist response as JSON.

Legacy ``ntl.assignment.v1`` / ``ntl.handoff.v1`` objects remain readable for
compatibility.  A validated legacy handoff/decision pair may be linked as
evidence, but model-authored legacy identity is never used to bind a native
task call.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from contracts.agent_packages import (
    AnalysisPackage,
    EngineerDecision,
    EventContext,
    HandoffEnvelope,
    ObservationPackage,
    PackageRef,
    canonical_json,
    contract_sha256,
)


ASSIGNMENT_RECORD_SCHEMA = "ntl.assignment-record.v2"
HANDOFF_RECORD_SCHEMA = "ntl.handoff-record.v2"

_SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_MAX_RECORD_BYTES = 10 * 1024 * 1024
_SOURCE_ROLE = "NTL_Engineer"
_TARGET_OUTPUTS = {
    "NTL_Data_Searcher": ("ObservationPackage", "save_observation_package"),
    "NTL_Analyst": ("AnalysisPackage", "save_analysis_package"),
    "NTL_Event_Tracker": ("EventContext", "save_event_context"),
}
_SPECIALIST_SAVE_TOOLS = frozenset(value[1] for value in _TARGET_OUTPUTS.values())
_PACKAGE_MODELS = {
    "ObservationPackage": ObservationPackage,
    "AnalysisPackage": AnalysisPackage,
    "EventContext": EventContext,
}

TRANSFER_TASK_IDENTITY_INVALID = "TRANSFER_TASK_IDENTITY_INVALID"
TRANSFER_TASK_ROLE_INVALID = "TRANSFER_TASK_ROLE_INVALID"
TRANSFER_TASK_CALL_INVALID = "TRANSFER_TASK_CALL_INVALID"
TRANSFER_PACKAGE_LINK_INVALID = "TRANSFER_PACKAGE_LINK_INVALID"
TRANSFER_RECORD_CONFLICT = "TRANSFER_RECORD_CONFLICT"
TRANSFER_RECORD_IO_ERROR = "TRANSFER_RECORD_IO_ERROR"


def _safe_record_path(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if (
        not raw.startswith("outputs/runs/")
        or "\x00" in raw
        or PureWindowsPath(raw).is_absolute()
    ):
        raise ValueError("record path must stay beneath outputs/runs")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("record path cannot be absolute or contain traversal segments")
    return raw


def _utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an offset")
    return parsed.astimezone(timezone.utc)


class _StrictRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, str_strip_whitespace=True)


class RecordFileRef(_StrictRecord):
    record_type: Literal[
        "assignment_record_v2",
        "handoff_record_v2",
        "handoff_envelope_v1",
        "engineer_decision_v1",
    ]
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)
    bytes: int = Field(ge=0)

    @field_validator("path")
    @classmethod
    def _path_is_safe(cls, value: str) -> str:
        return _safe_record_path(value)


class AssignmentRecordV2(_StrictRecord):
    """Immutable system view of one native ``task`` invocation."""

    schema_version: Literal["ntl.assignment-record.v2"] = ASSIGNMENT_RECORD_SCHEMA
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_tool_call_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    source_role: Literal["NTL_Engineer"] = _SOURCE_ROLE
    target_role: Literal["NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"]
    started_at_utc: datetime
    description_sha256: str = Field(pattern=_SHA256_PATTERN)
    description_bytes: int = Field(ge=0)

    @field_validator("started_at_utc")
    @classmethod
    def _time_is_utc(cls, value: datetime) -> datetime:
        return _utc_datetime(value)


class HandoffRecordV2(_StrictRecord):
    """System view of the specialist-to-Engineer return; content stays opaque."""

    schema_version: Literal["ntl.handoff-record.v2"] = HANDOFF_RECORD_SCHEMA
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_tool_call_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    producer_role: Literal["NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"]
    recipient_role: Literal["NTL_Engineer"] = _SOURCE_ROLE
    started_at_utc: datetime
    ended_at_utc: datetime | None = None
    outcome: Literal["succeeded", "error", "in_flight"]
    assignment_record: RecordFileRef
    response_observed: bool
    response_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    response_bytes: int | None = Field(default=None, ge=0)
    package_association: Literal["none", "linked", "ambiguous"] = "none"
    package_candidate_count: int = Field(default=0, ge=0)
    package: PackageRef | None = None
    legacy_handoff: RecordFileRef | None = None
    legacy_decision: RecordFileRef | None = None
    error_code: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator("started_at_utc", "ended_at_utc")
    @classmethod
    def _times_are_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc_datetime(value)

    @model_validator(mode="after")
    def _call_and_links_are_consistent(self) -> "HandoffRecordV2":
        if self.ended_at_utc is not None and self.ended_at_utc < self.started_at_utc:
            raise ValueError("handoff end precedes task start")
        if self.outcome == "succeeded":
            if (
                self.ended_at_utc is None
                or not self.response_observed
                or self.response_sha256 is None
                or self.response_bytes is None
            ):
                raise ValueError("successful task calls require an observed hashed response")
        elif self.outcome == "error":
            if self.ended_at_utc is None or self.error_code is None:
                raise ValueError("errored task calls require end time and error code")
        elif self.ended_at_utc is not None:
            raise ValueError("in-flight task calls cannot have an end time")
        if self.response_observed != (self.response_sha256 is not None):
            raise ValueError("response_observed and response_sha256 disagree")
        if (self.response_sha256 is None) != (self.response_bytes is None):
            raise ValueError("response hash and byte count must appear together")
        if self.assignment_record.record_type != "assignment_record_v2":
            raise ValueError("handoff must reference an assignment-record.v2")
        if self.package_association == "linked":
            if self.package is None or self.package_candidate_count < 1:
                raise ValueError("linked package association requires a package")
        elif self.package is not None:
            raise ValueError("unlinked package association cannot carry a package")
        if self.package_association == "none" and self.package_candidate_count != 0:
            raise ValueError("none package association requires zero candidates")
        if self.package_association == "ambiguous" and self.package_candidate_count < 2:
            raise ValueError("ambiguous package association requires multiple candidates")
        if (self.legacy_handoff is None) != (self.legacy_decision is None):
            raise ValueError("legacy handoff and decision references must be paired")
        if self.legacy_handoff is not None:
            if self.package is None:
                raise ValueError("legacy links require a linked package")
            if self.legacy_handoff.record_type != "handoff_envelope_v1":
                raise ValueError("legacy_handoff has the wrong record type")
            assert self.legacy_decision is not None
            if self.legacy_decision.record_type != "engineer_decision_v1":
                raise ValueError("legacy_decision has the wrong record type")
        if self.package is not None:
            expected_type = _TARGET_OUTPUTS[self.producer_role][0]
            if self.package.artifact_type != expected_type:
                raise ValueError("linked package type does not match target role")
        return self


@dataclass(frozen=True)
class _PackageCandidate:
    value: EventContext | ObservationPackage | AnalysisPackage
    reference: PackageRef


@dataclass(frozen=True)
class _LegacyPair:
    handoff: HandoffEnvelope
    decision: EngineerDecision
    handoff_ref: RecordFileRef
    decision_ref: RecordFileRef


class _RecordConflictError(RuntimeError):
    pass


def _linklike(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    attributes = getattr(info, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return path.is_symlink() or bool(reparse and attributes & reparse)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _stable_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8", errors="replace")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record_path(path: Path, *, outputs_root: Path) -> str:
    resolved = path.resolve(strict=True)
    if _linklike(path) or not _inside(resolved, outputs_root):
        raise ValueError("record path escapes outputs root")
    return "outputs/" + path.relative_to(outputs_root).as_posix()


def _file_ref(
    path: Path,
    *,
    outputs_root: Path,
    record_type: Literal[
        "assignment_record_v2",
        "handoff_record_v2",
        "handoff_envelope_v1",
        "engineer_decision_v1",
    ],
) -> RecordFileRef:
    return RecordFileRef(
        record_type=record_type,
        path=_record_path(path, outputs_root=outputs_root),
        sha256=_file_sha256(path),
        bytes=path.stat().st_size,
    )


def _read_canonical_model(path: Path, model: type[BaseModel], *, outputs_root: Path) -> BaseModel:
    resolved = path.resolve(strict=True)
    if _linklike(path) or not _inside(resolved, outputs_root):
        raise ValueError("unsafe internal record path")
    raw_bytes = path.read_bytes()
    if len(raw_bytes) > _MAX_RECORD_BYTES:
        raise ValueError("internal record exceeds size limit")
    raw = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("internal record must be a JSON object")
    value = model.model_validate(raw)
    if raw_bytes != canonical_json(value).encode("utf-8"):
        raise ValueError("internal record is not canonical JSON")
    return value


def _scan_packages(outputs_root: Path, run_id: str, task_id: str) -> list[_PackageCandidate]:
    directory = outputs_root / "runs" / run_id / "contracts"
    if not directory.is_dir() or _linklike(directory):
        return []
    packages: list[_PackageCandidate] = []
    for path in sorted(directory.glob("*.json"), key=lambda item: item.name.casefold()):
        if not path.is_file() or _linklike(path):
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            artifact_type = str(raw.get("artifact_type") or "") if isinstance(raw, dict) else ""
            model = _PACKAGE_MODELS.get(artifact_type)
            if model is None:
                continue
            value = _read_canonical_model(path, model, outputs_root=outputs_root)
            if value.run_id != run_id or value.task_id != task_id:
                continue
            packages.append(
                _PackageCandidate(
                    value=value,  # type: ignore[arg-type]
                    reference=PackageRef(
                        artifact_id=value.artifact_id,
                        artifact_type=value.artifact_type,
                        path=_record_path(path, outputs_root=outputs_root),
                        sha256=_file_sha256(path),
                    ),
                )
            )
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
            continue
    return packages


def _scan_legacy_pairs(outputs_root: Path, run_id: str, task_id: str) -> list[_LegacyPair]:
    run_root = outputs_root / "runs" / run_id
    handoffs: list[tuple[HandoffEnvelope, Path]] = []
    decisions: list[tuple[EngineerDecision, Path]] = []
    for path in sorted((run_root / "handoffs").glob("*.json")):
        try:
            value = _read_canonical_model(path, HandoffEnvelope, outputs_root=outputs_root)
            assert isinstance(value, HandoffEnvelope)
            if value.run_id == run_id and value.task_id == task_id:
                handoffs.append((value, path))
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
            continue
    for path in sorted((run_root / "decisions").glob("*.json")):
        try:
            value = _read_canonical_model(path, EngineerDecision, outputs_root=outputs_root)
            assert isinstance(value, EngineerDecision)
            if value.run_id == run_id and value.task_id == task_id:
                decisions.append((value, path))
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
            continue

    pairs: list[_LegacyPair] = []
    for handoff, handoff_path in handoffs:
        for decision, decision_path in decisions:
            if (
                decision.assignment_id != handoff.assignment_id
                or decision.handoff_id != handoff.handoff_id
                or decision.handoff_sha256 != contract_sha256(handoff)
                or decision.package != handoff.package
            ):
                continue
            pairs.append(
                _LegacyPair(
                    handoff=handoff,
                    decision=decision,
                    handoff_ref=_file_ref(
                        handoff_path,
                        outputs_root=outputs_root,
                        record_type="handoff_envelope_v1",
                    ),
                    decision_ref=_file_ref(
                        decision_path,
                        outputs_root=outputs_root,
                        record_type="engineer_decision_v1",
                    ),
                )
            )
    return pairs


def _ensure_record_directory(path: Path, *, outputs_root: Path) -> None:
    if not _inside(path.resolve(strict=False), outputs_root):
        raise ValueError("record directory escapes outputs root")
    current = outputs_root
    for part in path.relative_to(outputs_root).parts:
        current = current / part
        current.mkdir(exist_ok=True)
        if not current.is_dir() or _linklike(current):
            raise ValueError("record directory is not a safe local directory")


def _append_only_record(
    path: Path,
    value: BaseModel,
    *,
    outputs_root: Path,
    record_type: Literal[
        "assignment_record_v2",
        "handoff_record_v2",
        "handoff_envelope_v1",
        "engineer_decision_v1",
    ],
) -> RecordFileRef:
    _ensure_record_directory(path.parent, outputs_root=outputs_root)
    encoded = canonical_json(value).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        if _linklike(path) or not path.is_file() or path.read_bytes() != encoded:
            raise _RecordConflictError(path.name)
    else:
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                path.unlink(missing_ok=True)
            finally:
                raise
    return _file_ref(path, outputs_root=outputs_root, record_type=record_type)


def _native_task_rows(tool_trace: Any) -> list[Mapping[str, Any]]:
    if not isinstance(tool_trace, list):
        return []
    rows = [row for row in tool_trace if isinstance(row, Mapping) and row.get("tool_name") == "task"]
    # Older fixtures and pre-v2 imported traces do not carry the native
    # telemetry hashes.  Leave them readable, but do not fabricate v2 records.
    return [
        row
        for row in rows
        if "sequence" in row and "arguments_sha256" in row
    ]


def _package_ref_key(reference: PackageRef) -> tuple[str, str, str, str]:
    raw_path = reference.path.replace("\\", "/")
    if raw_path.startswith("/data/processed/"):
        logical_path = raw_path[len("/data/processed/") :]
    elif raw_path.startswith("outputs/"):
        logical_path = raw_path[len("outputs/") :]
    else:
        logical_path = raw_path.lstrip("/")
    return (
        reference.artifact_type,
        reference.artifact_id,
        logical_path,
        reference.sha256,
    )


def _same_package_ref(left: PackageRef, right: PackageRef) -> bool:
    return _package_ref_key(left) == _package_ref_key(right)


def _task_identity(
    row: Mapping[str, Any],
    *,
    expected_run_id: str,
    expected_task_id: str,
) -> tuple[str, str, str, str, str, datetime, str, int]:
    call_id = str(row.get("tool_call_id") or "").strip()
    metadata = row.get("metadata")
    arguments = row.get("arguments")
    if not isinstance(metadata, Mapping):
        raise LookupError(TRANSFER_TASK_IDENTITY_INVALID)
    run_id = str(metadata.get("task_run_id") or "").strip()
    task_id = str(metadata.get("case_id") or "").strip()
    if run_id != expected_run_id or task_id != expected_task_id:
        raise LookupError(TRANSFER_TASK_IDENTITY_INVALID)
    source_role = str(metadata.get("lc_agent_name") or "").strip()
    if source_role != _SOURCE_ROLE or not isinstance(arguments, Mapping):
        raise LookupError(TRANSFER_TASK_ROLE_INVALID)
    target_role = str(arguments.get("subagent_type") or "").strip()
    if target_role not in _TARGET_OUTPUTS:
        raise LookupError(TRANSFER_TASK_ROLE_INVALID)
    description = arguments.get("description")
    if not isinstance(description, str):
        raise LookupError(TRANSFER_TASK_CALL_INVALID)
    if hashlib.sha256(_stable_json_bytes(dict(arguments))).hexdigest() != row.get("arguments_sha256"):
        raise LookupError(TRANSFER_TASK_CALL_INVALID)
    try:
        started = _utc_datetime(row.get("started_at"))
        # Pydantic performs the same portable-id validation before persistence.
        AssignmentRecordV2(
            run_id=run_id,
            task_id=task_id,
            task_tool_call_id=call_id,
            target_role=target_role,
            started_at_utc=started,
            description_sha256=hashlib.sha256(description.encode("utf-8")).hexdigest(),
            description_bytes=len(description.encode("utf-8")),
        )
    except (TypeError, ValueError) as exc:
        raise LookupError(TRANSFER_TASK_CALL_INVALID) from exc
    description_bytes = description.encode("utf-8")
    return (
        run_id,
        task_id,
        call_id,
        source_role,
        target_role,
        started,
        hashlib.sha256(description_bytes).hexdigest(),
        len(description_bytes),
    )


def _handoff_call_fields(row: Mapping[str, Any], started: datetime) -> dict[str, Any]:
    status = str(row.get("status") or "").strip()
    if status not in {"succeeded", "error", "in_flight"}:
        raise LookupError(TRANSFER_TASK_CALL_INVALID)
    ended = None if row.get("ended_at") in {None, ""} else _utc_datetime(row.get("ended_at"))
    response_observed = row.get("result_observed") is True
    response_sha256 = row.get("result_sha256")
    response_bytes = row.get("result_bytes")
    error = row.get("error")
    error_code = (
        str(error.get("code") or "").strip()
        if isinstance(error, Mapping) and error.get("code") not in {None, ""}
        else None
    )
    try:
        # Validate only system call mechanics.  The response body is not read.
        probe = HandoffRecordV2(
            run_id="probe",
            task_id="probe",
            task_tool_call_id="probe",
            producer_role="NTL_Data_Searcher",
            started_at_utc=started,
            ended_at_utc=ended,
            outcome=status,
            assignment_record=RecordFileRef(
                record_type="assignment_record_v2",
                path="outputs/runs/probe/assignment_records/probe.json",
                sha256="0" * 64,
                bytes=0,
            ),
            response_observed=response_observed,
            response_sha256=response_sha256,
            response_bytes=response_bytes,
            error_code=error_code,
        )
    except (TypeError, ValueError) as exc:
        raise LookupError(TRANSFER_TASK_CALL_INVALID) from exc
    return {
        "ended_at_utc": probe.ended_at_utc,
        "outcome": probe.outcome,
        "response_observed": probe.response_observed,
        "response_sha256": probe.response_sha256,
        "response_bytes": probe.response_bytes,
        "error_code": probe.error_code,
    }


def _package_for_task(
    task_row: Mapping[str, Any],
    *,
    tool_trace: Sequence[Mapping[str, Any]],
    packages: Sequence[_PackageCandidate],
    legacy_pairs: Sequence[_LegacyPair],
    run_id: str,
    task_id: str,
    call_id: str,
    target_role: str,
) -> tuple[str, list[_PackageCandidate], _PackageCandidate | None, _LegacyPair | None]:
    del task_row
    expected_type, expected_save_tool = _TARGET_OUTPUTS[target_role]
    descendants = [
        row
        for row in tool_trace
        if call_id in {str(value) for value in (row.get("ancestor_tool_call_ids") or [])}
    ]
    successful_saves = [
        row
        for row in descendants
        if row.get("tool_name") in _SPECIALIST_SAVE_TOOLS
        and row.get("status") == "succeeded"
        and row.get("result_observed") is True
        # ``save_*`` returns structured persistence failures rather than
        # raising. Telemetry marks those responses as ``error``; never infer a
        # package link from a same-role package merely because such a response
        # was observed in the specialist's descendant trace.
        and row.get("error") in (None, "")
        and isinstance(row.get("metadata"), Mapping)
        and row["metadata"].get("lc_agent_name") == target_role
    ]
    candidates: list[_PackageCandidate] = []
    for row in successful_saves:
        metadata = row.get("metadata")
        arguments = row.get("arguments")
        if (
            row.get("tool_name") != expected_save_tool
            or not isinstance(metadata, Mapping)
            or metadata.get("task_run_id") != run_id
            or metadata.get("case_id") != task_id
            or metadata.get("lc_agent_name") != target_role
            or not isinstance(arguments, Mapping)
            or not isinstance(arguments.get("contract"), Mapping)
        ):
            raise LookupError(TRANSFER_PACKAGE_LINK_INVALID)
        contract = arguments["contract"]
        # Typed save tools deliberately hide artifact_id/artifact_type from
        # model-facing specialist arguments; the runtime injects those fields
        # into the persisted package.  When identity is present (legacy or
        # explicit tool trace), match it exactly.  When it is absent, bind the
        # native save to the unique ready package produced by this role and
        # task.  Ambiguous or wrong explicit identity remains fail-closed.
        artifact_id = str(contract.get("artifact_id") or "").strip()
        declared_type = str(contract.get("artifact_type") or "").strip()
        if declared_type and declared_type != expected_type:
            raise LookupError(TRANSFER_PACKAGE_LINK_INVALID)
        if artifact_id:
            matches = [
                package
                for package in packages
                if package.value.artifact_id == artifact_id
                and package.value.artifact_type == expected_type
                and str(package.value.producer) == target_role
                and str(package.value.status) in {"ready", "ContractStatus.READY"}
            ]
        else:
            matches = [
                package
                for package in packages
                if package.value.artifact_type == expected_type
                and str(package.value.producer) == target_role
                and str(package.value.status) in {"ready", "ContractStatus.READY"}
            ]
        if len(matches) != 1:
            raise LookupError(TRANSFER_PACKAGE_LINK_INVALID)
        if all(existing.reference != matches[0].reference for existing in candidates):
            candidates.append(matches[0])

    accepted_pairs = [
        pair
        for pair in legacy_pairs
        if pair.decision.decision == "accepted"
        and pair.handoff.producer == target_role
        and pair.handoff.package is not None
        and any(_same_package_ref(pair.handoff.package, candidate.reference) for candidate in candidates)
    ]
    selected: _PackageCandidate | None = None
    legacy: _LegacyPair | None = None
    if len(accepted_pairs) == 1:
        legacy = accepted_pairs[0]
        selected = next(
            candidate
            for candidate in candidates
            if _same_package_ref(candidate.reference, legacy.handoff.package)
        )
    elif len(candidates) == 1:
        selected = candidates[0]
        matching_pairs = [
            pair
            for pair in legacy_pairs
            if pair.handoff.producer == target_role
            and pair.handoff.package is not None
            and _same_package_ref(pair.handoff.package, selected.reference)
        ]
        if len(matching_pairs) == 1:
            legacy = matching_pairs[0]

    association = "linked" if selected is not None else ("ambiguous" if len(candidates) > 1 else "none")
    return association, candidates, selected, legacy


def reconcile_transfer_records(
    outputs_dir: str | Path,
    *,
    tool_trace: Any,
    expected_run_id: str,
    expected_task_id: str,
) -> dict[str, Any]:
    """Append immutable v2 records for native task calls and return stable issues.

    Only telemetry identity/role/call mechanics and an optional package link are
    hard-validated.  Assignment and response text are never interpreted; only
    their system-captured hashes and byte counts enter these records.
    """

    outputs_root = Path(outputs_dir).resolve(strict=False)
    result: dict[str, Any] = {
        "assignment_records": [],
        "handoff_records": [],
        "issues": [],
    }
    task_rows = _native_task_rows(tool_trace)
    if not task_rows:
        return result
    if not re.fullmatch(_SAFE_ID_PATTERN, expected_run_id) or not re.fullmatch(
        _SAFE_ID_PATTERN, expected_task_id
    ):
        result["issues"].append(TRANSFER_TASK_IDENTITY_INVALID)
        return result
    trace_rows = [row for row in tool_trace if isinstance(row, Mapping)]
    try:
        packages = _scan_packages(outputs_root, expected_run_id, expected_task_id)
        legacy_pairs = _scan_legacy_pairs(outputs_root, expected_run_id, expected_task_id)
    except (OSError, ValueError):
        result["issues"].append(TRANSFER_RECORD_IO_ERROR)
        return result

    seen_call_ids: set[str] = set()
    associated_package_keys: set[tuple[str, str, str, str]] = set()
    for row in task_rows:
        try:
            (
                run_id,
                task_id,
                call_id,
                source_role,
                target_role,
                started,
                description_sha256,
                description_bytes,
            ) = _task_identity(
                row,
                expected_run_id=expected_run_id,
                expected_task_id=expected_task_id,
            )
            if call_id in seen_call_ids:
                raise LookupError(TRANSFER_TASK_CALL_INVALID)
            seen_call_ids.add(call_id)
            call_fields = _handoff_call_fields(row, started)
            association, candidates, selected, legacy = _package_for_task(
                row,
                tool_trace=trace_rows,
                packages=packages,
                legacy_pairs=legacy_pairs,
                run_id=run_id,
                task_id=task_id,
                call_id=call_id,
                target_role=target_role,
            )
            assignment = AssignmentRecordV2(
                run_id=run_id,
                task_id=task_id,
                task_tool_call_id=call_id,
                source_role=source_role,
                target_role=target_role,
                started_at_utc=started,
                description_sha256=description_sha256,
                description_bytes=description_bytes,
            )
            run_root = outputs_root / "runs" / run_id
            assignment_path = (
                run_root / "assignment_records" / f"assignment_record__{call_id}.json"
            )
            assignment_ref = _append_only_record(
                assignment_path,
                assignment,
                outputs_root=outputs_root,
                record_type="assignment_record_v2",
            )
            if selected is not None:
                associated_package_keys.add(_package_ref_key(selected.reference))
            handoff = HandoffRecordV2(
                run_id=run_id,
                task_id=task_id,
                task_tool_call_id=call_id,
                producer_role=target_role,
                started_at_utc=started,
                assignment_record=assignment_ref,
                package_association=association,
                package_candidate_count=len(candidates),
                package=selected.reference if selected is not None else None,
                legacy_handoff=legacy.handoff_ref if legacy is not None else None,
                legacy_decision=legacy.decision_ref if legacy is not None else None,
                **call_fields,
            )
            handoff_path = run_root / "handoff_records" / f"handoff_record__{call_id}.json"
            handoff_ref = _append_only_record(
                handoff_path,
                handoff,
                outputs_root=outputs_root,
                record_type="handoff_record_v2",
            )
            result["assignment_records"].append(assignment_ref.model_dump(mode="json"))
            result["handoff_records"].append(handoff_ref.model_dump(mode="json"))
        except LookupError as exc:
            result["issues"].append(str(exc) or TRANSFER_TASK_CALL_INVALID)
        except _RecordConflictError:
            result["issues"].append(TRANSFER_RECORD_CONFLICT)
        except (OSError, TypeError, ValueError):
            result["issues"].append(TRANSFER_RECORD_IO_ERROR)

    # An accepted legacy package is allowed to remain unlinked only when no
    # native task row was eligible (handled above).  With native task rows, a
    # package accepted by the Engineer must be descended from exactly one task
    # call before the v2 record may claim it.
    for pair in legacy_pairs:
        if pair.decision.decision != "accepted" or pair.handoff.package is None:
            continue
        key = _package_ref_key(pair.handoff.package)
        if key not in associated_package_keys:
            result["issues"].append(TRANSFER_PACKAGE_LINK_INVALID)

    result["issues"] = list(dict.fromkeys(result["issues"]))
    return result


__all__ = [
    "ASSIGNMENT_RECORD_SCHEMA",
    "HANDOFF_RECORD_SCHEMA",
    "AssignmentRecordV2",
    "HandoffRecordV2",
    "RecordFileRef",
    "TRANSFER_PACKAGE_LINK_INVALID",
    "TRANSFER_RECORD_CONFLICT",
    "TRANSFER_RECORD_IO_ERROR",
    "TRANSFER_TASK_CALL_INVALID",
    "TRANSFER_TASK_IDENTITY_INVALID",
    "TRANSFER_TASK_ROLE_INVALID",
    "reconcile_transfer_records",
]

