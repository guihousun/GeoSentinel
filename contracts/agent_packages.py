"""Versioned contracts exchanged by the hierarchical NTL-GPT agents.

The models in this module are deliberately independent from the benchmark
runner and evaluator.  They describe evidence produced *inside* the tested
system and must never contain Gold answers or judge instructions.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import to_jsonable_python


CONTRACT_SCHEMA_VERSION = "ntl.contract.v1"
_SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class AgentRole(str, Enum):
    ENGINEER = "NTL_Engineer"
    DATA_SEARCHER = "NTL_Data_Searcher"
    ANALYST = "NTL_Analyst"
    EVENT_TRACKER = "NTL_Event_Tracker"


class ContractStatus(str, Enum):
    READY = "ready"
    NEEDS_INPUT = "needs_input"
    NEEDS_REVISION = "needs_revision"
    BLOCKED = "blocked"
    FAILED = "failed"


class HandoffDecision(str, Enum):
    ACCEPTED = "accepted"
    REVISION_REQUESTED = "revision_requested"
    REJECTED = "rejected"
    BLOCKED = "blocked"


class EvidenceReportStatus(str, Enum):
    COMPLETED = "completed"
    COMPLETED_WITH_LIMITATIONS = "completed_with_limitations"
    BLOCKED = "blocked"
    FAILED = "failed"


class ErrorCode(str, Enum):
    TASK_CONTRACT_UNRESOLVED = "TASK_CONTRACT_UNRESOLVED"
    EVENT_SOURCE_UNAVAILABLE = "EVENT_SOURCE_UNAVAILABLE"
    EVENT_SOURCE_CONFLICT = "EVENT_SOURCE_CONFLICT"
    OBSERVATION_NOT_AVAILABLE = "OBSERVATION_NOT_AVAILABLE"
    OBSERVATION_QUALITY_INSUFFICIENT = "OBSERVATION_QUALITY_INSUFFICIENT"
    DATA_CONTRACT_INVALID = "DATA_CONTRACT_INVALID"
    ANALYSIS_EXECUTION_FAILED = "ANALYSIS_EXECUTION_FAILED"
    ANALYSIS_VALIDATION_FAILED = "ANALYSIS_VALIDATION_FAILED"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    USER_DECISION_REQUIRED = "USER_DECISION_REQUIRED"
    UNSUPPORTED_TASK = "UNSUPPORTED_TASK"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_artifact_path(value: str) -> str:
    """Accept workspace input/output artifact paths; reject absolute/traversal paths."""

    raw = str(value or "").strip().replace("\\", "/")
    if not raw or "\x00" in raw or PureWindowsPath(raw).is_absolute():
        raise ValueError("artifact path must be a non-empty workspace artifact path")
    if raw.startswith("/data/processed/"):
        relative = raw[len("/data/processed/") :]
    elif raw.startswith("/data/raw/"):
        relative = raw[len("/data/raw/") :]
    elif raw.startswith("/outputs/"):
        relative = raw[len("/outputs/") :]
    elif raw.startswith("/inputs/"):
        relative = raw[len("/inputs/") :]
    elif raw.startswith("outputs/"):
        relative = raw[len("outputs/") :]
    elif raw.startswith("inputs/"):
        relative = raw[len("inputs/") :]
    elif raw.startswith("/"):
        raise ValueError("artifact path uses an unsupported workspace root")
    else:
        relative = raw
    path = PurePosixPath(relative)
    if not relative or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("artifact path cannot be absolute or contain traversal segments")
    return raw


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
        str_strip_whitespace=True,
    )


class ContractError(StrictModel):
    code: ErrorCode
    message: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)


class PackageRef(StrictModel):
    artifact_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    artifact_type: Literal[
        "TaskPlan", "EventContext", "ObservationPackage", "AnalysisPackage", "EvidenceReport"
    ]
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)

    @field_validator("path")
    @classmethod
    def _path_is_safe(cls, value: str) -> str:
        return _safe_artifact_path(value)


class ArtifactRecord(StrictModel):
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)
    bytes: int = Field(ge=0)
    media_type: str | None = None
    role: str = Field(default="artifact", min_length=1)

    @field_validator("path")
    @classmethod
    def _path_is_safe(cls, value: str) -> str:
        return _safe_artifact_path(value)


class LocalArtifactDraft(StrictModel):
    """Model-authored local artifact metadata before system identity binding.

    Checksums and byte counts are deliberately absent.  They are injected by
    the save layer from the runner's staged-input registry or from a safely
    resolved output file in the current workspace.
    """

    path: str = Field(min_length=1)
    media_type: str | None = None
    role: str = Field(default="artifact", min_length=1)

    @field_validator("path")
    @classmethod
    def _path_is_safe(cls, value: str) -> str:
        return _safe_artifact_path(value)


class ContractEnvelope(StrictModel):
    schema_version: Literal["ntl.contract.v1"] = CONTRACT_SCHEMA_VERSION
    artifact_type: str
    artifact_id: str = Field(
        default_factory=lambda: str(uuid4()), min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN
    )
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    producer: AgentRole
    created_at_utc: datetime = Field(default_factory=_utc_now)
    as_of_utc: datetime | None = None
    status: ContractStatus
    parent_artifact_ids: list[str] = Field(default_factory=list)
    skill_manifest: list[dict[str, Any]] = Field(default_factory=list)
    tool_manifest: list[dict[str, Any]] = Field(default_factory=list)
    artifact_manifest_path: str | None = None
    limitations: list[str] = Field(default_factory=list)
    error: ContractError | None = None

    @field_validator("created_at_utc", "as_of_utc")
    @classmethod
    def _timestamps_are_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("contract timestamps must include a UTC offset")
        return value.astimezone(timezone.utc)

    @field_validator("artifact_manifest_path")
    @classmethod
    def _manifest_path_is_safe(cls, value: str | None) -> str | None:
        return None if value is None else _safe_artifact_path(value)

    @field_validator("parent_artifact_ids")
    @classmethod
    def _parent_ids_are_unique(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("parent_artifact_ids must be unique")
        return value

    @model_validator(mode="after")
    def _validate_terminal_error(self) -> "ContractEnvelope":
        if self.status in {ContractStatus.BLOCKED, ContractStatus.FAILED} and self.error is None:
            raise ValueError("blocked/failed contracts require an error")
        if self.status == ContractStatus.READY and self.error is not None:
            raise ValueError("ready contracts cannot carry an error")
        return self


class TaskPlan(ContractEnvelope):
    artifact_type: Literal["TaskPlan"] = "TaskPlan"
    producer: Literal[AgentRole.ENGINEER] = AgentRole.ENGINEER
    original_request: str = Field(min_length=1)
    normalized_objective: str = Field(min_length=1)
    aoi_contract: dict[str, Any] = Field(default_factory=dict)
    time_contract: dict[str, Any] = Field(default_factory=dict)
    product_requirements: dict[str, Any] = Field(default_factory=dict)
    expected_outputs: list[dict[str, Any]] = Field(default_factory=list)
    scientific_boundaries: list[str] = Field(default_factory=list)
    event_context_required: bool = False
    observation_required: bool = False
    analysis_required: bool = False
    specialist_dag: list[dict[str, Any]] = Field(default_factory=list)
    skip_reasons: dict[str, str] = Field(default_factory=dict)
    acceptance_criteria: dict[str, list[str]] = Field(default_factory=dict)
    budget: dict[str, Any] = Field(default_factory=dict)
    risk_flags: list[str] = Field(default_factory=list)
    clarification_fields: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _ready_plan_has_no_open_clarification(self) -> "TaskPlan":
        if self.status == ContractStatus.READY and self.clarification_fields:
            raise ValueError("a ready TaskPlan cannot contain unresolved clarification_fields")
        return self


class EventContext(ContractEnvelope):
    artifact_type: Literal["EventContext"] = "EventContext"
    producer: Literal[AgentRole.EVENT_TRACKER] = AgentRole.EVENT_TRACKER
    retrieval_executed_at_utc: datetime
    source_policy: dict[str, Any] = Field(default_factory=dict)
    event: dict[str, Any] = Field(default_factory=dict)
    sources: list[dict[str, Any]] = Field(default_factory=list)
    deduplication: dict[str, Any] = Field(default_factory=dict)
    timeline: list[dict[str, Any]] = Field(default_factory=list)
    source_conflicts: list[dict[str, Any]] = Field(default_factory=list)
    source_coverage: dict[str, Any] = Field(default_factory=dict)
    candidate_windows: dict[str, Any] = Field(default_factory=dict)
    non_attribution_boundary: str = Field(min_length=1)

    @field_validator("retrieval_executed_at_utc")
    @classmethod
    def _retrieval_time_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("retrieval_executed_at_utc must include a UTC offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _event_has_as_of_and_ready_sources(self) -> "EventContext":
        if self.as_of_utc is None:
            raise ValueError("EventContext requires as_of_utc")
        if self.status == ContractStatus.READY and not self.sources:
            raise ValueError("a ready EventContext requires at least one source")
        return self


class ObservationPackage(ContractEnvelope):
    artifact_type: Literal["ObservationPackage"] = "ObservationPackage"
    producer: Literal[AgentRole.DATA_SEARCHER] = AgentRole.DATA_SEARCHER
    query_executed_at_utc: datetime
    product: dict[str, Any] = Field(default_factory=dict)
    availability: dict[str, Any] = Field(default_factory=dict)
    aoi: dict[str, Any] = Field(default_factory=dict)
    grid: dict[str, Any] = Field(default_factory=dict)
    qa_scaling_nodata: dict[str, Any] = Field(default_factory=dict)
    acquisition_route: dict[str, Any] = Field(default_factory=dict)
    preprocessing: list[dict[str, Any]] = Field(default_factory=list)
    source_records: list[dict[str, Any]] = Field(default_factory=list)
    validation: dict[str, Any] = Field(default_factory=dict)
    fallback: dict[str, Any] = Field(default_factory=dict)
    analysis_ready_artifacts: list[ArtifactRecord] = Field(default_factory=list)

    @field_validator("query_executed_at_utc")
    @classmethod
    def _query_time_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("query_executed_at_utc must include a UTC offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _ready_observation_has_product_and_validation(self) -> "ObservationPackage":
        if self.status == ContractStatus.READY and (not self.product or not self.validation):
            raise ValueError("a ready ObservationPackage requires product and validation records")
        return self


class RevisionRequest(StrictModel):
    schema_version: Literal["ntl.revision-request.v1"] = "ntl.revision-request.v1"
    revision_id: str = Field(
        default_factory=lambda: str(uuid4()), min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN
    )
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    source_agent: AgentRole
    target_agent: AgentRole
    related_package: PackageRef | None = None
    reason: str = Field(min_length=1)
    required_changes: list[str] = Field(min_length=1, max_length=8)
    semantic_change_required: bool = False
    revision_number: int = Field(ge=1)
    created_at_utc: datetime = Field(default_factory=_utc_now)

    @field_validator("created_at_utc")
    @classmethod
    def _created_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at_utc must include a UTC offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _revision_must_pass_through_engineer(self) -> "RevisionRequest":
        if self.source_agent == self.target_agent:
            raise ValueError("revision source_agent and target_agent must differ")
        if AgentRole.ENGINEER not in {self.source_agent, self.target_agent}:
            raise ValueError("specialists cannot send RevisionRequest directly to another specialist")
        return self


class AnalysisPackage(ContractEnvelope):
    artifact_type: Literal["AnalysisPackage"] = "AnalysisPackage"
    producer: Literal[AgentRole.ANALYST] = AgentRole.ANALYST
    linked_contracts: list[PackageRef] = Field(default_factory=list)
    scientific_question: str = Field(min_length=1)
    analysis_unit: str = Field(min_length=1)
    windows: dict[str, Any] = Field(default_factory=dict)
    method: dict[str, Any] = Field(default_factory=dict)
    execution_records: list[dict[str, Any]] = Field(default_factory=list)
    artifacts: list[ArtifactRecord] = Field(default_factory=list)
    validation: dict[str, Any] = Field(default_factory=dict)
    repair_history: list[dict[str, Any]] = Field(default_factory=list)
    findings: list[dict[str, Any]] = Field(default_factory=list)
    alternative_explanations: list[str] = Field(default_factory=list)
    revision_request: RevisionRequest | None = None

    @model_validator(mode="after")
    def _revision_status_matches_request(self) -> "AnalysisPackage":
        if self.status == ContractStatus.NEEDS_REVISION and self.revision_request is None:
            raise ValueError("needs_revision AnalysisPackage requires RevisionRequest")
        if self.status == ContractStatus.READY and self.revision_request is not None:
            raise ValueError("ready AnalysisPackage cannot carry RevisionRequest")
        return self


class EvidenceReport(ContractEnvelope):
    artifact_type: Literal["EvidenceReport"] = "EvidenceReport"
    producer: Literal[AgentRole.ENGINEER] = AgentRole.ENGINEER
    final_status: EvidenceReportStatus
    direct_answer: str = Field(min_length=1)
    findings: list[dict[str, Any]] = Field(default_factory=list)
    source_and_artifact_links: list[dict[str, Any]] = Field(default_factory=list)
    data_and_time_summary: dict[str, Any] = Field(default_factory=dict)
    method_summary: dict[str, Any] = Field(default_factory=dict)
    representative_artifacts: list[ArtifactRecord] = Field(default_factory=list)
    route_trace_path: str | None = None
    specialist_status: dict[str, str] = Field(default_factory=dict)
    validation_summary: dict[str, Any] = Field(default_factory=dict)
    unresolved_items: list[str] = Field(default_factory=list)
    uncertainty: list[str] = Field(default_factory=list)
    runtime_metrics: dict[str, Any] = Field(default_factory=dict)

    @field_validator("route_trace_path")
    @classmethod
    def _trace_path_is_safe(cls, value: str | None) -> str | None:
        return None if value is None else _safe_artifact_path(value)

    @model_validator(mode="after")
    def _final_and_envelope_status_agree(self) -> "EvidenceReport":
        expected = {
            EvidenceReportStatus.COMPLETED: ContractStatus.READY,
            EvidenceReportStatus.COMPLETED_WITH_LIMITATIONS: ContractStatus.READY,
            EvidenceReportStatus.BLOCKED: ContractStatus.BLOCKED,
            EvidenceReportStatus.FAILED: ContractStatus.FAILED,
        }
        if self.status != expected[self.final_status]:
            raise ValueError("EvidenceReport final_status conflicts with envelope status")
        if self.final_status == EvidenceReportStatus.COMPLETED_WITH_LIMITATIONS and not self.limitations:
            raise ValueError("completed_with_limitations requires at least one limitation")
        return self


class AssignmentEnvelope(StrictModel):
    schema_version: Literal["ntl.assignment.v1"] = "ntl.assignment.v1"
    assignment_id: str = Field(
        default_factory=lambda: str(uuid4()), min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN
    )
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    source_agent: Literal[AgentRole.ENGINEER] = AgentRole.ENGINEER
    target_agent: Literal[AgentRole.DATA_SEARCHER, AgentRole.ANALYST, AgentRole.EVENT_TRACKER]
    objective: str = Field(min_length=1)
    accepted_parent_contracts: list[PackageRef] = Field(default_factory=list)
    allowed_inputs: list[str] = Field(default_factory=list)
    required_output_type: Literal["EventContext", "ObservationPackage", "AnalysisPackage"]
    acceptance_criteria: list[str] = Field(default_factory=list)
    prohibited_changes: list[str] = Field(default_factory=list)
    budget: dict[str, Any] = Field(default_factory=dict)
    return_format: Literal["HandoffEnvelope"] = "HandoffEnvelope"

    @field_validator("allowed_inputs")
    @classmethod
    def _allowed_inputs_are_workspace_paths(cls, values: list[str]) -> list[str]:
        for value in values:
            raw = str(value or "").strip().replace("\\", "/")
            if (
                not raw
                or "\x00" in raw
                or PureWindowsPath(raw).is_absolute()
                or (PurePosixPath(raw).is_absolute() and not raw.startswith(("/data/raw/", "/data/processed/", "/shared/")))
                or ".." in PurePosixPath(raw).parts
            ):
                raise ValueError(f"unsafe allowed input path: {value}")
        return values

    @model_validator(mode="after")
    def _target_matches_output(self) -> "AssignmentEnvelope":
        expected = {
            AgentRole.DATA_SEARCHER: "ObservationPackage",
            AgentRole.ANALYST: "AnalysisPackage",
            AgentRole.EVENT_TRACKER: "EventContext",
        }
        if self.required_output_type != expected[self.target_agent]:
            raise ValueError("required_output_type does not match target_agent")
        return self


class HandoffEnvelope(StrictModel):
    schema_version: Literal["ntl.handoff.v1"] = "ntl.handoff.v1"
    handoff_id: str = Field(
        default_factory=lambda: str(uuid4()), min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN
    )
    assignment_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    producer: Literal[AgentRole.DATA_SEARCHER, AgentRole.ANALYST, AgentRole.EVENT_TRACKER]
    status: ContractStatus
    package: PackageRef | None = None
    summary: list[str] = Field(default_factory=list, max_length=8)
    validation_verdict: Literal["passed", "failed", "not_applicable"]
    limitations: list[str] = Field(default_factory=list)
    engineer_decision_required: bool = True
    upstream_revision_required: bool = False
    error: ContractError | None = None

    @model_validator(mode="after")
    def _validate_payload(self) -> "HandoffEnvelope":
        expected_type = {
            AgentRole.DATA_SEARCHER: "ObservationPackage",
            AgentRole.ANALYST: "AnalysisPackage",
            AgentRole.EVENT_TRACKER: "EventContext",
        }
        if self.status == ContractStatus.READY:
            if self.package is None or self.validation_verdict != "passed":
                raise ValueError("ready handoffs require a validated package")
            if self.package.artifact_type != expected_type[self.producer]:
                raise ValueError("handoff package type does not match producer")
            if not 3 <= len(self.summary) <= 8:
                raise ValueError("ready handoff summary must contain 3-8 items")
        elif self.status in {ContractStatus.BLOCKED, ContractStatus.FAILED} and self.error is None:
            raise ValueError("blocked/failed handoffs require an error")
        return self


class EngineerValidation(StrictModel):
    schema_valid: bool
    artifact_exists: bool
    checksum_valid: bool
    assignment_scope_valid: bool
    semantic_consistency_valid: bool
    producer_validation_passed: bool

    def all_passed(self) -> bool:
        return all(bool(value) for value in self.model_dump().values())


class EngineerDecision(StrictModel):
    schema_version: Literal["ntl.engineer-decision.v1"] = "ntl.engineer-decision.v1"
    decision_id: str = Field(
        default_factory=lambda: str(uuid4()), min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN
    )
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    assignment_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    handoff_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    handoff_sha256: str = Field(pattern=_SHA256_PATTERN)
    decision: HandoffDecision
    package: PackageRef | None = None
    validation: EngineerValidation
    reason: str = Field(min_length=1)
    revision_request: RevisionRequest | None = None
    error: ContractError | None = None
    decided_at_utc: datetime = Field(default_factory=_utc_now)

    @field_validator("decided_at_utc")
    @classmethod
    def _decided_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("decided_at_utc must include a UTC offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _validate_decision(self) -> "EngineerDecision":
        if self.decision == HandoffDecision.ACCEPTED:
            if self.package is None or not self.validation.all_passed():
                raise ValueError("accepted handoff requires a package and all Engineer checks to pass")
            if self.revision_request is not None or self.error is not None:
                raise ValueError("accepted handoff cannot carry revision_request or error")
        elif self.decision == HandoffDecision.REVISION_REQUESTED:
            if self.revision_request is None:
                raise ValueError("revision_requested decision requires RevisionRequest")
        elif self.decision in {HandoffDecision.REJECTED, HandoffDecision.BLOCKED} and self.error is None:
            raise ValueError("rejected/blocked decision requires an error")
        return self


def canonical_json(value: BaseModel | dict[str, Any]) -> str:
    """Return stable UTF-8 JSON used both for persistence and SHA-256."""

    raw = value.model_dump(mode="json", exclude_none=False) if isinstance(value, BaseModel) else value
    jsonable = to_jsonable_python(raw)
    return json.dumps(jsonable, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def contract_sha256(value: BaseModel | dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()

