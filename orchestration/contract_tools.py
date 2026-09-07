"""LangChain-callable tools for the hierarchical NTL-GPT contract layer."""

from __future__ import annotations

from copy import deepcopy
import json
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Literal, Optional
from uuid import uuid4

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    create_model,
    field_validator,
    model_validator,
)

from contracts.agent_packages import (
    AnalysisPackage,
    ContractEnvelope,
    EngineerDecision,
    EventContext,
    EvidenceReport,
    HandoffEnvelope,
    LocalArtifactDraft,
    ObservationPackage,
    PackageRef,
    RevisionRequest,
    TaskPlan,
    canonical_json,
    contract_sha256,
)
from orchestration.contracts_io import (
    ContractIOError,
    inspect_saved_contract,
    load_contract,
    persist_handoff_decision,
    persist_route_transition,
    save_contract,
    validate_contract_payload,
)
from orchestration.artifact_runtime import (
    ArtifactIdentityError,
    hydrate_local_artifact_identities,
    strip_model_facing_local_artifact_identity,
)
from orchestration.observation_runtime import (
    ObservationToolEvidence,
    consume_full_inspector_evidence,
    restore_full_inspector_evidence,
)
from storage_manager import current_thread_id, storage_manager


ArtifactType = Literal["TaskPlan", "EventContext", "ObservationPackage", "AnalysisPackage", "EvidenceReport"]

_MODEL_HIDDEN_ENVELOPE_FIELDS = frozenset(
    {"run_id", "task_id", "created_at_utc", "query_executed_at_utc"}
)
_PACKAGE_HANDLE_PREFIX = "package/"
_PACKAGE_HANDLE_LIMIT = 4096
_PACKAGE_HANDLE_LOCK = RLock()
_PACKAGE_HANDLES: dict[tuple[str, str], PackageRef] = {}
_PACKAGE_HANDLE_REVERSE: dict[tuple[str, str, str], str] = {}


class OpaquePackageRef(PackageRef):
    """Model-facing package reference that cannot carry a real audit path."""

    path: str = Field(pattern=r"^package/[0-9a-f]{32}$")


def _draft_model(
    name: str,
    source: type[BaseModel],
    *,
    exclude: frozenset[str],
    annotations: dict[str, Any] | None = None,
) -> type[BaseModel]:
    """Build a typed model-facing draft while hiding system-owned fields."""

    annotation_overrides = annotations or {}
    fields: dict[str, tuple[Any, Any]] = {}
    for field_name, field_info in source.model_fields.items():
        if field_name in exclude:
            continue
        fields[field_name] = (
            annotation_overrides.get(field_name, field_info.annotation),
            deepcopy(field_info),
        )
    return create_model(
        name,
        __config__=ConfigDict(**source.model_config),
        **fields,
    )


RevisionRequestDraft = _draft_model(
    "RevisionRequestDraft",
    RevisionRequest,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
    annotations={"related_package": OpaquePackageRef | None},
)
TaskPlanDraft = _draft_model(
    "TaskPlanDraft",
    TaskPlan,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
)
EventContextDraft = _draft_model(
    "EventContextDraft",
    EventContext,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
)
ObservationPackageDraft = _draft_model(
    "ObservationPackageDraft",
    ObservationPackage,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
    annotations={"analysis_ready_artifacts": list[LocalArtifactDraft]},
)
AnalysisPackageDraft = _draft_model(
    "AnalysisPackageDraft",
    AnalysisPackage,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
    annotations={
        "artifacts": list[LocalArtifactDraft],
        "linked_contracts": list[OpaquePackageRef],
        "revision_request": RevisionRequestDraft | None,
    },
)
EvidenceReportDraft = _draft_model(
    "EvidenceReportDraft",
    EvidenceReport,
    exclude=_MODEL_HIDDEN_ENVELOPE_FIELDS,
    annotations={"representative_artifacts": list[LocalArtifactDraft]},
)
HandoffEnvelopeDraft = _draft_model(
    "HandoffEnvelopeDraft",
    HandoffEnvelope,
    exclude=frozenset({"run_id", "task_id"}),
    annotations={"package": OpaquePackageRef | None},
)
EngineerDecisionDraft = _draft_model(
    "EngineerDecisionDraft",
    EngineerDecision,
    exclude=frozenset(
        {
            "run_id",
            "task_id",
            "assignment_id",
            "handoff_id",
            "handoff_sha256",
            "package",
            "decided_at_utc",
        }
    ),
    annotations={"revision_request": RevisionRequestDraft | None},
)


def _register_package_handle(reference: PackageRef, *, thread_id: str) -> PackageRef:
    """Return a thread-scoped opaque reference for one verified real reference."""

    reverse_key = (thread_id, reference.path, reference.sha256)
    with _PACKAGE_HANDLE_LOCK:
        token = _PACKAGE_HANDLE_REVERSE.get(reverse_key)
        if token is None:
            token = uuid4().hex
            if len(_PACKAGE_HANDLES) >= _PACKAGE_HANDLE_LIMIT:
                oldest_key = next(iter(_PACKAGE_HANDLES))
                oldest = _PACKAGE_HANDLES.pop(oldest_key)
                _PACKAGE_HANDLE_REVERSE.pop(
                    (oldest_key[0], oldest.path, oldest.sha256),
                    None,
                )
            _PACKAGE_HANDLES[(thread_id, token)] = reference
            _PACKAGE_HANDLE_REVERSE[reverse_key] = token
    return reference.model_copy(update={"path": f"{_PACKAGE_HANDLE_PREFIX}{token}"})


def _expand_package_handle(
    reference: PackageRef | dict[str, Any],
    *,
    thread_id: str,
) -> PackageRef:
    """Resolve a current-thread opaque ref, rejecting unknown or altered handles."""

    supplied = reference if isinstance(reference, PackageRef) else PackageRef.model_validate(reference)
    if not supplied.path.startswith(_PACKAGE_HANDLE_PREFIX):
        return supplied
    token = supplied.path[len(_PACKAGE_HANDLE_PREFIX) :]
    if not token or "/" in token:
        raise ContractIOError("invalid opaque package handle")
    with _PACKAGE_HANDLE_LOCK:
        actual = _PACKAGE_HANDLES.get((thread_id, token))
    if actual is None:
        raise ContractIOError("unknown package handle for the current thread")
    if (
        supplied.artifact_id != actual.artifact_id
        or supplied.artifact_type != actual.artifact_type
        or supplied.sha256 != actual.sha256
    ):
        raise ContractIOError("opaque package handle metadata does not match its registered package")
    return actual


def _expand_package_path_handle(path: str, *, thread_id: str) -> str:
    raw = str(path or "").strip()
    if not raw.startswith(_PACKAGE_HANDLE_PREFIX):
        return raw
    token = raw[len(_PACKAGE_HANDLE_PREFIX) :]
    if not token or "/" in token:
        raise ContractIOError("invalid opaque package handle")
    with _PACKAGE_HANDLE_LOCK:
        actual = _PACKAGE_HANDLES.get((thread_id, token))
    if actual is None:
        raise ContractIOError("unknown package handle for the current thread")
    return actual.path


def _expand_related_package(
    raw: dict[str, Any],
    *,
    thread_id: str,
) -> None:
    reference = raw.get("related_package")
    if reference is not None:
        raw["related_package"] = _expand_package_handle(
            reference,
            thread_id=thread_id,
        ).model_dump(mode="json")


def _expand_contract_package_handles(raw: dict[str, Any], *, thread_id: str) -> None:
    if raw.get("artifact_type") == "AnalysisPackage":
        raw["linked_contracts"] = [
            _expand_package_handle(reference, thread_id=thread_id).model_dump(mode="json")
            for reference in raw.get("linked_contracts", [])
        ]
    revision = raw.get("revision_request")
    if isinstance(revision, dict):
        revision = dict(revision)
        _expand_related_package(revision, thread_id=thread_id)
        raw["revision_request"] = revision


class _ToolInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SaveTaskPlanInput(_ToolInput):
    contract: TaskPlanDraft = Field(
        description=(
            "Complete model-authored TaskPlan draft. Runtime identity and creation time are injected by the "
            "system; Gold/evaluator fields are forbidden."
        )
    )


class SaveEventContextInput(_ToolInput):
    contract: EventContextDraft = Field(
        description=(
            "Complete ntl.contract.v1 EventContext, including as-of/retrieval timestamps, sources, and the "
            "non-attribution boundary. For staged-input or generated-output provenance, declare only the local "
            "path and semantic metadata; the system injects SHA-256 and byte count. Gold/evaluator fields are "
            "forbidden."
        )
    )


class SaveObservationPackageInput(_ToolInput):
    contract: ObservationPackageDraft = Field(
        description=(
            "Complete ntl.contract.v1 ObservationPackage with product, availability, validation, provenance, "
            "and any analysis-ready artifacts. For local artifacts declare only path, media_type, and role; "
            "the system injects SHA-256 and byte count. The query execution time is injected from the current "
            "task's successful full geodata inspection; Gold/evaluator fields are forbidden."
        )
    )


class SaveAnalysisPackageInput(_ToolInput):
    contract: AnalysisPackageDraft = Field(
        description=(
            "Complete ntl.contract.v1 AnalysisPackage, including the scientific question, analysis unit, "
            "method, validation, findings, artifacts, and any revision request. For local artifacts declare "
            "only path, media_type, and role; the system injects SHA-256 and byte count. Gold/evaluator fields "
            "are forbidden."
        )
    )


class SaveEvidenceReportInput(_ToolInput):
    contract: EvidenceReportDraft = Field(
        description=(
            "Complete ntl.contract.v1 EvidenceReport, including final status, direct answer, route evidence, "
            "validation, limitations, and runtime metrics. Local artifact references need only path and semantic "
            "metadata; the system injects SHA-256 and byte count. Gold/evaluator fields are forbidden."
        )
    )


class ValidateContractInput(_ToolInput):
    contract_path: str = Field(
        pattern=r"^package/[0-9a-f]{32}$",
        description="Opaque current-thread package handle returned by a typed save tool.",
    )
    expected_artifact_type: ArtifactType | None = None


class RecordHandoffDecisionInput(_ToolInput):
    handoff: HandoffEnvelopeDraft = Field(
        description="Specialist HandoffEnvelope draft; runtime run/task identity is injected by the system."
    )
    decision: EngineerDecisionDraft = Field(
        description=(
            "EngineerDecision draft. The tool derives run/task/assignment/handoff identity, package, canonical "
            "handoff_sha256, and decision time from the supplied handoff and runtime."
        )
    )


class RecordRouteTransitionInput(_ToolInput):
    target_status: Literal[
        "planning",
        "needs_clarification",
        "direct_execution",
        "specialist_routing",
        "event_tracking",
        "data_preparation",
        "analysis",
        "handoff_validation",
        "revision_requested",
        "synthesis",
        "completed",
        "blocked",
        "failed",
    ]
    reason: str = Field(min_length=1)
    max_revisions: int = Field(default=2, ge=0, le=10)
    contract_refs: list[OpaquePackageRef] = Field(default_factory=list)
    error_code: str | None = Field(
        default=None,
        description="Required for blocked/failed; must be one of the standard NTL contract error codes.",
    )


def _resolve_thread_id(config: Optional[RunnableConfig]) -> str:
    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    if isinstance(runtime, dict):
        thread_id = str(storage_manager.get_thread_id_from_config(runtime) or "").strip()
        if thread_id:
            return thread_id
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _runtime_identity(config: Optional[RunnableConfig]) -> tuple[str, str, bool]:
    """Resolve run/task identity from benchmark metadata, with a UI-safe fallback."""

    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    metadata = runtime.get("metadata", {}) if isinstance(runtime, dict) else {}
    thread_id = _resolve_thread_id(config)
    has_authoritative_identity = bool(metadata.get("task_run_id") and metadata.get("case_id"))
    run_id = str(metadata.get("task_run_id") or thread_id).strip()
    task_id = str(metadata.get("case_id") or thread_id).strip()
    return run_id, task_id, has_authoritative_identity


def _utc_now() -> datetime:
    """Return the system-authored timestamp used for persisted contract creation."""

    return datetime.now(timezone.utc)


def _runtime_created_at(config: Optional[RunnableConfig]) -> datetime:
    """Use the stable system submission time when the runner supplies it."""

    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    metadata = runtime.get("metadata", {}) if isinstance(runtime, dict) else {}
    value = metadata.get("task_submitted_at") if isinstance(metadata, dict) else None
    if isinstance(value, datetime):
        parsed = value
    elif value is not None and value != "":
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return _utc_now()
    else:
        return _utc_now()
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return _utc_now()
    return parsed.astimezone(timezone.utc)


def _coerce_tool_mapping(value: Any) -> dict[str, Any] | Any:
    """Copy a model/JSON mapping without weakening downstream schema validation."""

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=False)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        return dict(parsed) if isinstance(parsed, dict) else value
    return dict(value) if isinstance(value, dict) else value


def _strip_observation_query_time(value: Any) -> Any:
    """Remove model-authored copies of the system-managed observation time."""

    if isinstance(value, dict):
        return {
            key: _strip_observation_query_time(item)
            for key, item in value.items()
            if key != "query_executed_at_utc"
        }
    if isinstance(value, list):
        return [_strip_observation_query_time(item) for item in value]
    return value


def _fill_missing_identity(raw: dict[str, Any], *, run_id: str, task_id: str) -> None:
    if raw.get("run_id") in {None, ""}:
        raw["run_id"] = run_id
    if raw.get("task_id") in {None, ""}:
        raw["task_id"] = task_id


def _normalize_nested_revision_identity(
    raw: dict[str, Any],
    *,
    run_id: str,
    task_id: str,
    authoritative: bool,
) -> None:
    """Bind a nested RevisionRequest to the same run without touching its package ref."""

    revision = raw.get("revision_request")
    if not isinstance(revision, dict):
        return
    revision = dict(revision)
    if authoritative:
        revision["run_id"] = run_id
        revision["task_id"] = task_id
    else:
        _fill_missing_identity(revision, run_id=run_id, task_id=task_id)
    raw["revision_request"] = revision


def _hydrate_contract_identity(
    contract: ContractEnvelope | dict[str, Any] | str,
    config: Optional[RunnableConfig],
    *,
    artifact_type: ArtifactType | None = None,
) -> dict[str, Any] | str:
    """Bind benchmark contracts to runtime identity and hydrate UI fallbacks.

    Benchmark ``task_run_id``/``case_id`` metadata is a system fact and wins
    over model-authored envelope values.  The creation timestamp is likewise
    authored by the save tool.  Scientific timestamps such as ``as_of_utc``
    remain untouched.
    """

    raw = _coerce_tool_mapping(contract)
    if not isinstance(raw, dict):
        return contract
    run_id, task_id, authoritative = _runtime_identity(config)
    if authoritative:
        raw["run_id"] = run_id
        raw["task_id"] = task_id
        raw["created_at_utc"] = _runtime_created_at(config).isoformat()
    else:
        _fill_missing_identity(raw, run_id=run_id, task_id=task_id)
    _normalize_nested_revision_identity(
        raw,
        run_id=run_id,
        task_id=task_id,
        authoritative=authoritative,
    )
    thread_id = _resolve_thread_id(config)
    effective_artifact_type = str(artifact_type or raw.get("artifact_type") or "").strip()
    # TaskPlan.expected_outputs contains declarations of files that downstream
    # work will create. They are not ArtifactRecords yet and therefore must not
    # be measured at planning time. Other local references remain subject to
    # the normal identity binding.
    planned_outputs = (
        raw.pop("expected_outputs", None)
        if effective_artifact_type == "TaskPlan"
        else None
    )
    raw = hydrate_local_artifact_identities(
        raw,
        thread_id=thread_id,
        run_id=run_id,
        task_id=task_id,
        fallback_workspace=(
            None if authoritative else storage_manager.get_workspace(thread_id)
        ),
        # Full ContractEnvelope instances are trusted Python inputs.  Their
        # legacy identity fields are discarded and recomputed, while every
        # model-facing draft (including raw dictionaries) must omit them.
        reject_supplied_identity=not isinstance(contract, ContractEnvelope),
    )
    if effective_artifact_type == "TaskPlan":
        raw["expected_outputs"] = planned_outputs or []
    _expand_contract_package_handles(raw, thread_id=thread_id)
    return raw


def _hydrate_handoff_decision_identity(
    handoff: dict[str, Any] | str,
    decision: dict[str, Any] | str,
    config: Optional[RunnableConfig],
) -> tuple[dict[str, Any] | str, dict[str, Any] | str]:
    """Normalize runtime-owned handoff/decision identity before verification.

    The specialist's package reference is never rewritten: its path, artifact
    identity, and checksum must still pass ``persist_handoff_decision``.  For
    an authoritative benchmark run, the Engineer decision mirrors the
    normalized handoff's deterministic identity and package reference so an
    LLM cannot create a second, conflicting decision identity.
    """

    handoff_raw = _coerce_tool_mapping(handoff)
    decision_raw = _coerce_tool_mapping(decision)
    if not isinstance(handoff_raw, dict) or not isinstance(decision_raw, dict):
        return handoff, decision

    run_id, task_id, authoritative = _runtime_identity(config)
    if authoritative:
        handoff_raw["run_id"] = run_id
        handoff_raw["task_id"] = task_id
    else:
        _fill_missing_identity(handoff_raw, run_id=run_id, task_id=task_id)

    effective_run_id = str(handoff_raw.get("run_id") or run_id).strip()
    effective_task_id = str(handoff_raw.get("task_id") or task_id).strip()
    thread_id = _resolve_thread_id(config)
    if handoff_raw.get("package") is not None:
        handoff_raw["package"] = _expand_package_handle(
            handoff_raw["package"],
            thread_id=thread_id,
        ).model_dump(mode="json")
    if authoritative:
        decision_raw["run_id"] = effective_run_id
        decision_raw["task_id"] = effective_task_id
        # These are deterministic envelope links, not independent model facts.
        decision_raw["assignment_id"] = handoff_raw.get("assignment_id")
        decision_raw["handoff_id"] = handoff_raw.get("handoff_id")
        # Let the persistence layer hash the schema-normalized handoff.
        decision_raw.pop("handoff_sha256", None)
        decision_raw["package"] = handoff_raw.get("package")
    else:
        _fill_missing_identity(
            decision_raw,
            run_id=effective_run_id,
            task_id=effective_task_id,
        )

    _normalize_nested_revision_identity(
        decision_raw,
        run_id=effective_run_id,
        task_id=effective_task_id,
        authoritative=authoritative,
    )
    revision = decision_raw.get("revision_request")
    if isinstance(revision, dict):
        revision = dict(revision)
        _expand_related_package(revision, thread_id=thread_id)
        decision_raw["revision_request"] = revision
    return handoff_raw, decision_raw


def _effective_route_identity(
    run_id: str | None,
    task_id: str | None,
    config: Optional[RunnableConfig],
) -> tuple[str, str]:
    runtime_run_id, runtime_task_id, authoritative = _runtime_identity(config)
    if authoritative:
        return runtime_run_id, runtime_task_id
    return (
        str(run_id or runtime_run_id).strip() or runtime_run_id,
        str(task_id or runtime_task_id).strip() or runtime_task_id,
    )


def _model_facing_result(
    result: dict[str, Any],
    *,
    thread_id: str,
    allow_scientific_paths: bool = False,
) -> dict[str, Any]:
    """Remove raw runtime identity and replace real package paths with handles."""

    if {"artifact_id", "artifact_type", "path", "sha256"}.issubset(result):
        try:
            actual_ref = PackageRef.model_validate(result)
        except Exception:  # noqa: BLE001 - non-PackageRef dictionaries continue below
            pass
        else:
            return _register_package_handle(actual_ref, thread_id=thread_id).model_dump(mode="json")

    actual_package_ref: PackageRef | None = None
    if isinstance(result.get("package_ref"), dict):
        actual_package_ref = PackageRef.model_validate(result["package_ref"])
    public: dict[str, Any] = {}
    for key, value in result.items():
        if key in {"run_id", "task_id"}:
            continue
        # System tool results may contain real audit paths. Scientific contract
        # content may contain workspace-relative inputs/outputs paths needed by
        # the next phase; validate_contract opts into those only below its
        # already identity-stripped ``contract`` field.
        if key == "path" and not allow_scientific_paths:
            continue
        if isinstance(value, dict):
            public[key] = _model_facing_result(
                value,
                thread_id=thread_id,
                allow_scientific_paths=(allow_scientific_paths or key == "contract"),
            )
        elif isinstance(value, list):
            public[key] = [
                _model_facing_result(
                    item,
                    thread_id=thread_id,
                    allow_scientific_paths=allow_scientific_paths,
                )
                if isinstance(item, dict)
                else item
                for item in value
            ]
        else:
            public[key] = value
    if actual_package_ref is not None:
        opaque_ref = _register_package_handle(actual_package_ref, thread_id=thread_id)
        public["package_ref"] = opaque_ref.model_dump(mode="json")
        public["package_handle"] = opaque_ref.path
        # Retain the legacy result key as an opaque alias, never a real path.
        public["path"] = opaque_ref.path
    return public


def _model_facing_contract_content(package: ContractEnvelope, *, thread_id: str) -> dict[str, Any]:
    """Return scientific contract content with all audit identity removed."""

    raw = package.model_dump(mode="json", exclude_none=False)
    for field in _MODEL_HIDDEN_ENVELOPE_FIELDS:
        raw.pop(field, None)
    # This field points into the hidden system audit tree and is not scientific
    # evidence. Route status is available through the typed route tool instead.
    raw.pop("route_trace_path", None)
    if package.artifact_type == "AnalysisPackage":
        raw["linked_contracts"] = [
            _register_package_handle(reference, thread_id=thread_id).model_dump(mode="json")
            for reference in package.linked_contracts
        ]
    revision = raw.get("revision_request")
    if isinstance(revision, dict):
        for field in _MODEL_HIDDEN_ENVELOPE_FIELDS:
            revision.pop(field, None)
        related = package.revision_request.related_package if package.revision_request else None
        if related is not None:
            revision["related_package"] = _register_package_handle(
                related, thread_id=thread_id
            ).model_dump(mode="json")
    return strip_model_facing_local_artifact_identity(raw)


def _failure(tool: str, exc: Exception) -> dict[str, Any]:
    # Internal validators deliberately include exact paths and audit identity
    # in exceptions for runner-side diagnosis. Those details are not part of
    # the model contract: returning them would undo opaque identity isolation.
    if isinstance(exc, ValidationError):
        issues: list[dict[str, str]] = []
        messages = {
            "missing": "Required field is missing.",
            "extra_forbidden": "Field is not accepted by this contract.",
            "list_type": "Expected a list.",
            "dict_type": "Expected an object.",
            "string_type": "Expected text.",
            "bool_type": "Expected true or false.",
            "literal_error": "Value does not match the allowed contract value.",
        }
        for error in exc.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        )[:6]:
            location = ".".join(str(part) for part in error.get("loc", ())) or "contract"
            error_type = str(error.get("type") or "value_error")
            issues.append(
                {
                    "field": location,
                    "type": error_type,
                    "message": messages.get(error_type, "Field failed contract validation."),
                }
            )
        return {
            "status": "failed",
            "tool": tool,
            "error": {
                "code": "CONTRACT_SCHEMA_INVALID",
                "message": "The contract does not match the typed tool schema.",
                "issues": issues,
                "suggestion": (
                    "Correct these fields in one complete draft and retry the same "
                    "save once; do not create probe packages."
                ),
            },
        }
    if isinstance(exc, ArtifactIdentityError):
        supplied_identity = "must be omitted" in str(exc)
        return {
            "status": "failed",
            "tool": tool,
            "error": {
                "code": (
                    "LOCAL_ARTIFACT_IDENTITY_IS_SYSTEM_MANAGED"
                    if supplied_identity
                    else "LOCAL_ARTIFACT_NOT_READY"
                ),
                "message": (
                    "Local artifact checksums and byte counts are injected by the system."
                    if supplied_identity
                    else "A declared local artifact could not be bound to a ready workspace file."
                ),
                "suggestion": (
                    "Remove sha256 and bytes from every inputs/outputs artifact "
                    "record, keep only path, role, and media_type, then retry once."
                    if supplied_identity
                    else "Create and validate the declared workspace-relative file before saving the scientific package."
                ),
            },
        }
    if isinstance(exc, FileExistsError):
        return {
            "status": "failed",
            "tool": tool,
            "error": {
                "code": "CONTRACT_IMMUTABLE_CONFLICT",
                "message": "That contract identity was already persisted with different content.",
                "suggestion": (
                    "Reuse the earlier successful package handle; create a revision "
                    "only for a genuine scientific change, not for schema probing."
                ),
            },
        }
    return {
        "status": "failed",
        "tool": tool,
        "error": {
            "code": "CONTRACT_VALIDATION_OR_IO_FAILED",
            "message": "The system-owned contract could not be validated or persisted.",
            "suggestion": "Correct only the reported schema, path, checksum, or handoff inconsistency and retry within budget.",
        },
    }


def _save_typed_contract(
    contract: ContractEnvelope | dict[str, Any] | str,
    *,
    artifact_type: ArtifactType,
    tool_name: str,
    config: Optional[RunnableConfig],
) -> dict[str, Any]:
    observation_evidence: ObservationToolEvidence | None = None
    try:
        thread_id = _resolve_thread_id(config)
        contract_to_hydrate: ContractEnvelope | dict[str, Any] | str = contract
        if artifact_type == "ObservationPackage":
            raw = _coerce_tool_mapping(contract)
            if not isinstance(raw, dict):
                raise ContractIOError("ObservationPackage draft must be a mapping")
            # Never accept a caller/model timestamp, including trusted direct
            # Python callers that bypass the model-facing draft schema.  Free-
            # form validation/provenance objects are scrubbed recursively too,
            # so a conflicting duplicate cannot survive below the envelope.
            raw = _strip_observation_query_time(raw)
            observation_evidence = consume_full_inspector_evidence(config)
            if observation_evidence is None:
                raise ContractIOError(
                    "ObservationPackage requires an unconsumed full geodata inspection "
                    "from the current thread and submitted task"
                )
            raw["query_executed_at_utc"] = observation_evidence.completed_at_utc.isoformat()
            contract_to_hydrate = raw
        return _model_facing_result(
            save_contract(
                _hydrate_contract_identity(
                    contract_to_hydrate,
                    config,
                    artifact_type=artifact_type,
                ),
                thread_id=thread_id,
                expected_artifact_type=artifact_type,
            ),
            thread_id=thread_id,
        )
    except Exception as exc:  # noqa: BLE001 - agent tools return structured failures
        if observation_evidence is not None:
            restore_full_inspector_evidence(observation_evidence, config)
        return _failure(tool_name, exc)


def save_task_plan(
    contract: TaskPlan | dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    return _save_typed_contract(
        contract, artifact_type="TaskPlan", tool_name="save_task_plan", config=config
    )


def save_event_context(
    contract: EventContext | dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    return _save_typed_contract(
        contract, artifact_type="EventContext", tool_name="save_event_context", config=config
    )


def save_observation_package(
    contract: ObservationPackage | dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    return _save_typed_contract(
        contract,
        artifact_type="ObservationPackage",
        tool_name="save_observation_package",
        config=config,
    )


def save_analysis_package(
    contract: AnalysisPackage | dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    return _save_typed_contract(
        contract,
        artifact_type="AnalysisPackage",
        tool_name="save_analysis_package",
        config=config,
    )


def save_evidence_report(
    contract: EvidenceReport | dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    return _save_typed_contract(
        contract,
        artifact_type="EvidenceReport",
        tool_name="save_evidence_report",
        config=config,
    )


def validate_contract(
    contract: dict[str, Any] | str | None = None,
    contract_path: str | None = None,
    expected_artifact_type: ArtifactType | None = None,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    """Validate a trusted in-memory contract or persisted opaque package handle."""

    try:
        # The LangChain tool surface exposes only contract_path. The in-memory
        # branch remains a trusted Python API for internal tests/callers.
        if (contract is None) == (contract_path is None):
            raise ContractIOError("supply exactly one of contract or contract_path")
        if contract_path is not None:
            thread_id = _resolve_thread_id(config)
            actual_path = _expand_package_path_handle(contract_path, thread_id=thread_id)
            inspected = inspect_saved_contract(
                actual_path,
                thread_id=thread_id,
                expected_artifact_type=expected_artifact_type,
            )
            package = load_contract(
                actual_path,
                thread_id=thread_id,
                expected_artifact_type=expected_artifact_type,
            )
            inspected["contract"] = _model_facing_contract_content(
                package,
                thread_id=thread_id,
            )
            return _model_facing_result(
                inspected,
                thread_id=thread_id,
            )
        validated = validate_contract_payload(contract, expected_artifact_type=expected_artifact_type)
        canonical = canonical_json(validated)
        return _model_facing_result(
            {
                "status": "success",
                "schema_version": validated.schema_version,
                "artifact_type": validated.artifact_type,
                "artifact_id": validated.artifact_id,
                "run_id": validated.run_id,
                "task_id": validated.task_id,
                "sha256": contract_sha256(validated),
                "canonical_bytes": len(canonical.encode("utf-8")),
            },
            thread_id=_resolve_thread_id(config),
        )
    except Exception as exc:  # noqa: BLE001 - agent tools return structured failures
        return _failure("validate_contract", exc)


def record_handoff_decision(
    handoff: dict[str, Any] | str,
    decision: dict[str, Any] | str,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    try:
        normalized_handoff, normalized_decision = _hydrate_handoff_decision_identity(
            handoff,
            decision,
            config,
        )
        thread_id = _resolve_thread_id(config)
        return _model_facing_result(
            persist_handoff_decision(
                normalized_handoff,
                normalized_decision,
                thread_id=thread_id,
            ),
            thread_id=thread_id,
        )
    except Exception as exc:  # noqa: BLE001 - agent tools return structured failures
        return _failure("record_handoff_decision", exc)


def record_route_transition(
    target_status: str,
    reason: str,
    run_id: str | None = None,
    task_id: str | None = None,
    max_revisions: int = 2,
    contract_refs: list[dict[str, Any]] | None = None,
    error_code: str | None = None,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    try:
        effective_run_id, effective_task_id = _effective_route_identity(
            run_id,
            task_id,
            config,
        )
        thread_id = _resolve_thread_id(config)
        expanded_refs = [
            _expand_package_handle(reference, thread_id=thread_id)
            for reference in (contract_refs or [])
        ]
        return _model_facing_result(
            persist_route_transition(
                run_id=effective_run_id,
                task_id=effective_task_id,
                target_status=target_status,
                reason=reason,
                thread_id=thread_id,
                max_revisions=max_revisions,
                contract_refs=expanded_refs,
                error_code=error_code,
            ),
            thread_id=thread_id,
        )
    except Exception as exc:  # noqa: BLE001 - agent tools return structured failures
        return _failure("record_route_transition", exc)


save_task_plan_tool = StructuredTool.from_function(
    func=save_task_plan,
    name="save_task_plan",
    description=(
        "Validate and immutably save a TaskPlan under the system-owned audit tree. "
        "Returns an opaque current-thread package handle and SHA-256, never runtime identity. "
        "Never include benchmark Gold."
    ),
    args_schema=SaveTaskPlanInput,
)

save_event_context_tool = StructuredTool.from_function(
    func=save_event_context,
    name="save_event_context",
    description=(
        "Validate and immutably save an EventContext with as-of time, source provenance, conflicts, and "
        "non-attribution boundary. Returns an opaque PackageRef-compatible handle and SHA-256."
    ),
    args_schema=SaveEventContextInput,
)

save_observation_package_tool = StructuredTool.from_function(
    func=save_observation_package,
    name="save_observation_package",
    description=(
        "Validate and immutably save an ObservationPackage with product, availability, QA, provenance, "
        "validation, and analysis-ready artifact records."
    ),
    args_schema=SaveObservationPackageInput,
)

save_analysis_package_tool = StructuredTool.from_function(
    func=save_analysis_package,
    name="save_analysis_package",
    description=(
        "Validate and immutably save an AnalysisPackage with linked contracts, method, artifacts, "
        "validation, findings, and any bounded RevisionRequest."
    ),
    args_schema=SaveAnalysisPackageInput,
)

save_evidence_report_tool = StructuredTool.from_function(
    func=save_evidence_report,
    name="save_evidence_report",
    description=(
        "Validate and immutably save the Engineer's final EvidenceReport with answer, provenance, route, "
        "limitations, validation, and runtime metrics."
    ),
    args_schema=SaveEvidenceReportInput,
)

validate_contract_tool = StructuredTool.from_function(
    func=validate_contract,
    name="validate_contract",
    description=(
        "Validate an opaque current-thread package handle, canonical bytes, and actual SHA-256, then return "
        "the scientific contract content with runtime identity and internal audit paths removed."
    ),
    args_schema=ValidateContractInput,
)

record_handoff_decision_tool = StructuredTool.from_function(
    func=record_handoff_decision,
    name="record_handoff_decision",
    description=(
        "Verify a specialist HandoffEnvelope and record the Engineer's accept, revise, reject, or block "
        "decision. Acceptance checks the referenced package and declared artifact checksums."
    ),
    args_schema=RecordHandoffDecisionInput,
)

record_route_transition_tool = StructuredTool.from_function(
    func=record_route_transition,
    name="record_route_transition",
    description=(
        "Advance and atomically checkpoint the current run's deterministic route state. It initializes at "
        "received, enforces legal transitions and revision budget, and verifies referenced packages."
    ),
    args_schema=RecordRouteTransitionInput,
)


CONTRACT_TOOLS = [
    save_task_plan_tool,
    save_event_context_tool,
    save_observation_package_tool,
    save_analysis_package_tool,
    save_evidence_report_tool,
    validate_contract_tool,
    record_handoff_decision_tool,
    record_route_transition_tool,
]


__all__ = [
    "CONTRACT_TOOLS",
    "record_handoff_decision",
    "record_handoff_decision_tool",
    "record_route_transition",
    "record_route_transition_tool",
    "save_analysis_package",
    "save_analysis_package_tool",
    "save_event_context",
    "save_event_context_tool",
    "save_evidence_report",
    "save_evidence_report_tool",
    "save_observation_package",
    "save_observation_package_tool",
    "save_task_plan",
    "save_task_plan_tool",
    "validate_contract",
    "validate_contract_tool",
]

