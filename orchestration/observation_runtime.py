"""System-owned runtime evidence for ObservationPackage query timestamps.

The model never receives this registry or its scope identifiers.  A successful
geodata inspection records a timestamp under the current LangGraph thread and
benchmark task scope.  ``save_observation_package`` may consume exactly one
matching full-inspector record, preventing model-authored, stale, or
cross-thread query timestamps from entering the persisted contract.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config

from storage_manager import current_thread_id, storage_manager


ObservationToolName = Literal["geodata_inspector_tool", "geodata_quick_check_tool"]
ObservationMode = Literal["basic", "full"]

_EVIDENCE_LIMIT = 4096
_EVIDENCE_LOCK = RLock()
_EVIDENCE_SEQUENCE = 0


@dataclass(frozen=True, slots=True)
class ObservationToolEvidence:
    """Internal-only proof that one observation tool call completed."""

    thread_id: str
    run_scope: str
    task_scope: str
    task_submitted_at_utc: datetime
    tool_name: ObservationToolName
    mode: ObservationMode
    started_at_utc: datetime
    completed_at_utc: datetime


_EVIDENCE: "OrderedDict[int, ObservationToolEvidence]" = OrderedDict()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def observation_tool_started_at() -> datetime:
    """Return a trusted UTC start time for an observation tool call."""

    return _utc_now()


def _runtime_config(config: Optional[RunnableConfig]) -> dict[str, Any]:
    runtime = config if isinstance(config, dict) else var_child_runnable_config.get()
    return runtime if isinstance(runtime, dict) else {}


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value is not None and value != "":
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _scope(
    config: Optional[RunnableConfig],
) -> tuple[str, str, str, datetime] | None:
    runtime = _runtime_config(config)
    thread_id = str(storage_manager.get_thread_id_from_config(runtime) or "").strip()
    if not thread_id:
        thread_id = str(current_thread_id.get() or "debug").strip() or "debug"
    metadata = runtime.get("metadata", {})
    metadata = metadata if isinstance(metadata, dict) else {}
    run_scope = str(metadata.get("task_run_id") or "").strip()
    task_scope = str(metadata.get("case_id") or "").strip()
    submitted_at = _parse_utc(metadata.get("task_submitted_at"))
    if not run_scope or not task_scope or submitted_at is None:
        return None
    return thread_id, run_scope, task_scope, submitted_at


def record_observation_tool_success(
    *,
    tool_name: ObservationToolName,
    mode: ObservationMode,
    started_at_utc: datetime,
    config: Optional[RunnableConfig] = None,
) -> ObservationToolEvidence | None:
    """Record a completed successful call without exposing runtime identity.

    Invalid or future submission metadata is rejected rather than weakening
    the later save gate.  Calls that raise before reaching this function leave
    no success evidence.
    """

    global _EVIDENCE_SEQUENCE
    started = _parse_utc(started_at_utc)
    if started is None:
        return None
    completed = _utc_now()
    scope = _scope(config)
    if scope is None:
        return None
    thread_id, run_scope, task_scope, submitted_at = scope
    if completed < submitted_at:
        return None
    evidence = ObservationToolEvidence(
        thread_id=thread_id,
        run_scope=run_scope,
        task_scope=task_scope,
        task_submitted_at_utc=submitted_at,
        tool_name=tool_name,
        mode=mode,
        started_at_utc=started,
        completed_at_utc=completed,
    )
    with _EVIDENCE_LOCK:
        _EVIDENCE_SEQUENCE += 1
        _EVIDENCE[_EVIDENCE_SEQUENCE] = evidence
        while len(_EVIDENCE) > _EVIDENCE_LIMIT:
            _EVIDENCE.popitem(last=False)
    return evidence


def consume_full_inspector_evidence(
    config: Optional[RunnableConfig] = None,
) -> ObservationToolEvidence | None:
    """Consume the newest valid full-inspector record for the current task.

    A benchmark submission time is mandatory.  Exact submission-time matching
    plus thread/run/task binding prevents evidence from an older retry or a
    different task from being reused, even if an identifier were recycled.
    """

    scope = _scope(config)
    if scope is None:
        return None
    thread_id, run_scope, task_scope, submitted_at = scope
    with _EVIDENCE_LOCK:
        for sequence, evidence in reversed(tuple(_EVIDENCE.items())):
            if (
                evidence.thread_id == thread_id
                and evidence.run_scope == run_scope
                and evidence.task_scope == task_scope
                and evidence.task_submitted_at_utc == submitted_at
                and evidence.tool_name == "geodata_inspector_tool"
                and evidence.mode == "full"
                and evidence.completed_at_utc >= submitted_at
            ):
                del _EVIDENCE[sequence]
                return evidence
    return None


def restore_full_inspector_evidence(
    evidence: ObservationToolEvidence,
    config: Optional[RunnableConfig] = None,
) -> bool:
    """Restore a reserved record after contract validation/persistence fails.

    Restoration is accepted only for the exact current benchmark scope.  This
    gives the model one repair path without allowing cross-task evidence reuse.
    """

    global _EVIDENCE_SEQUENCE
    scope = _scope(config)
    if scope is None:
        return False
    thread_id, run_scope, task_scope, submitted_at = scope
    if (
        evidence.thread_id != thread_id
        or evidence.run_scope != run_scope
        or evidence.task_scope != task_scope
        or evidence.task_submitted_at_utc != submitted_at
        or evidence.tool_name != "geodata_inspector_tool"
        or evidence.mode != "full"
        or evidence.completed_at_utc < submitted_at
    ):
        return False
    with _EVIDENCE_LOCK:
        _EVIDENCE_SEQUENCE += 1
        _EVIDENCE[_EVIDENCE_SEQUENCE] = evidence
        while len(_EVIDENCE) > _EVIDENCE_LIMIT:
            _EVIDENCE.popitem(last=False)
    return True


def clear_observation_evidence_for_tests() -> None:
    """Reset the process-local registry for deterministic provider-free tests."""

    global _EVIDENCE_SEQUENCE
    with _EVIDENCE_LOCK:
        _EVIDENCE.clear()
        _EVIDENCE_SEQUENCE = 0


__all__ = [
    "ObservationToolEvidence",
    "clear_observation_evidence_for_tests",
    "consume_full_inspector_evidence",
    "observation_tool_started_at",
    "record_observation_tool_success",
    "restore_full_inspector_evidence",
]

