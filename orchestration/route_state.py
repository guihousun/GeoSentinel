"""Deterministic, restorable route-state ledger outside LLM message state."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from contracts.agent_packages import ErrorCode, PackageRef


_SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$"


class RouteStatus(str, Enum):
    RECEIVED = "received"
    PLANNING = "planning"
    NEEDS_CLARIFICATION = "needs_clarification"
    DIRECT_EXECUTION = "direct_execution"
    SPECIALIST_ROUTING = "specialist_routing"
    EVENT_TRACKING = "event_tracking"
    DATA_PREPARATION = "data_preparation"
    ANALYSIS = "analysis"
    HANDOFF_VALIDATION = "handoff_validation"
    REVISION_REQUESTED = "revision_requested"
    SYNTHESIS = "synthesis"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    FAILED = "failed"


TERMINAL_ROUTE_STATUSES = frozenset({RouteStatus.COMPLETED, RouteStatus.BLOCKED, RouteStatus.FAILED})

ALLOWED_TRANSITIONS: dict[RouteStatus, frozenset[RouteStatus]] = {
    RouteStatus.RECEIVED: frozenset({RouteStatus.PLANNING}),
    RouteStatus.PLANNING: frozenset(
        {RouteStatus.NEEDS_CLARIFICATION, RouteStatus.DIRECT_EXECUTION, RouteStatus.SPECIALIST_ROUTING}
    ),
    RouteStatus.NEEDS_CLARIFICATION: frozenset({RouteStatus.PLANNING, RouteStatus.BLOCKED}),
    RouteStatus.DIRECT_EXECUTION: frozenset({RouteStatus.SYNTHESIS, RouteStatus.FAILED}),
    RouteStatus.SPECIALIST_ROUTING: frozenset(
        {
            RouteStatus.EVENT_TRACKING,
            RouteStatus.DATA_PREPARATION,
            RouteStatus.ANALYSIS,
            # A native task may legitimately return a summary-only result.
            # It has no named package phase to record, but Engineer still
            # needs one bounded handoff-validation transition before synthesis.
            RouteStatus.HANDOFF_VALIDATION,
            RouteStatus.BLOCKED,
        }
    ),
    RouteStatus.EVENT_TRACKING: frozenset({RouteStatus.HANDOFF_VALIDATION, RouteStatus.FAILED}),
    RouteStatus.DATA_PREPARATION: frozenset({RouteStatus.HANDOFF_VALIDATION, RouteStatus.FAILED}),
    RouteStatus.ANALYSIS: frozenset({RouteStatus.HANDOFF_VALIDATION, RouteStatus.FAILED}),
    RouteStatus.HANDOFF_VALIDATION: frozenset(
        {
            RouteStatus.SPECIALIST_ROUTING,
            RouteStatus.REVISION_REQUESTED,
            RouteStatus.SYNTHESIS,
            RouteStatus.BLOCKED,
            RouteStatus.FAILED,
        }
    ),
    RouteStatus.REVISION_REQUESTED: frozenset({RouteStatus.SPECIALIST_ROUTING, RouteStatus.BLOCKED}),
    RouteStatus.SYNTHESIS: frozenset({RouteStatus.COMPLETED, RouteStatus.BLOCKED, RouteStatus.FAILED}),
    RouteStatus.COMPLETED: frozenset(),
    RouteStatus.BLOCKED: frozenset(),
    RouteStatus.FAILED: frozenset(),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RouteEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True, str_strip_whitespace=True)

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    from_status: RouteStatus
    to_status: RouteStatus
    occurred_at_utc: datetime = Field(default_factory=_utc_now)
    actor: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    contract_refs: list[PackageRef] = Field(default_factory=list)
    revision_count: int = Field(ge=0, default=0)
    error_code: ErrorCode | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at_utc")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at_utc must include a UTC offset")
        return value.astimezone(timezone.utc)


class RouteState(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True, str_strip_whitespace=True)

    schema_version: str = "ntl.route-state.v1"
    run_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    task_id: str = Field(min_length=1, max_length=160, pattern=_SAFE_ID_PATTERN)
    status: RouteStatus = RouteStatus.RECEIVED
    revision_count: int = Field(default=0, ge=0)
    max_revisions: int = Field(default=2, ge=0)
    accepted_packages: dict[str, PackageRef] = Field(default_factory=dict)
    specialist_status: dict[str, str] = Field(default_factory=dict)
    skipped_specialists: dict[str, str] = Field(default_factory=dict)
    events: list[RouteEvent] = Field(default_factory=list)
    started_at_utc: datetime = Field(default_factory=_utc_now)
    updated_at_utc: datetime = Field(default_factory=_utc_now)

    @field_validator("started_at_utc", "updated_at_utc")
    @classmethod
    def _state_times_are_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("route timestamps must include a UTC offset")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _ledger_is_replayable(self) -> "RouteState":
        if self.revision_count > self.max_revisions:
            raise ValueError("revision_count cannot exceed max_revisions")
        if not self.events:
            if self.status != RouteStatus.RECEIVED or self.revision_count:
                raise ValueError("non-initial route state requires replayable events")
            return self

        expected_from = RouteStatus.RECEIVED
        counted_revisions = 0
        previous_time = self.started_at_utc
        for event in self.events:
            event_from = RouteStatus(event.from_status)
            event_to = RouteStatus(event.to_status)
            if event_from != expected_from:
                raise ValueError("route event chain is discontinuous")
            if event_to not in ALLOWED_TRANSITIONS[event_from]:
                raise ValueError(f"illegal persisted route transition: {event_from.value} -> {event_to.value}")
            if event.occurred_at_utc < previous_time:
                raise ValueError("route event timestamps must be monotonic")
            if event_to == RouteStatus.REVISION_REQUESTED:
                counted_revisions += 1
            if event.revision_count != counted_revisions:
                raise ValueError("route event revision_count is inconsistent")
            previous_time = event.occurred_at_utc
            expected_from = event_to

        if expected_from != RouteStatus(self.status):
            raise ValueError("route status does not match the last persisted event")
        if counted_revisions != self.revision_count:
            raise ValueError("route revision_count cannot be reconstructed from events")
        if self.updated_at_utc < previous_time:
            raise ValueError("updated_at_utc cannot precede the last event")
        return self

    @property
    def terminal(self) -> bool:
        return RouteStatus(self.status) in TERMINAL_ROUTE_STATUSES


class RouteStateMachine:
    def __init__(self, state: RouteState):
        # Revalidate here so restored ledgers cannot bypass replay checks.
        self.state = RouteState.model_validate(state.model_dump(mode="json"))

    def transition(
        self,
        target: RouteStatus,
        *,
        actor: str,
        reason: str,
        contract_refs: list[PackageRef | dict[str, Any]] | None = None,
        error_code: ErrorCode | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RouteState:
        current = RouteStatus(self.state.status)
        target = RouteStatus(target)
        if target not in ALLOWED_TRANSITIONS[current]:
            raise ValueError(f"illegal route transition: {current.value} -> {target.value}")
        revision_count = self.state.revision_count
        if target == RouteStatus.REVISION_REQUESTED:
            revision_count += 1
            if revision_count > self.state.max_revisions:
                raise ValueError("specialist revision budget exceeded")
        if target in {RouteStatus.BLOCKED, RouteStatus.FAILED} and error_code is None:
            raise ValueError("blocked/failed route transitions require an error_code")
        if target not in {RouteStatus.BLOCKED, RouteStatus.FAILED} and error_code is not None:
            raise ValueError("error_code is only valid for blocked/failed route transitions")

        event = RouteEvent(
            from_status=current,
            to_status=target,
            actor=actor,
            reason=reason,
            contract_refs=[
                ref if isinstance(ref, PackageRef) else PackageRef.model_validate(ref)
                for ref in (contract_refs or [])
            ],
            revision_count=revision_count,
            error_code=error_code,
            metadata=metadata or {},
        )
        state_payload = self.state.model_dump(mode="json")
        state_payload.update(
            {
                "status": target,
                "revision_count": revision_count,
                "events": [*self.state.events, event],
                "updated_at_utc": event.occurred_at_utc,
            }
        )
        self.state = RouteState.model_validate(state_payload)
        return self.state

    def request_revision_or_block(
        self,
        *,
        actor: str,
        reason: str,
        contract_refs: list[PackageRef | dict[str, Any]] | None = None,
    ) -> RouteState:
        """Request a bounded correction, or deterministically block at the cap."""

        if RouteStatus(self.state.status) != RouteStatus.HANDOFF_VALIDATION:
            raise ValueError("revisions can only be requested during handoff_validation")
        if self.state.revision_count >= self.state.max_revisions:
            return self.transition(
                RouteStatus.BLOCKED,
                actor=actor,
                reason=f"revision budget exhausted: {reason}",
                contract_refs=contract_refs,
                error_code=ErrorCode.BUDGET_EXCEEDED,
            )
        return self.transition(
            RouteStatus.REVISION_REQUESTED,
            actor=actor,
            reason=reason,
            contract_refs=contract_refs,
        )

    def accept_package(self, reference: PackageRef | dict[str, Any], *, specialist: str) -> RouteState:
        """Record an already verified package in central route state."""

        if RouteStatus(self.state.status) != RouteStatus.HANDOFF_VALIDATION:
            raise ValueError("packages can only be accepted during handoff_validation")
        ref = reference if isinstance(reference, PackageRef) else PackageRef.model_validate(reference)
        payload = self.state.model_dump(mode="json")
        payload["accepted_packages"][ref.artifact_type] = ref.model_dump(mode="json")
        payload["specialist_status"][specialist] = "accepted"
        self.state = RouteState.model_validate(payload)
        return self.state

