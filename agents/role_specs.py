"""Authoritative runtime metadata for the four GeoSentinel roles.

The role names and skill namespaces in this module are part of the experiment
snapshot.  Tool objects remain in :mod:`tools`; keeping this metadata import
light lets graph construction and tests inspect boundaries without importing
optional geospatial dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass


COMMON_SKILL_SOURCE = "/skills/common/"
GEE_NTL_DATE_BOUNDARY_SKILL_SOURCE = "/skills/gee-ntl-date-boundary-handling/"


@dataclass(frozen=True, slots=True)
class RoleSpec:
    """Stable description of one role's orchestration boundary."""

    name: str
    display_name: str
    description: str
    tool_group: str
    skill_sources: tuple[str, ...]
    expected_package_type: str
    can_delegate: bool = False


def _role_skills(namespace: str, *shared_sources: str) -> tuple[str, ...]:
    """Build an ordered role Skill surface with optional shared procedures."""
    return (COMMON_SKILL_SOURCE, f"/skills/{namespace}/", *shared_sources)


ROLE_SPECS: dict[str, RoleSpec] = {
    "NTL_Engineer": RoleSpec(
        name="NTL_Engineer",
        display_name="地缘分析师",
        description=(
            "GeoSentinel supervisor and task-truth owner. Frames the geopolitical or "
            "environmental decision question, plans and conditionally routes work, "
            "accepts typed specialist evidence, and synthesizes the final auditable conclusion."
        ),
        tool_group="engineer_tools",
        skill_sources=_role_skills("engineer", GEE_NTL_DATE_BOUNDARY_SKILL_SOURCE),
        expected_package_type="EvidenceReport",
        can_delegate=True,
    ),
    "NTL_Data_Searcher": RoleSpec(
        name="NTL_Data_Searcher",
        display_name="数据助手",
        description=(
            "Geoenvironmental observation specialist for event feeds, administrative "
            "boundaries, remote sensing, Earthdata/GEE, climate, population, infrastructure, "
            "socioeconomic sources, AOI/time resolution, acquisition, QA and provenance; "
            "owns analysis-ready "
            "ObservationPackage production when a typed handoff is requested; it may "
            "return a bounded evidence summary for summary-only assignments."
        ),
        tool_group="data_searcher_tools",
        skill_sources=_role_skills("data_searcher", GEE_NTL_DATE_BOUNDARY_SKILL_SOURCE),
        expected_package_type="ObservationPackage",
    ),
    "NTL_Analyst": RoleSpec(
        name="NTL_Analyst",
        display_name="分析助手",
        description=(
            "Geoenvironmental scientific-analysis specialist for spatial, temporal, raster, "
            "vector, remote-sensing, event-impact and socioeconomic methods, including "
            "nighttime-light evidence; owns contract-bound execution, artifacts, "
            "internal validation, and AnalysisPackage production when a typed handoff "
            "is requested; it may return a bounded summary-only result otherwise."
        ),
        tool_group="analyst_tools",
        skill_sources=_role_skills("analyst"),
        expected_package_type="AnalysisPackage",
    ),
    "NTL_Event_Tracker": RoleSpec(
        name="NTL_Event_Tracker",
        display_name="事件助手",
        description=(
            "Source-bounded event specialist for conflict, disaster, outage, accident, "
            "policy, logistics and recovery tasks; preserves timelines, as-of semantics, "
            "provenance, source "
            "conflicts, coverage limits, and produces an EventContext when a typed "
            "handoff is requested; it may return a bounded source summary otherwise."
        ),
        tool_group="event_tracker_tools",
        skill_sources=_role_skills("event_tracker"),
        expected_package_type="EventContext",
    ),
}


ROLE_SKILL_SOURCES: dict[str, tuple[str, ...]] = {
    name: spec.skill_sources for name, spec in ROLE_SPECS.items()
}

ROLE_DISPLAY_NAMES: dict[str, str] = {
    name: spec.display_name for name, spec in ROLE_SPECS.items()
}


_ROLE_ALIASES = {
    "engineer": "NTL_Engineer",
    "ntl_engineer": "NTL_Engineer",
    "data_searcher": "NTL_Data_Searcher",
    "ntl_data_searcher": "NTL_Data_Searcher",
    "analyst": "NTL_Analyst",
    "ntl_analyst": "NTL_Analyst",
    "event_tracker": "NTL_Event_Tracker",
    "ntl_event_tracker": "NTL_Event_Tracker",
}


def get_role_spec(role: str) -> RoleSpec:
    """Resolve a canonical role name or a conservative lowercase alias."""

    raw = str(role or "").strip()
    if raw in ROLE_SPECS:
        return ROLE_SPECS[raw]
    normalized = raw.lower().replace("-", "_").replace(" ", "_")
    canonical = _ROLE_ALIASES.get(normalized)
    if canonical is None:
        raise KeyError(f"Unknown GeoSentinel role: {role!r}")
    return ROLE_SPECS[canonical]


SPECIALIST_ROLE_NAMES = (
    "NTL_Data_Searcher",
    "NTL_Analyst",
    "NTL_Event_Tracker",
)


__all__ = [
    "COMMON_SKILL_SOURCE",
    "GEE_NTL_DATE_BOUNDARY_SKILL_SOURCE",
    "ROLE_SKILL_SOURCES",
    "ROLE_DISPLAY_NAMES",
    "ROLE_SPECS",
    "SPECIALIST_ROLE_NAMES",
    "RoleSpec",
    "get_role_spec",
]
