from __future__ import annotations

import posixpath
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Literal

from ssl_compat import configure_outbound_ssl

configure_outbound_ssl()

from deepagents import (
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    create_deep_agent,
    register_harness_profile,
)
from deepagents.backends import (
    BackendProtocol,
    CompositeBackend,
    FilesystemBackend,
    StateBackend,
)
from deepagents.backends.protocol import (
    DeleteResult,
    EditResult,
    FileDownloadResponse,
    FileUploadResponse,
    WriteResult,
)
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.memory import MemoryMiddleware
from langchain_core.messages import BaseMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from langchain_openai import ChatOpenAI as _LangChainChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from agents.NTL_Analyst import system_prompt_analyst
from agents.NTL_Data_Searcher import hierarchical_system_prompt_data_searcher
from agents.NTL_Event_Tracker import system_prompt_event_tracker
from agents.role_specs import ROLE_SPECS
from model_config import get_api_model_name, get_base_url, get_model_config
from orchestration.contract_tools import (
    record_route_transition_tool,
    save_analysis_package_tool,
    save_event_context_tool,
    save_evidence_report_tool,
    save_observation_package_tool,
    save_task_plan_tool,
    validate_contract_tool,
)
from runtime_limits import consume_model_call
from storage_manager import current_thread_id, storage_manager
from tools import (
    analyst_tools,
    data_searcher_tools,
    engineer_tools,
    event_tracker_tools,
    single_agent_tools,
)


ArchitectureMode = Literal["full", "single_agent"]
ARCHITECTURE_MODES: tuple[ArchitectureMode, ...] = ("full", "single_agent")
ResourceProfile = Literal["standard", "tools_prompt_only"]
RESOURCE_PROFILES: tuple[ResourceProfile, ...] = ("standard", "tools_prompt_only")


def _text_only_message(message: BaseMessage) -> BaseMessage:
    """Replace unsupported multimodal blocks before calling text-only DeepSeek APIs."""

    content = message.content
    if not isinstance(content, list):
        return message
    parts: list[str] = []
    changed = False
    for block in content:
        if isinstance(block, str):
            parts.append(block)
            continue
        if not isinstance(block, dict):
            parts.append(f"[Unsupported non-text content omitted: {type(block).__name__}]")
            changed = True
            continue
        block_type = str(block.get("type") or "unknown")
        if block_type in {"text", "input_text", "output_text"} and isinstance(
            block.get("text"), str
        ):
            parts.append(str(block["text"]))
            continue
        if block_type in {"image", "image_url", "input_image"}:
            parts.append(
                "[Image content omitted for the text-only model; use the persisted file and "
                "registered geospatial inspection tools for validation.]"
            )
        else:
            parts.append(f"[Unsupported content block omitted: {block_type}]")
        changed = True
    if not changed:
        return message
    return message.model_copy(update={"content": "\n".join(part for part in parts if part)})


class ChatOpenAI(_LangChainChatOpenAI):
    """ChatOpenAI adapter that keeps DeepSeek request histories text-only."""

    def _get_request_payload(
        self,
        input_: Any,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        messages = self._convert_input(input_).to_messages()
        text_only = [_text_only_message(message) for message in messages]
        return super()._get_request_payload(text_only, stop=stop, **kwargs)

    def _generate(self, *args: Any, **kwargs: Any):
        # The same adapter instance is used by the supervisor and every
        # specialist, so this budget counts the whole research run rather than
        # only the root agent's turns.
        consume_model_call()
        return super()._generate(*args, **kwargs)

    async def _agenerate(self, *args: Any, **kwargs: Any):
        consume_model_call()
        return await super()._agenerate(*args, **kwargs)

checkpointer = MemorySaver()
SKILLS_ROOT = Path(__file__).resolve().parent / ".ntl-gpt" / "skills"
RUNTIME_MEMORY_TEMPLATE = SKILLS_ROOT.parent / "AGENTS.md"
RUNTIME_MEMORY_SOURCE = "/memories/AGENTS.md"
RUNTIME_MEMORY_SYSTEM_PROMPT = """<agent_memory>
{agent_memory}

</agent_memory>

<memory_guidelines>
    This is versioned GeoSentinel（地缘环境智能计算平台） architecture reference context loaded at startup.
    Treat it as read-only reference material. Do not edit or evolve this file
    during a benchmark run; workflow changes belong in the active prompts,
    Skills, and source code. Prefer the user's request and verified tool
    evidence when they conflict with this reference.
</memory_guidelines>
"""
DEEPAGENTS_HARNESS_MODEL_SPECS = (
    "openai:deepseek-v4-flash",
    "openai:deepseek-v4-pro",
)
DEEPAGENTS_HARNESS_MODEL_SPEC = DEEPAGENTS_HARNESS_MODEL_SPECS[0]
DEEPAGENTS_HARNESS_API_MODELS = tuple(
    spec.partition(":")[2] for spec in DEEPAGENTS_HARNESS_MODEL_SPECS
)
NTL_TASK_DESCRIPTION = """Delegate one self-contained natural-language task to exactly one registered GeoSentinel specialist.

Available specialists:
{available_agents}

Set `subagent_type` to one listed specialist name only. In `description`, state the objective, scientific scope, known inputs or parent package handles, and the requested result mode: default to `summary_only` when a concise evidence-based result plus real workspace paths is enough for the next step; request `typed_package` only when downstream computation genuinely needs structured fields that cannot be carried in that normal result. Include acceptance checks and limitations in normal prose. Do not request a typed package merely to make a route look complete. Copy each known workspace-relative path verbatim (never rename, relocate, or replace it with an inferred alias); pass a returned `package/<token>` handle verbatim and never reconstruct a hidden contract path. Describe each known local input by workspace-relative path and semantic role/media type only; never include or request its SHA-256 or byte count because typed save binds that identity. Do not serialize an AssignmentEnvelope or HandoffEnvelope. Do not include benchmark Gold or evaluator material. Runtime identity, timestamps, local artifact identity, and assignment/handoff records are system-managed; never try to discover or supply them. This is a tool-calling turn: invoke `task` directly when delegation is required; never send a prose-only "I will delegate/save next" response.
"""

NTL_HARNESS_PROFILE = HarnessProfile(
    tool_description_overrides={"task": NTL_TASK_DESCRIPTION},
    general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
)
for _harness_model_spec in DEEPAGENTS_HARNESS_MODEL_SPECS:
    register_harness_profile(_harness_model_spec, NTL_HARNESS_PROFILE)

ENGINEER_CONTRACT_TOOLS = (
    save_task_plan_tool,
    save_evidence_report_tool,
    validate_contract_tool,
    record_route_transition_tool,
)
DATA_SEARCHER_CONTRACT_TOOLS = (save_observation_package_tool, validate_contract_tool)
ANALYST_CONTRACT_TOOLS = (save_analysis_package_tool, validate_contract_tool)
EVENT_TRACKER_CONTRACT_TOOLS = (save_event_context_tool, validate_contract_tool)
SINGLE_AGENT_CONTRACT_TOOLS = (
    save_task_plan_tool,
    save_event_context_tool,
    save_observation_package_tool,
    save_analysis_package_tool,
    save_evidence_report_tool,
    validate_contract_tool,
    record_route_transition_tool,
)


def _prompt_text(value: Any) -> str:
    content = getattr(value, "content", value)
    return str(content)


class _KnowledgeBaseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(description="Question plus brief NTL context.")
    response_mode: Literal["theory", "workflow"] = "theory"
    locale: str = "en"
    need_citations: bool = True
    skill_gap_confirmed: bool = False


def _invoke_knowledge_base(
    query: str,
    response_mode: str = "theory",
    locale: str = "en",
    need_citations: bool = True,
    skill_gap_confirmed: bool = False,
    config: RunnableConfig | None = None,
):
    # Chroma opens its persistent stores during module import. Keep that side
    # effect out of graph construction and provider-free tests; import only if
    # the tested agent actually requests supplemental knowledge.
    from tools.NTL_Knowledge_Base_Searcher import NTL_Knowledge_Base

    try:
        return NTL_Knowledge_Base.invoke(
            {
                "query": query,
                "response_mode": response_mode,
                "locale": locale,
                "need_citations": need_citations,
                "skill_gap_confirmed": skill_gap_confirmed,
            },
            config=config,
        )
    except Exception as exc:
        # Supplemental retrieval must not terminate an otherwise executable
        # benchmark task.  Do not expose the provider payload to the model;
        # it can proceed with the registered tools and role Skills instead.
        return {
            "status": "knowledge_unavailable",
            "error_type": type(exc).__name__,
            "message": "Supplemental knowledge retrieval is unavailable for this call.",
            "fallback": "Use the registered task-specific tool and active role Skills.",
        }


def _knowledge_base_tool():
    return StructuredTool.from_function(
        func=_invoke_knowledge_base,
        name="NTL_Knowledge_Base",
        description=(
            "Supplement local NTL skills with grounded theory or literature context. "
            "Use workflow mode only after relevant role skills lack coverage."
        ),
        args_schema=_KnowledgeBaseInput,
    )


def _build_llm(
    model_name: str,
    api_key: str,
    request_timeout_s: int,
    *,
    parallel_tool_calls: bool | None = None,
):
    model_config = get_model_config(model_name)
    api_model = get_api_model_name(model_name)
    if model_config.provider != "deepseek":
        raise ValueError(f"Unsupported frontend model provider: {model_config.provider}")
    base_url = get_base_url(model_name)
    if not api_key or not base_url:
        raise RuntimeError(
            f"{model_config.api_key_env} and {model_config.base_url_env} are required for {api_model}."
        )
    model_kwargs: dict[str, Any] = {}
    if parallel_tool_calls is not None:
        # The pinned ChatOpenAI.bind_tools surface accepts this OpenAI-
        # compatible request option. Full runs serialize dependent package
        # handoffs so a save cannot race its downstream use.
        model_kwargs["parallel_tool_calls"] = parallel_tool_calls
    # Bound one model turn. Long subagent turns (large tool schemas + skills)
    # can otherwise stream for many minutes and stall the run UI inside a
    # retry loop; 12k tokens covers argument construction and analysis text.
    return ChatOpenAI(
        api_key=SecretStr(api_key),
        base_url=base_url,
        model=api_model,
        temperature=0,
        timeout=request_timeout_s,
        max_retries=3,
        max_tokens=12000,
        model_kwargs=model_kwargs,
    )


def _validate_skill_sources(skill_sources: Sequence[str]) -> None:
    missing: list[str] = []
    for src in sorted(set(skill_sources)):
        if not src.startswith("/skills/"):
            missing.append(f"{src} -> role skill sources must be mounted below /skills/")
            continue
        rel = src[len("/skills/") :].strip("/")
        source_dir = SKILLS_ROOT / rel
        if not source_dir.exists() or not source_dir.is_dir():
            missing.append(f"{src} -> missing directory: {source_dir}")
            continue
        has_direct_skill = (source_dir / "SKILL.md").is_file()
        has_child_skill = any((p / "SKILL.md").exists() for p in source_dir.iterdir() if p.is_dir())
        if not has_direct_skill and not has_child_skill:
            missing.append(f"{src} -> no SKILL.md at or beneath {source_dir}")
    if missing:
        raise ValueError("Invalid Deep Agents skill sources:\n" + "\n".join(missing))


def _is_protected_evidence_path(file_path: str) -> bool:
    """Return whether a virtual path belongs to the internal run-evidence tree.

    Deep Agents' generic filesystem middleware and the typed contract tools use
    different persistence paths: the former goes through this backend, while
    the latter writes via ``storage_manager`` after schema validation.  Protect
    both the canonical ``/outputs`` tree and its documented
    ``/data/processed`` alias so an agent cannot overwrite or spoof contracts,
    handoffs, decisions, system-authored transfer records, or route checkpoints
    with ``write_file``/``edit_file``. The model-facing filesystem cannot read
    this identity-bearing audit tree; typed tools and the runner own access.
    """

    raw = str(file_path or "").replace("\\", "/")
    normalized = posixpath.normpath("/" + raw.lstrip("/"))
    parts = tuple(part.casefold() for part in normalized.split("/") if part)
    if parts[:2] == ("outputs", "runs"):
        evidence_parts = parts[2:]
    elif parts[:3] == ("data", "processed", "runs"):
        evidence_parts = parts[3:]
    else:
        return False
    return any(
        part
        in {
            "contracts",
            "handoffs",
            "decisions",
            "assignment_records",
            "handoff_records",
        }
        or part.startswith("route_state")
        for part in evidence_parts
    )


def _normalize_virtual_path(path: str) -> str:
    raw = str(path or "").replace("\\", "/")
    return posixpath.normpath("/" + raw.lstrip("/"))


class ContextFilesystemBackend(BackendProtocol):
    """Deep Agents 0.7.5 backend whose filesystem root follows ContextVar state."""

    def __init__(self, root_resolver: Callable[[], Path]) -> None:
        self._root_resolver = root_resolver

    def _backend(self) -> FilesystemBackend:
        root = Path(self._root_resolver()).resolve()
        root.mkdir(parents=True, exist_ok=True)
        return FilesystemBackend(root_dir=root, virtual_mode=True)

    def ls(self, path: str):
        return self._backend().ls(path)

    def read(self, file_path: str, offset: int = 0, limit: int = 2000):
        return self._backend().read(file_path, offset=offset, limit=limit)

    def grep(self, pattern: str, path: str | None = None, glob: str | None = None, *, max_count: int | None = None):
        return self._backend().grep(pattern, path=path, glob=glob, max_count=max_count)

    def glob(self, pattern: str, path: str | None = None):
        return self._backend().glob(pattern, path=path)

    def write(self, file_path: str, content: str) -> WriteResult:
        return self._backend().write(file_path, content)

    def edit(self, file_path: str, old_string: str, new_string: str, replace_all: bool = False) -> EditResult:
        return self._backend().edit(file_path, old_string, new_string, replace_all=replace_all)

    def delete(self, file_path: str) -> DeleteResult:
        return self._backend().delete(file_path)

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        return self._backend().upload_files(files)

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        return self._backend().download_files(paths)


def _workspace_subdir(name: str) -> Path:
    workspace = storage_manager.get_workspace(str(current_thread_id.get() or "debug").strip() or "debug")
    return workspace / name


def _seed_runtime_memory() -> Path:
    """Materialize the versioned startup memory in the thread memory route."""

    if not RUNTIME_MEMORY_TEMPLATE.is_file():
        raise FileNotFoundError(f"startup memory template is missing: {RUNTIME_MEMORY_TEMPLATE}")
    directory = _workspace_subdir("memory")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "AGENTS.md"
    if not target.exists():
        target.write_bytes(RUNTIME_MEMORY_TEMPLATE.read_bytes())
    return target


def _runtime_memory_middleware(backend: CompositeBackend = None) -> MemoryMiddleware:
    return MemoryMiddleware(
        backend=backend or RUNTIME_BACKEND,
        sources=[RUNTIME_MEMORY_SOURCE],
        system_prompt=RUNTIME_MEMORY_SYSTEM_PROMPT,
    )


def _build_skills_backend() -> CompositeBackend:
    return CompositeBackend(
        # The project tree is the catch-all skill mount so listing /skills works
        # outside graph execution too. Explicit role routes keep source paths
        # stable; native permissions still hide every namespace except common
        # plus the active role (or the Single-Agent union).
        default=FilesystemBackend(root_dir=SKILLS_ROOT, virtual_mode=True),
        routes={
            f"/{namespace}/": FilesystemBackend(root_dir=SKILLS_ROOT / namespace, virtual_mode=True)
            for namespace in ("common", "engineer", "data_searcher", "analyst", "event_tracker")
        },
    )


def _build_runtime_backend(*, skills_enabled: bool = True) -> CompositeBackend:
    inputs = ContextFilesystemBackend(lambda: _workspace_subdir("inputs"))
    outputs = ContextFilesystemBackend(lambda: _workspace_subdir("outputs"))
    # Thread memory remains an ordinary runtime-state route for export
    # manifests and failed-run records. The one versioned AGENTS.md source is
    # seeded per thread and loaded through the explicit MemoryMiddleware; it is
    # denied to model writes while the rest of /memories/ remains available.
    memories = ContextFilesystemBackend(lambda: _workspace_subdir("memory"))
    routes: dict[str, BackendProtocol] = {
        "/inputs/": inputs,
        "/outputs/": outputs,
        "/data/raw/": inputs,
        "/data/processed/": outputs,
        "/memories/": memories,
        "/shared/": FilesystemBackend(root_dir=storage_manager.shared_dir, virtual_mode=True),
    }
    if skills_enabled:
        routes["/skills/"] = _build_skills_backend()
    return CompositeBackend(default=StateBackend(), routes=routes)


RUNTIME_BACKEND = _build_runtime_backend()


def filesystem_permissions(
    skill_sources: Sequence[str], *, memory_access: bool, skills_enabled: bool = True
) -> list[FilesystemPermission]:
    sources = (
        tuple(dict.fromkeys(_normalize_virtual_path(source) for source in skill_sources))
        if skills_enabled
        else ()
    )
    # The whole internal audit tree is owned exclusively by typed contract
    # tools. Denying reads as well as writes prevents runtime identity from
    # leaking through directory names or persisted envelope fields.
    protected = [
        "/outputs/runs{,/**}",
        "/data/processed/runs{,/**}",
    ]
    readable = [
        "/",
        "/inputs{,/**}",
        "/outputs{,/**}",
        "/data/raw{,/**}",
        "/data/processed{,/**}",
        "/shared{,/**}",
        "/large_tool_results{,/**}",
        "/conversation_history{,/**}",
    ]
    if skills_enabled:
        readable.append("/skills")
        readable.extend(f"{source.rstrip('/')}{'{,/**}'}" for source in sources)
    writable = ["/outputs{,/**}", "/data/processed{,/**}"]
    if memory_access:
        readable.append("/memories{,/**}")
        writable.append("/memories{,/**}")
    permissions = [
        FilesystemPermission(operations=["read", "write"], paths=protected, mode="deny"),
        FilesystemPermission(
            operations=["read", "write"], paths=["/**/.*{,/**}"], mode="deny"
        ),
        FilesystemPermission(operations=["read"], paths=readable, mode="allow"),
        FilesystemPermission(operations=["write"], paths=writable, mode="allow"),
        FilesystemPermission(operations=["read", "write"], paths=["/**"], mode="deny"),
    ]
    # Startup architecture memory is loaded through MemoryMiddleware but must
    # not become a mutable learning channel during a benchmark run. The normal
    # /memories/ route remains writable for runtime manifests and failed-run
    # records.
    permissions.insert(
        2,
        FilesystemPermission(
            operations=["write"], paths=[RUNTIME_MEMORY_SOURCE], mode="deny"
        ),
    )
    return permissions


def filesystem_runtime_descriptor(
    skill_sources: Sequence[str], *, memory_access: bool, skills_enabled: bool = True
) -> dict[str, Any]:
    rules = filesystem_permissions(
        skill_sources, memory_access=memory_access, skills_enabled=skills_enabled
    )
    routes = {
        "/inputs/": "ContextFilesystemBackend(thread.inputs)",
        "/outputs/": "ContextFilesystemBackend(thread.outputs)",
        "/data/raw/": "ContextFilesystemBackend(thread.inputs)",
        "/data/processed/": "ContextFilesystemBackend(thread.outputs)",
        "/memories/": "ContextFilesystemBackend(thread.memory)",
        "/shared/": "FilesystemBackend(read-only by permissions)",
    }
    if skills_enabled:
        routes["/skills/"] = "CompositeBackend(project skill tree plus explicit role namespaces)"
    return {
        "backend_type": "CompositeBackend(default=StateBackend)",
        "routes": routes,
        "skills_enabled": skills_enabled,
        "internal_state_paths": ["/large_tool_results/", "/conversation_history/"],
        "permissions": [
            {"operations": list(rule.operations), "paths": list(rule.paths), "mode": rule.mode}
            for rule in rules
        ],
    }


def _full_system_prompt() -> str:
    return f"""GeoSentinel（地缘环境智能计算平台）地缘环境智能计算总协调器（user-facing name: `地缘分析师`; internal runtime identifier: NTL_Engineer）。

Product mission:
- GeoSentinel is a geoenvironmental intelligence platform for geopolitical events, conflict, earthquake, wildfire, flood and other disasters, climate and ecosystems, land use, population and settlements, infrastructure, logistics and supply chains, socioeconomic indicators, administrative geography, and remote-sensing evidence.
- Resolve product/date/AOI semantics explicitly before acquisition or analysis, and preserve the user's requested spatial and temporal scope.
- Nighttime light is one evidence layer, not the product identity and not a mandatory route. Match evidence and specialists to the user's actual decision question.
- GeoSentinel correction operations are opt-in. Use standard product values by default; do not apply viewing-angle, seasonal, radiometric, cross-sensor, atmospheric, geometric, or other correction/harmonization unless the user explicitly requests that correction or explicitly names a method whose required workflow includes it. A model-generated plan alone cannot authorize correction.
- Separate observed facts, computed results, analytical inference, uncertainty, and missing evidence. Preserve source, time, place, method, and artifact provenance.
- Use Simplified Chinese for user-facing communication while preserving exact technical identifiers where compatibility requires them.

System boundary and delegation contract:
- You are the only internal supervisor and the owner of task truth, routing, handoff acceptance, and final EvidenceReport.
- The only specialists are NTL_Data_Searcher, NTL_Analyst, and NTL_Event_Tracker.
- Specialists never call one another. Delegate sequentially through the `task` tool. Validate a returned typed package when the assignment requested `typed_package`; accept and record a bounded `summary_only` result when no downstream package is needed.
- NTL_Data_Searcher（数据助手）owns event feeds, boundaries, remote sensing, Earthdata/GEE, climate, population, infrastructure and socioeconomic observations, plus AOI/time validity, QA/scaling, acquisition, standard preprocessing, provenance, and ObservationPackage when a typed handoff is requested. For a metadata, availability, or source-confirmation task, it may return a concise evidence summary without creating a package.
- ObservationPackage query time is system-managed: after a successful full geodata inspection, the runtime injects its completion time. Never ask a model to supply or guess it.
- NTL_Analyst（分析助手）owns specialized geoenvironmental methods: spatial and temporal statistics, raster/vector analysis, remote-sensing interpretation, event-impact and recovery assessment, climate/ecosystem indicators, population and infrastructure exposure, socioeconomic modeling, visualization, nighttime-light evidence, internal validation, and AnalysisPackage.
- NTL_Event_Tracker（事件助手）owns authorized geopolitical conflict, disaster, outage, accident, policy, logistics and recovery sources, source-bounded timelines, conflicts, as-of semantics, and EventContext.
- Use the bounded direct-execution fast path for simple tasks with existing verified inputs, settled semantics, a mature single-stage operation, and an immediate deterministic check. You may use `execute_geospatial_script_tool` for ordinary local code: deterministic format conversion, file preparation, small aggregations, and report/figure assembly. The script contract and one final validation still apply.
- Do not route routine coding merely because it is code, and do not route complex work merely because it is complex. Engineer owns ordinary tabular statistics, generic GIS, file conversion, deterministic transformations, plotting, and report synthesis. Route to NTL_Analyst when the task requires a specialized geoenvironmental method or scientific interpretation, including remote-sensing indices/classification, spatial exposure, event-impact comparison, climate/ecosystem analysis, nighttime-light methods, or domain-specific statistical modeling. You may prepare clearly defined inputs first and may perform lightweight post-handoff formatting or synthesis yourself.
- Method fidelity takes precedence over convenience: when the request names a sensor-specific index, cited threshold/classification, or other declared method and a matching registered dedicated tool is available, inspect that tool and preserve its documented formula, threshold, reducer, and units. Do not call supplemental knowledge retrieval merely to rediscover a method contract that the dedicated tool already exposes. A generic script may add a missing output or validation, but never silently substitute an alternate formula, threshold scan, or proxy. Include this method contract in a specialist assignment.
- Treat an explicitly requested model-selection criterion as part of the method contract. When the task says to choose by stated metrics or curve form, report that criterion's winner; explain a near-tie or parsimony concern as a limitation, not as a replacement decision rule. A metric tie is exact unless the task declares a tolerance: any lower finite RMSE wins, even when its advantage is small.
- Exact registered-method dispatch: for a Liu-style electricity-access proxy with 0/1 calibration labels and a population raster, use `Detect_Electrified_Areas_by_Thresholding` and take the extrema-based threshold from its metadata; for SVM urban or built-up-area extraction, use `Detect_Urban_Area_by_SVM` before any generic script. These are execution methods, not optional examples.
- For a staged local raster-plus-boundary request for a standard zonal nighttime-light metric supported by `NTL_raster_statistics`, route directly to NTL_Analyst and require that tool as its primary operation. Its source-grid, pixel-centre, NoData, and area defaults are the method contract; a generic script may only format, map, or validate its output.
- A ready specialist package is not accepted until you check schema, the system-bound local artifact identity returned by typed save/validate, assignment scope, scientific semantics, validation, and limitations. A summary-only result is accepted through the native task telemetry and evidence text when the TaskPlan did not require a package; do not probe a package or block solely because no package handle exists in that mode.
- Apply minimum-sufficient validation. Validate only the exact package handle returned by the current specialist, once, against the explicit TaskPlan acceptance checks. Do not search for alternate packages, repeat a successful validation, or request a revision for an unrequested sensitivity analysis, a near-tie, or a limitation when the requested primary method and outputs are valid; report those points as limitations instead.
- Never read benchmark Gold or evaluator material. Do not accept post-run repair suggestions during the tested run.
- An intent, route recommendation, or plan is never a task result. Before a final answer, obtain task evidence through a substantive registered tool call, a verified staged-input read, or an accepted specialist result. For remote acquisition or a named-boundary request, invoke `task` (or the authorized direct tool) in that same response; never finish after merely saying that you will do so.
- Scientific execution is primary. Typed contract tools and route records capture run-level audit evidence. Save one concise EvidenceReport when the task requires a durable audit package; an optional report save must never block an otherwise complete direct answer.
- Form one complete TaskPlan at task start. Persist it only when a typed handoff or an explicit task requirement needs it, and checkpoint legal route transitions only when using the routed workflow. A returned `package/<token>` handle means persistence succeeded: retain it and do not save another TaskPlan unless the scientific plan genuinely changes. Preserve every returned workspace-relative artifact path and `package/<token>` handle verbatim across the next handoff; never rename, relocate, infer, reconstruct, or re-save it solely to make a downstream description look cleaner. A `save_*` response with `status: failed` is an audit diagnostic, not a package handle: do not validate or link it, and do not let an optional TaskPlan/EvidenceReport save block a scientifically complete answer. Never create new artifact IDs to probe the schema. Runtime IDs, case identity, system timestamps, and local input/output SHA-256 plus byte counts are injected or bound by the system and are intentionally absent from model-facing tool inputs; producer is schema-fixed to the active role. For local artifacts, models declare only workspace-relative path and semantic role/media type. Never compute, guess, copy, null-fill, echo, or override system-owned identity; refer to saved packages only through the opaque references returned by typed contract tools.
- Never use generic filesystem mutation (`write_file` or `edit_file`) on the internal contracts or route-state tree. Never create or modify system-owned assignment/handoff records. Use only typed `save_*`, `validate_contract`, and `record_route_transition` tools for model-owned contract work.
- Every `task` delegation description must be a self-contained natural-language request: give the selected specialist the objective, scientific scope, known inputs or parent package handles, result mode (`typed_package` or `summary_only`), acceptance checks, and limitations. For each local input, provide its workspace-relative path and semantic role/media type only; do not pass or request SHA-256/bytes. Do not ask for or emit an AssignmentEnvelope or HandoffEnvelope. The runtime records the native task call and return automatically.
- Route only as needed: direct fast path; Engineer→Data Searcher; Engineer→Analyst when analysis-ready inputs are already staged, their identity will be system-bound during save, and `observation_required=false`; Engineer→Event Tracker for a source-bounded event-context-only task; Engineer→Data Searcher→Analyst; or Engineer→Event Tracker→Data Searcher→Analyst. Record why unused specialists were skipped. Do not delegate ordinary local code execution just to make the route look multi-agent.
- For a disaster, accident, outage, or recovery request, first read `/skills/common/disaster-event-observation-workflow/SKILL.md`. Follow its minimum event-to-observation-to-analysis order, but do not create a package, buffer, control, or extra specialist leg that the actual task does not need.
- For a frozen-fixture task whose named inputs are already staged and whose primary operation is a named Analyst method, treat those inputs as analysis-ready: set `observation_required=false` and delegate directly to NTL_Analyst. Do not add a Data Searcher leg merely to re-inspect the same files, catalogue their already-declared product, or restate provenance. Use Data Searcher only when acquisition, standard preprocessing, live availability, or an explicit observation contract is genuinely needed.
- Direct Analyst capability index for staged inputs: raster/zonal statistics, spatial exposure, time-series and event-window comparison, seasonal adjustment, persistence classification, DMSP--VIIRS harmonization, trends, anomalies, SVM or threshold urban extraction, Otsu road extraction, electricity-access detection, DEI estimation, visualization and local model fitting are Analyst methods. They are not Data Searcher jobs. If the request explicitly names one of their registered tools or methods, delegate to NTL_Analyst rather than asking Data Searcher to search the same local fixture.
- Correction is never a default preprocessing or analysis step. Route seasonal adjustment, cross-sensor harmonization, viewing-angle correction, radiometric correction, or any comparable correction only when the user explicitly asked for it or explicitly selected a method that requires it; otherwise preserve the standard product values and report that no extra correction was applied.
- Named Chinese administrative-boundary, POI, address, or landmark-coordinate requests are acquisition tasks: route them to NTL_Data_Searcher so it can use the runtime-callable `get_administrative_division_data`, `poi_search_tool`, or `geocode_tool`. Do not treat the absence of staged input files as evidence that those network tools are unavailable.
- For live GEE or other remote-source acquisition tasks, an empty `/inputs/` directory is expected at task start. For a retrieval-only request, route the request to NTL_Data_Searcher in `summary_only` mode and accept its verified input paths, coverage, source/tool, and limitations; do not create a TaskPlan or ObservationPackage purely for audit. Request a typed ObservationPackage only when a later NTL analysis genuinely needs its structured observation fields. Use `needs_input` only when the task explicitly requires a user-supplied file or a credential that the current runtime genuinely lacks.
- Acquisition tools that persist source files in the current workspace use `inputs/<filename>` targets; do not ask them to write `outputs/...`. The specialist may reference those inputs in its ObservationPackage, while Engineer-owned derived reports remain under `outputs/`.
- For a small raster composite explicitly requested by the user, Data Searcher may retrieve daily inputs and call `NTL_composite_local_tool` once. A request for separate daily layers is not a composite request: retrieve and validate those layers, then answer without an unnecessary aggregate. For an uncorrected city/province/county daily VNP46A2 ANTL table, route the bounded `NTL_daily_antl_statistics` executor to Analyst rather than accepting a generic server-side blueprint as a completed result. It accepts named administrative AOIs only: never pass an event-buffer radius or coordinate as its `scale_level` or `study_area`. For a fixture-only event-buffer task with a verified staged VNP46A2 statistics table, route directly to Analyst and derive the requested comparison from that authorized GEE snapshot; for a live arbitrary-buffer task without a registered arbitrary-AOI executor, report the capability boundary rather than misparameterizing the city tool. For an official VNP46A1 request that requires at-sensor radiance or `UTC_Time`, route directly to Data Searcher and require its registered `official_vnp46a1_h5_tool` with a bounded WGS84 bbox or one ISO3 country and `include_utc_time=true`; never substitute VNP46A2 or a GEE-only result. For a VNP46A2 viewing-angle or angle-effect correction, route directly to Data Searcher and require its registered `VNP46A2_angular_correction_tool`; do not substitute the uncorrected daily-ANTL executor. That tool returns corrected daily statistics and method metadata server-side. Request a persistent Earth Engine asset only when the user explicitly asks for one.
- Treat data-channel dates explicitly: a `gee_catalog` date is the Earth Engine collection's visible extent, while a `nasa_earthdata_cmr_laads` date is an official NASA granule date; ingestion latency can make them differ by days. For an official/latest NASA request, route Data Searcher to the NASA channel; for an explicitly GEE request, report only the GEE date; for an unqualified recent request, require separate channel rows with `query_executed_at_utc` and never merge the dates.
- A specialist returns normally through the native `task` result with status, an exact opaque package handle when one was saved, an evidence-based summary, validation verdict, and limitations or error. A returned package handle is persistence success; do not ask the specialist to resave the same package. Preserve its exact workspace-relative paths and opaque handle in the next handoff, rather than renaming them or saving an equivalent duplicate. A summary-only result is valid when the assignment explicitly chose that mode and no downstream package is required. A genuinely blocked/failed specialist may return without a package or handle. After a specialist returns, first transition to `handoff_validation` exactly once and inspect the native result; validate only its exact ready package handle once when the assignment requested `typed_package`, otherwise record the summary and proceed without package probing. Request a bounded revision only for a concrete defect in the requested method, required output, or contract; never delegate a checksum-only retry or an optional robustness exercise. Then transition to `synthesis` once and continue. A successful route transition must not be repeated. The runtime, not the model, standardizes assignment/handoff process records.
- For a summary-only acquisition (for example, a geocoded place or a live catalog lookup), the specialist's returned summary is the scientific result: return it with the requested value, source/tool, retrieval status, and limitation. Do not invent an intermediate package or EvidenceReport just to satisfy an audit gate.
- One normal scientifically successful Event Tracker task must write/inspect the requested source-bounded artifact and save/validate its ready EventContext before returning when `typed_package` was requested. For `summary_only` source confirmation, return the bounded source-grounded summary directly. Do not split that work across a second delegation merely to obtain local artifact identity.
- When route state is used, transition to `synthesis` and then `completed` after required scientific work. You may save the final EvidenceReport once when it is straightforward, but it is not a completion prerequisite. Do not leave a routed run in `handoff_validation` or `synthesis` when an actual required scientific action remains.
- A model response with no tool call ends the run. When a required scientific operation, typed handoff, or route transition remains, call that next tool in the same response rather than narrating a future action. Do not write “let me save”, “I will save”, or “next I will” without invoking that tool in the same response. If the scientific work is complete and only audit persistence or a repeated check remains, return the direct answer instead. In this rule, “stop” means stop checking and either advance to a required scientific action or finish the answer.
- Once required scientific work and outputs are complete, emit the direct answer with real workspace-relative output paths, validation, and limitations. Do not wait for or retry an optional EvidenceReport save.

Workspace protocol:
- Read user or staged data only from `/inputs/`; write task artifacts only to `/outputs/`.
- Earth Engine runtime/billing-project selection and initialization are system-managed by the current run. Never request, guess, or override that billing project, and never start interactive authentication; full source/output asset IDs may still be required by an explicit resource contract. Use the registered bounded GEE tools and report classified runtime failures.
- Declare local artifacts by workspace-relative path plus semantic role/media type only. Typed save resolves the exact task-workspace files and binds SHA-256/bytes; absence of model-side checksum tooling is not a reason to block or fail.
- `/shared/` is read-only; `/memories/` stores thread memory. Never inspect environment variables, runner telemetry, hidden control files, or host paths. Never expose credentials or absolute local paths.
"""


def _single_agent_prompt() -> str:
    return f"""GeoSentinel single-context baseline（user-facing name: `地缘分析师`; internal runtime identifier: NTL_Engineer）。

Complete the task in one context with the same data, tools, procedural knowledge, output contracts, and scientific guardrails as the Full system. You perform planning, event-context work, observation preparation, task-specific analysis, validation, and evidence synthesis yourself.

- For a disaster, accident, outage, or recovery request, first read `/skills/common/disaster-event-observation-workflow/SKILL.md` and apply the same bounded evidence-to-observation-to-analysis order within this one context. Do not claim that a specialist performed any part of it.

Critical comparison rules:
- No delegation or inter-agent handoff is available. Do not claim that another role performed work.
- Correction operations are opt-in: keep standard product values unless the user explicitly requests a correction or explicitly names a method whose required workflow includes it. Do not infer correction from the event type, sensor, data age, or a model-authored plan.
- Method fidelity takes precedence over convenience: when the request names a sensor-specific index, cited threshold/classification, or other declared method and a matching registered dedicated tool is available, inspect that tool and preserve its documented formula, threshold, reducer, and units. Do not call supplemental knowledge retrieval merely to rediscover a method contract that the dedicated tool already exposes. A generic script may add a missing output or validation, but never silently substitute an alternate formula, threshold scan, or proxy.
- Treat an explicitly requested model-selection criterion as part of the method contract. When the task says to choose by stated metrics or curve form, report that criterion's winner; explain a near-tie or parsimony concern as a limitation, not as a replacement decision rule. A metric tie is exact unless the task declares a tolerance: any lower finite RMSE wins, even when its advantage is small.
- Exact registered-method dispatch: for a Liu-style electricity-access proxy with 0/1 calibration labels and a population raster, use `Detect_Electrified_Areas_by_Thresholding` and take the extrema-based threshold from its metadata; for SVM urban or built-up-area extraction, use `Detect_Urban_Area_by_SVM` before any generic script. These are execution methods, not optional examples.
- For a staged local raster-plus-boundary request for a standard zonal nighttime-light metric supported by `NTL_raster_statistics`, invoke that tool as the primary operation. Its source-grid, pixel-centre, NoData, and area defaults are the method contract; a generic script may only format, map, or validate its output.
- For named Chinese administrative boundaries, POIs, addresses, or landmark coordinates, use the runtime-callable `get_administrative_division_data`, `poi_search_tool`, or `geocode_tool` directly when available; do not infer that a missing staged input means the network tool is unavailable.
- Source-acquisition tools that persist files use `inputs/<filename>` targets; keep derived summaries and reports under `outputs/`.
- Complete the same scientific work and persist only an intermediate EventContext, ObservationPackage, or AnalysisPackage explicitly required by the task. TaskPlan and EvidenceReport are optional audit aids: save either at most once when straightforward, but never let their absence or a failed save block the direct answer.
- Treat every package type explicitly required by the task as mandatory, not optional: persist it with the matching typed `save_*` tool before EvidenceReport synthesis. In particular, an analysis task must save a ready AnalysisPackage and a source-reconciliation event task must save a ready EventContext even though one context performs all work. An EvidenceReport alone never substitutes for the required intermediate package.
- Finalize, re-open, and inspect every artifact for semantic correctness before saving its typed package. For each local artifact declare only the workspace-relative path plus semantic role/media type; typed save binds its SHA-256 and byte count. Never compute, guess, copy, null-fill, or placeholder-fill those system-owned fields, and never block merely because checksum tooling is absent. Saving a package makes all referenced files immutable: never overwrite or edit them afterward. Complete every bounded repair before package persistence.
- Use one batched final validation pass after the last mutation. Do not re-read or re-inspect an unchanged artifact, repeat a successful contract validation or route transition, create unrequested diagnostic outputs, or run an alternate method solely for reassurance. One primary execution plus at most one concrete non-semantic repair is sufficient; after that, return a bounded failed/limited result rather than starting another version.
- For a non-delegating event-context task, write and inspect the requested source-bounded artifact; save and validate EventContext when the task explicitly requires it; then use the routed checkpoints that are genuinely needed. Never end a model turn with future-tense process narration such as "now persist" or "next save" while a required tool call remains—call that tool in the same turn.
- Form one complete TaskPlan at task start. Persist it only when an explicit task requirement or required intermediate package needs it, and checkpoint the non-delegating route only when that workflow is used. A returned `package/<token>` handle means the save succeeded: retain it and do not save the same package again or create probe artifact IDs. Runtime IDs, case identity, system timestamps, and local SHA-256/byte counts are system-owned and intentionally absent from model-facing tool inputs; producer is schema-fixed to the active role. Never discover, request, guess, null-fill, echo, or override system identity; use only opaque package references returned by typed contract tools. Persist every package you actually need once. A genuinely blocked/failed task may finish without an intermediate package or handle, but missing checksum tooling alone is not such a failure.
- ObservationPackage query time is system-managed: after a successful full geodata inspection, the runtime injects its completion time. Never supply or guess it.
- Never use generic filesystem mutation (`write_file` or `edit_file`) on the internal contracts or route-state tree; use only typed `save_*`, `validate_contract`, and `record_route_transition` tools there. No assignment or handoff envelope is part of this non-delegating baseline.
- Respect the same AOI/time/product/QA/provenance/non-attribution boundaries and bounded repair policy.
- A model response with no tool call ends the run. When a required scientific operation, required package save, or route transition remains, call it in the same response rather than narrating a future action. Do not write “let me save”, “I will save”, or “next I will” without invoking that tool in the same response. If only audit persistence or a repeated check remains, return the direct answer. “Stop” means stop checking and either advance to a required scientific action or finish the answer.
- Earth Engine runtime/billing-project selection and initialization are system-managed. Never request, guess, or override that billing project, and never start interactive authentication; full source/output asset IDs may still be required by an explicit resource contract. Use registered bounded GEE tools and report classified runtime failures.
- Never read benchmark Gold or evaluator material.
- An intent, route recommendation, or plan is never a task result. Before a final answer, obtain task evidence through a substantive registered tool call or a verified staged-input read; never finish after merely saying that you will save, inspect, or execute something.
- Once required scientific work and outputs are complete, emit the direct answer with real workspace-relative output paths, validation, and limitations. Do not wait for or retry an optional EvidenceReport save.
- Read `/inputs/`, write `/outputs/`, keep `/shared/` read-only, and never inspect environment variables, runner telemetry, hidden control files, or host paths. Never expose credentials or absolute local paths.
"""


TOOLS_PROMPT_ONLY_NOTICE = """

Benchmark resource profile: `tools_prompt_only`.
Role Skills, supplemental RAG/knowledge retrieval, and startup memory are deliberately disabled for this run. Do not attempt to read `/skills/` or `/memories/`, and do not request `NTL_Knowledge_Base`. Use this system prompt and the registered tools only; report a concrete capability boundary rather than substituting unavailable procedural knowledge.
""".strip()


def _prompt_for_resource_profile(prompt: str, resource_profile: ResourceProfile) -> str:
    if resource_profile == "standard":
        return prompt
    if resource_profile == "tools_prompt_only":
        return f"{prompt}\n\n{TOOLS_PROMPT_ONLY_NOTICE}"
    raise ValueError(f"unsupported resource_profile: {resource_profile}")


def architecture_descriptor(architecture_mode: ArchitectureMode) -> dict[str, Any]:
    if architecture_mode not in ARCHITECTURE_MODES:
        raise ValueError(f"unsupported architecture_mode: {architecture_mode}")
    role_names = ["NTL_Engineer"]
    if architecture_mode == "full":
        role_names.extend(["NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"])
    return {
        "architecture_mode": architecture_mode,
        "role_names": role_names,
        "delegation_enabled": architecture_mode == "full",
        "general_purpose_subagent_enabled": False,
        "contract_schema_version": "ntl.contract.v1",
    }


def build_ntl_graph(
    model_name: str,
    api_key: str,
    request_timeout_s: int = 120,
    graph_name: str = "NTL_Engineer",
    postgres_url: str | None = None,
    architecture_mode: ArchitectureMode = "full",
    resource_profile: ResourceProfile = "standard",
):
    if architecture_mode not in ARCHITECTURE_MODES:
        raise ValueError(f"unsupported architecture_mode: {architecture_mode}")
    if resource_profile not in RESOURCE_PROFILES:
        raise ValueError(
            "resource_profile must be one of: " + ", ".join(RESOURCE_PROFILES)
        )
    api_model = get_api_model_name(model_name)
    if api_model not in DEEPAGENTS_HARNESS_API_MODELS:
        raise ValueError(
            "Deep Agents architecture has no registered NTL harness profile for "
            f"{api_model}; supported API models: {', '.join(DEEPAGENTS_HARNESS_API_MODELS)}."
        )
    if postgres_url:
        raise NotImplementedError(
            "postgres_url is disabled until PostgresStore setup and connection lifetime "
            "are owned by the application runtime."
        )
    store = InMemoryStore()
    tools_prompt_only = resource_profile == "tools_prompt_only"
    skills_enabled = not tools_prompt_only
    rag_enabled = not tools_prompt_only
    memory_enabled = not tools_prompt_only
    backend = RUNTIME_BACKEND if skills_enabled else _build_runtime_backend(skills_enabled=False)

    model = _build_llm(
        model_name=model_name,
        api_key=api_key,
        request_timeout_s=request_timeout_s,
        # Native Deep Agents 0.7.5 forwards the same model into every Full
        # specialist. Serialize tool calls there so a dependent handoff never
        # races its preceding package save. The matched Single-Agent baseline
        # retains the provider default.
        parallel_tool_calls=False if architecture_mode == "full" else None,
    )
    if architecture_mode == "single_agent":
        skill_sources: tuple[str, ...] = ()
        if skills_enabled:
            sources = tuple(ROLE_SPECS[role].skill_sources for role in ROLE_SPECS)
            skill_sources = tuple(dict.fromkeys(source for group in sources for source in group))
            _validate_skill_sources(skill_sources)
        if memory_enabled:
            _seed_runtime_memory()
        knowledge_tools = [_knowledge_base_tool()] if rag_enabled else []
        middleware = [_runtime_memory_middleware(backend)] if memory_enabled else []
        return create_deep_agent(
            model,
            tools=[*single_agent_tools, *SINGLE_AGENT_CONTRACT_TOOLS, *knowledge_tools],
            system_prompt=_prompt_for_resource_profile(
                _single_agent_prompt(), resource_profile
            ),
            middleware=middleware,
            skills=list(skill_sources),
            permissions=filesystem_permissions(
                skill_sources,
                memory_access=memory_enabled,
                skills_enabled=skills_enabled,
            ),
            backend=backend,
            store=store,
            name=graph_name,
            checkpointer=checkpointer,
        ).with_config({"recursion_limit": 1000})

    role_data = ROLE_SPECS["NTL_Data_Searcher"]
    role_analyst = ROLE_SPECS["NTL_Analyst"]
    role_tracker = ROLE_SPECS["NTL_Event_Tracker"]
    all_sources = [
        *ROLE_SPECS["NTL_Engineer"].skill_sources,
        *role_data.skill_sources,
        *role_analyst.skill_sources,
        *role_tracker.skill_sources,
    ]
    if skills_enabled:
        _validate_skill_sources(all_sources)

    specialists = [
        {
            "name": "NTL_Data_Searcher",
            "description": role_data.description,
            "system_prompt": _prompt_for_resource_profile(
                _prompt_text(hierarchical_system_prompt_data_searcher), resource_profile
            ),
            "tools": [*data_searcher_tools, *DATA_SEARCHER_CONTRACT_TOOLS],
            "model": model,
            "skills": list(role_data.skill_sources) if skills_enabled else [],
            "permissions": filesystem_permissions(
                role_data.skill_sources,
                memory_access=False,
                skills_enabled=skills_enabled,
            ),
        },
        {
            "name": "NTL_Analyst",
            "description": role_analyst.description,
            "system_prompt": _prompt_for_resource_profile(
                _prompt_text(system_prompt_analyst), resource_profile
            ),
            "tools": [*analyst_tools, *ANALYST_CONTRACT_TOOLS],
            "model": model,
            "skills": list(role_analyst.skill_sources) if skills_enabled else [],
            "permissions": filesystem_permissions(
                role_analyst.skill_sources,
                memory_access=False,
                skills_enabled=skills_enabled,
            ),
        },
        {
            "name": "NTL_Event_Tracker",
            "description": role_tracker.description,
            "system_prompt": _prompt_for_resource_profile(
                _prompt_text(system_prompt_event_tracker), resource_profile
            ),
            "tools": [*event_tracker_tools, *EVENT_TRACKER_CONTRACT_TOOLS],
            "model": model,
            "skills": list(role_tracker.skill_sources) if skills_enabled else [],
            "permissions": filesystem_permissions(
                role_tracker.skill_sources,
                memory_access=False,
                skills_enabled=skills_enabled,
            ),
        },
    ]

    engineer_sources = ROLE_SPECS["NTL_Engineer"].skill_sources
    if memory_enabled:
        _seed_runtime_memory()
    knowledge_tools = [_knowledge_base_tool()] if rag_enabled else []
    middleware = [_runtime_memory_middleware(backend)] if memory_enabled else []
    return create_deep_agent(
        model,
        tools=[*engineer_tools, *ENGINEER_CONTRACT_TOOLS, *knowledge_tools],
        system_prompt=_prompt_for_resource_profile(_full_system_prompt(), resource_profile),
        middleware=middleware,
        subagents=specialists,
        skills=list(engineer_sources) if skills_enabled else [],
        permissions=filesystem_permissions(
            engineer_sources,
            memory_access=memory_enabled,
            skills_enabled=skills_enabled,
        ),
        backend=backend,
        store=store,
        name=graph_name,
        checkpointer=checkpointer,
    ).with_config({"recursion_limit": 1000})
