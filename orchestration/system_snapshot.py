"""Deterministic, secret-free identity for the tested NTL-GPT system.

The benchmark Git provenance proves which repository state was used.  This
module adds a human-auditable identity for the parts of that state that define
the experimental treatment: topology, role boundaries, tool/skill surfaces,
caller-authored prompts, native filesystem permissions/backends, typed package
schemas, framework versions, and bounded retry limits.

No model credentials, provider URLs, user paths, benchmark cases, Gold answers,
or evaluator instructions are read or serialized here.
"""

from __future__ import annotations

from dataclasses import asdict
import ast
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
from typing import Any, Mapping


SYSTEM_SNAPSHOT_SCHEMA = "ntl.system-snapshot.v4"

_CORE_CODE_FILES = (
    "graph_factory.py",
    "gee_runtime.py",
    "runtime_governance.py",
    "storage_manager.py",
    "agents/role_specs.py",
    "agents/NTL_Data_Searcher.py",
    "agents/NTL_Analyst.py",
    "agents/NTL_Event_Tracker.py",
    "tools/__init__.py",
    "tools/GaoDe_tool.py",
    "tools/NTL_Code_generation.py",
    "tools/NTL_Knowledge_Base.py",
    "tools/NTL_Knowledge_Base_Searcher.py",
    "utils/ntl_embeddings.py",
    "contracts/agent_packages.py",
    "orchestration/artifact_runtime.py",
    "orchestration/contract_tools.py",
    "orchestration/contracts_io.py",
    "orchestration/observation_runtime.py",
    "orchestration/route_state.py",
)

_RUNTIME_CODE_FILES = (
    "environment.yml",
    "packages/ntl_toolkit/pyproject.toml",
    "benchmark_runtime/contracts.py",
    "benchmark_runtime/runner.py",
    "benchmark_runtime/system_finalizer.py",
    "benchmark_runtime/telemetry.py",
    "orchestration/run_evidence.py",
    "orchestration/system_snapshot.py",
    "orchestration/transfer_records.py",
)

_NTL_TOOLKIT_SOURCE_ROOT = "packages/ntl_toolkit/src/ntl_toolkit"
_OFFICIAL_H5_SOURCE_ROOT = "tools/vnp46a2_official_h5"

_PACKAGE_MODEL_NAMES = (
    "TaskPlan",
    "EventContext",
    "ObservationPackage",
    "AnalysisPackage",
    "EvidenceReport",
)

_HANDOFF_MODEL_NAMES = (
    "AssignmentEnvelope",
    "HandoffEnvelope",
    "RevisionRequest",
    "EngineerDecision",
)

_SYSTEM_TRANSFER_RECORD_MODEL_NAMES = (
    "AssignmentRecordV2",
    "HandoffRecordV2",
)

_RUNTIME_DISTRIBUTIONS = (
    "deepagents",
    "langchain",
    "langchain-core",
    "langchain-openai",
    "langgraph",
    "langgraph-prebuilt",
    "langgraph-checkpoint",
    "langgraph-sdk",
    "langgraph-checkpoint-postgres",
    "earthengine-api",
    "geemap",
    "google-auth",
    "python-dotenv",
    "ntl-toolkit",
    "fiona",
    "geopandas",
    "rasterio",
    "pyproj",
    "shapely",
    "numpy",
    "pandas",
)


def canonical_snapshot_json(value: Mapping[str, Any]) -> str:
    """Return the canonical JSON representation used for snapshot hashes."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def system_snapshot_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_snapshot_json(value).encode("utf-8")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_identity(path: Path, *, repo_root: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=True)
    root = repo_root.resolve(strict=True)
    try:
        relative = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"snapshot file is outside repository root: {path}") from exc
    content = resolved.read_bytes()
    return {
        "relative_path": relative,
        "sha256": _sha256_bytes(content),
        "bytes": len(content),
    }


def _exported_tool_code_files(tools_init_path: Path) -> tuple[str, ...]:
    """Return repo-relative modules referenced by ``tools._EXPORTS``.

    Parse the repository source rather than importing the package so a
    snapshot is always derived from the requested ``repo_root``.  The export
    table is deliberately literal; rejecting dynamic or non-local entries
    keeps the code provenance boundary reviewable and fail closed.
    """

    tree = ast.parse(tools_init_path.read_text(encoding="utf-8"))
    exports_node: ast.expr | None = None
    for node in tree.body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "_EXPORTS"
        ):
            exports_node = node.value
            break
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_EXPORTS"
            for target in node.targets
        ):
            exports_node = node.value
            break
    if exports_node is None:
        raise ValueError("tools/__init__.py must declare a literal _EXPORTS mapping")
    try:
        exports = ast.literal_eval(exports_node)
    except (TypeError, ValueError) as exc:
        raise ValueError("tools._EXPORTS must be a literal mapping") from exc
    if not isinstance(exports, dict) or not exports:
        raise ValueError("tools._EXPORTS must be a non-empty mapping")

    modules: set[str] = set()
    for export_name, target in exports.items():
        if (
            not isinstance(export_name, str)
            or not export_name
            or not isinstance(target, tuple)
            or len(target) != 2
            or not all(isinstance(item, str) and item for item in target)
        ):
            raise ValueError("tools._EXPORTS contains an invalid entry")
        module_name = target[0]
        local_name = module_name.removeprefix(".")
        if not module_name.startswith(".") or module_name.count(".") != 1:
            raise ValueError(
                f"tools._EXPORTS entry {export_name!r} is not a local tools module"
            )
        if not local_name.isidentifier():
            raise ValueError(
                f"tools._EXPORTS entry {export_name!r} has an invalid module name"
            )
        modules.add(f"tools/{local_name}.py")
    return tuple(sorted(modules))


def _snapshot_code_relative_paths(repo_root: Path) -> tuple[str, ...]:
    """Enumerate every local source/config file defining the tested runtime."""

    tools_init = repo_root / "tools" / "__init__.py"
    # ``read_text`` in _exported_tool_code_files and ``resolve(strict=True)`` in
    # _file_identity make missing declared files terminal rather than silently
    # dropping them from the experimental identity.
    exported_tools = _exported_tool_code_files(tools_init)

    toolkit_root = repo_root / _NTL_TOOLKIT_SOURCE_ROOT
    if not toolkit_root.is_dir():
        raise ValueError(f"NTL toolkit source root is missing: {_NTL_TOOLKIT_SOURCE_ROOT}")
    toolkit_files = tuple(
        path.relative_to(repo_root).as_posix()
        for path in sorted(
            toolkit_root.rglob("*.py"),
            key=lambda item: (item.as_posix().casefold(), item.as_posix()),
        )
    )
    if not toolkit_files:
        raise ValueError("NTL toolkit source root contains no Python modules")

    official_h5_root = repo_root / _OFFICIAL_H5_SOURCE_ROOT
    if not official_h5_root.is_dir():
        raise ValueError(f"official HDF5 source root is missing: {_OFFICIAL_H5_SOURCE_ROOT}")
    official_h5_files = tuple(
        path.relative_to(repo_root).as_posix()
        for path in sorted(
            official_h5_root.rglob("*.py"),
            key=lambda item: (item.as_posix().casefold(), item.as_posix()),
        )
    )
    paths = set(
        (
            *_CORE_CODE_FILES,
            *_RUNTIME_CODE_FILES,
            *exported_tools,
            *toolkit_files,
            *official_h5_files,
        )
    )
    return tuple(sorted(paths))


def _code_manifest(repo_root: Path) -> dict[str, dict[str, Any]]:
    identities = (
        _file_identity(repo_root / relative, repo_root=repo_root)
        for relative in _snapshot_code_relative_paths(repo_root)
    )
    return {
        identity["relative_path"]: identity
        for identity in identities
    }


def _text_identity(text: str) -> dict[str, Any]:
    encoded = text.encode("utf-8")
    return {"sha256": _sha256_bytes(encoded), "bytes": len(encoded)}


def _distribution_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for distribution in _RUNTIME_DISTRIBUTIONS:
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = "not-installed"
    return versions


def _middleware_tool_names(*, backend: Any) -> tuple[str, ...]:
    """Inspect the model-visible native Deep Agents filesystem tool surface."""

    from deepagents.middleware.filesystem import FilesystemMiddleware, supports_execution

    # Deep Agents 0.7 removed backend factories; instantiating the middleware
    # with its native StateBackend keeps this metadata probe side-effect free.
    return tuple(
        tool.name
        for tool in FilesystemMiddleware().tools
        if tool.name != "execute" or supports_execution(backend)
    )


def _chat_model_retry_limit(graph_factory_path: Path) -> int:
    """Read the literal ChatOpenAI retry cap from ``_build_llm``.

    The value is parsed from source so the snapshot cannot silently drift from
    the actual constructor argument while keeping a duplicate constant here.
    """

    tree = ast.parse(graph_factory_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != "_build_llm":
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            called = child.func.id if isinstance(child.func, ast.Name) else None
            if called != "ChatOpenAI":
                continue
            for keyword in child.keywords:
                if keyword.arg == "max_retries" and isinstance(keyword.value, ast.Constant):
                    value = keyword.value.value
                    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                        return value
    raise ValueError("graph_factory._build_llm must declare a literal ChatOpenAI max_retries")


def _skill_manifest(
    *,
    repo_root: Path,
    role_specs: Mapping[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    skills_root = repo_root / ".ntl-gpt" / "skills"
    by_role: dict[str, list[dict[str, Any]]] = {}
    unique: dict[str, dict[str, Any]] = {}
    for role_name, spec in role_specs.items():
        rows: list[dict[str, Any]] = []
        for source in spec.skill_sources:
            if not source.startswith("/skills/"):
                raise ValueError(f"role skill source is outside /skills/: {source}")
            relative_source = source[len("/skills/") :].strip("/")
            source_root = skills_root / relative_source
            skill_files = sorted(
                source_root.rglob("SKILL.md"),
                key=lambda item: item.as_posix().casefold(),
            )
            if not skill_files:
                raise ValueError(f"role skill source has no SKILL.md: {source}")
            for skill_file in skill_files:
                identity = _file_identity(skill_file, repo_root=repo_root)
                row = {"source": source, **identity}
                rows.append(row)
                unique[identity["relative_path"]] = identity
        by_role[role_name] = rows
    return by_role, dict(sorted(unique.items()))


def _schema_manifest() -> dict[str, Any]:
    from contracts import agent_packages
    from orchestration.route_state import RouteState
    from orchestration import transfer_records

    models: dict[str, dict[str, Any]] = {}
    for model_name in (*_PACKAGE_MODEL_NAMES, *_HANDOFF_MODEL_NAMES):
        model = getattr(agent_packages, model_name)
        schema = model.model_json_schema()
        encoded = canonical_snapshot_json(schema).encode("utf-8")
        models[model_name] = {
            "schema_sha256": _sha256_bytes(encoded),
            "schema_bytes": len(encoded),
        }
    for model_name in _SYSTEM_TRANSFER_RECORD_MODEL_NAMES:
        model = getattr(transfer_records, model_name)
        schema = model.model_json_schema()
        encoded = canonical_snapshot_json(schema).encode("utf-8")
        models[model_name] = {
            "schema_sha256": _sha256_bytes(encoded),
            "schema_bytes": len(encoded),
        }
    route_schema = RouteState.model_json_schema()
    route_encoded = canonical_snapshot_json(route_schema).encode("utf-8")
    return {
        "contract_schema_version": agent_packages.CONTRACT_SCHEMA_VERSION,
        "package_models": list(_PACKAGE_MODEL_NAMES),
        # Retained for loading historical records; these are no longer
        # model-facing requirements in the native Deep Agents harness.
        "handoff_models": list(_HANDOFF_MODEL_NAMES),
        "legacy_handoff_models": list(_HANDOFF_MODEL_NAMES),
        "system_transfer_record_models": list(_SYSTEM_TRANSFER_RECORD_MODEL_NAMES),
        "models": models,
        "route_state": {
            "schema_version": str(RouteState.model_fields["schema_version"].default),
            "schema_sha256": _sha256_bytes(route_encoded),
            "schema_bytes": len(route_encoded),
        },
    }


def _ordered_union(*groups: tuple[str, ...] | list[str]) -> list[str]:
    return list(dict.fromkeys(item for group in groups for item in group))


def build_system_snapshot(
    repo_root: str | Path,
    *,
    architecture_mode: str,
    model_name: str = "deepseek-v4-flash",
    run_limits: Mapping[str, int | float] | None = None,
    resource_profile: str = "standard",
) -> dict[str, Any]:
    """Build one deterministic architecture snapshot for a batch.

    ``run_limits`` may contain the batch-specific request timeout, task timeout,
    and graph recursion limit.  They are configuration, not secrets, and make
    two otherwise-identical treatment runs distinguishable.
    """

    if architecture_mode not in {"full", "single_agent"}:
        raise ValueError(f"unsupported architecture_mode: {architecture_mode}")
    root = Path(repo_root).resolve(strict=True)

    # Imports stay inside the builder so validators can inspect old snapshots
    # without importing the current graph or optional geospatial tool modules.
    from agents.NTL_Analyst import system_prompt_analyst
    from agents.NTL_Data_Searcher import hierarchical_system_prompt_data_searcher
    from agents.NTL_Event_Tracker import system_prompt_event_tracker
    from agents.role_specs import ROLE_SPECS
    from graph_factory import (
        ANALYST_CONTRACT_TOOLS,
        DATA_SEARCHER_CONTRACT_TOOLS,
        ENGINEER_CONTRACT_TOOLS,
        EVENT_TRACKER_CONTRACT_TOOLS,
        SINGLE_AGENT_CONTRACT_TOOLS,
        DEEPAGENTS_HARNESS_MODEL_SPECS,
        NTL_TASK_DESCRIPTION,
        RUNTIME_MEMORY_SOURCE,
        RUNTIME_MEMORY_TEMPLATE,
        RUNTIME_BACKEND,
        RESOURCE_PROFILES,
        _full_system_prompt,
        _prompt_for_resource_profile,
        _single_agent_prompt,
        filesystem_runtime_descriptor,
    )
    if resource_profile not in RESOURCE_PROFILES:
        raise ValueError(
            "resource_profile must be one of: " + ", ".join(RESOURCE_PROFILES)
        )
    tools_prompt_only = resource_profile == "tools_prompt_only"
    skills_enabled = not tools_prompt_only
    rag_enabled = not tools_prompt_only
    memory_enabled = not tools_prompt_only
    from model_config import get_api_model_name
    from orchestration.route_state import RouteState
    from tools import (
        analyst_tools,
        data_searcher_tools,
        engineer_tools,
        event_tracker_tools,
        single_agent_tools,
    )

    contract_names = {
        "NTL_Engineer": tuple(tool.name for tool in ENGINEER_CONTRACT_TOOLS),
        "NTL_Data_Searcher": tuple(tool.name for tool in DATA_SEARCHER_CONTRACT_TOOLS),
        "NTL_Analyst": tuple(tool.name for tool in ANALYST_CONTRACT_TOOLS),
        "NTL_Event_Tracker": tuple(tool.name for tool in EVENT_TRACKER_CONTRACT_TOOLS),
    }
    domain_names = {
        "NTL_Engineer": tuple(engineer_tools.export_names),
        "NTL_Data_Searcher": tuple(data_searcher_tools.export_names),
        "NTL_Analyst": tuple(analyst_tools.export_names),
        "NTL_Event_Tracker": tuple(event_tracker_tools.export_names),
    }
    api_model = get_api_model_name(model_name)
    selected_harness_spec = f"openai:{api_model}"
    if selected_harness_spec not in DEEPAGENTS_HARNESS_MODEL_SPECS:
        raise ValueError(
            "tested model has no registered Deep Agents harness profile: "
            f"{selected_harness_spec}"
        )
    middleware_tool_names = _middleware_tool_names(backend=RUNTIME_BACKEND)
    if architecture_mode == "full":
        active_roles = list(ROLE_SPECS)
        tool_allowlists: dict[str, dict[str, Any]] = {}
        for role_name in active_roles:
            middleware = (
                *middleware_tool_names,
                *(() if role_name != "NTL_Engineer" else ("task",)),
            )
            knowledge = (
                ("NTL_Knowledge_Base",)
                if role_name == "NTL_Engineer" and rag_enabled
                else ()
            )
            tool_allowlists[role_name] = {
                "domain_tools": list(domain_names[role_name]),
                "contract_tools": list(contract_names[role_name]),
                "middleware_tools": list(middleware),
                "knowledge_tools": list(knowledge),
                "effective_declared_tools": _ordered_union(
                    domain_names[role_name], contract_names[role_name], middleware, knowledge
                ),
            }
    else:
        active_roles = ["NTL_Engineer"]
        single_contract_names = tuple(tool.name for tool in SINGLE_AGENT_CONTRACT_TOOLS)
        tool_allowlists = {
            "NTL_Engineer": {
                "domain_tools": list(single_agent_tools.export_names),
                "contract_tools": list(single_contract_names),
                "middleware_tools": list(middleware_tool_names),
                "knowledge_tools": ["NTL_Knowledge_Base"] if rag_enabled else [],
                "effective_declared_tools": _ordered_union(
                    tuple(single_agent_tools.export_names),
                    single_contract_names,
                    middleware_tool_names,
                    ("NTL_Knowledge_Base",) if rag_enabled else (),
                ),
            }
        }

    def prompt_text(value: Any) -> str:
        return str(getattr(value, "content", value))

    # Deep Agents 0.7 owns its framework/profile prompt assembly.  Only hash
    # the prompts authored by this caller; importing or copying a private
    # framework prompt would make the benchmark identity depend on an
    # implementation detail that the application does not control.
    prompts = {
        "NTL_Engineer": {
            "surface": "system_prompt",
            "text": (
                _prompt_for_resource_profile(_full_system_prompt(), resource_profile)
                if architecture_mode == "full"
                else _prompt_for_resource_profile(_single_agent_prompt(), resource_profile)
            ),
        },
    }
    if architecture_mode == "full":
        prompts.update(
            {
                "NTL_Data_Searcher": {
                    "surface": "system_prompt",
                    "text": _prompt_for_resource_profile(
                        prompt_text(hierarchical_system_prompt_data_searcher), resource_profile
                    ),
                },
                "NTL_Analyst": {
                    "surface": "system_prompt",
                    "text": _prompt_for_resource_profile(
                        prompt_text(system_prompt_analyst), resource_profile
                    ),
                },
                "NTL_Event_Tracker": {
                    "surface": "system_prompt",
                    "text": _prompt_for_resource_profile(
                        prompt_text(system_prompt_event_tracker), resource_profile
                    ),
                },
                "NTL_Engineer.task": {
                    "surface": "tool_description",
                    "text": NTL_TASK_DESCRIPTION,
                },
            }
        )

    if skills_enabled:
        skill_by_role, unique_skills = _skill_manifest(repo_root=root, role_specs=ROLE_SPECS)
    else:
        skill_by_role = {name: [] for name in ROLE_SPECS}
        unique_skills = {}
    role_specs = {
        name: {
            **asdict(spec),
            "skill_files": skill_by_role[name],
        }
        for name, spec in ROLE_SPECS.items()
    }
    runtime_filesystems = {
        name: filesystem_runtime_descriptor(
            ROLE_SPECS[name].skill_sources,
            memory_access=(name == "NTL_Engineer" and memory_enabled),
            skills_enabled=skills_enabled,
        )
        for name in active_roles
    }
    if architecture_mode == "single_agent":
        runtime_filesystems["NTL_Engineer"] = filesystem_runtime_descriptor(
            tuple(
                dict.fromkeys(
                    source
                    for spec in ROLE_SPECS.values()
                    for source in spec.skill_sources
                )
            ),
            memory_access=memory_enabled,
            skills_enabled=skills_enabled,
        )
    code = _code_manifest(root)
    startup_memory_sources = (
        [
            {
                "source": RUNTIME_MEMORY_SOURCE,
                **_file_identity(RUNTIME_MEMORY_TEMPLATE, repo_root=root),
            }
        ]
        if memory_enabled
        else []
    )
    limits = {
        "specialist_max_revisions": int(RouteState.model_fields["max_revisions"].default),
        "model_request_max_retries": _chat_model_retry_limit(root / "graph_factory.py"),
    }
    for name, value in sorted((run_limits or {}).items()):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f"run limit {name} must be a positive number")
        limits[str(name)] = value

    from gee_runtime import (
        GEE_RUNTIME_DESCRIPTOR,
        resolve_gee_boundary_asset_project_id,
        resolve_gee_project_id,
    )

    def _config_fingerprint(kind: str, value: str) -> str:
        return hashlib.sha256(f"ntl-gee-config-v1:{kind}:{value}".encode("utf-8")).hexdigest()

    try:
        runtime_project = resolve_gee_project_id()
    except Exception:  # noqa: BLE001 - snapshot records unconfigured state without blocking local cases
        runtime_project = ""
    try:
        boundary_project = resolve_gee_boundary_asset_project_id() if runtime_project else ""
    except Exception:  # noqa: BLE001
        boundary_project = ""
    sa_email = str(os.getenv("EE_SERVICE_ACCOUNT") or "").strip()
    sa_key = str(os.getenv("EE_PRIVATE_KEY_JSON") or "").strip()
    credential_mode = (
        "service_account"
        if sa_email and sa_key
        else "incomplete_service_account"
        if sa_email or sa_key
        else "ambient"
    )

    snapshot = {
        "schema_version": SYSTEM_SNAPSHOT_SCHEMA,
        "architecture_mode": architecture_mode,
        "topology": {
            "active_roles": active_roles,
            "supervisor": "NTL_Engineer",
            "specialists": (
                ["NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"]
                if architecture_mode == "full"
                else []
            ),
            "delegation_enabled": architecture_mode == "full",
            "general_purpose_subagent_enabled": False,
            "specialists_can_delegate": False,
            "resource_profile": resource_profile,
            "skills_enabled": skills_enabled,
            "rag_enabled": rag_enabled,
            "startup_memory_enabled": memory_enabled,
            "deepagents_harness_profile": selected_harness_spec,
            "deepagents_harness_profiles_supported": list(
                DEEPAGENTS_HARNESS_MODEL_SPECS
            ),
        },
        "role_specs": role_specs,
        "tool_allowlists": tool_allowlists,
        "skill_files": list(unique_skills.values()),
        "prompt_hashes": {
            name: {
                "layer": "caller_authored",
                "surface": prompt["surface"],
                **_text_identity(prompt["text"]),
            }
            for name, prompt in prompts.items()
        },
        "startup_memory_sources": startup_memory_sources,
        "gee_runtime": {
            "descriptor": dict(GEE_RUNTIME_DESCRIPTOR),
            "project_configured": bool(runtime_project),
            "project_fingerprint": (
                _config_fingerprint("runtime_project", runtime_project)
                if runtime_project
                else None
            ),
            "boundary_owner_configured_separately": bool(
                runtime_project and boundary_project and boundary_project != runtime_project
            ),
            "boundary_owner_fingerprint": (
                _config_fingerprint("boundary_project", boundary_project)
                if boundary_project
                else None
            ),
            "credential_mode": credential_mode,
        },
        "code_hashes": code,
        "filesystem_runtime": runtime_filesystems,
        "package_contracts": _schema_manifest(),
        "runtime_versions": _distribution_versions(),
        "limits": limits,
    }
    # A final round trip proves that no non-JSON object leaked into the snapshot.
    return json.loads(canonical_snapshot_json(snapshot))


def validate_system_snapshot(
    value: Any,
    *,
    expected_sha256: str | None = None,
    architecture_mode: str | None = None,
    model_name: str | None = None,
) -> dict[str, Any]:
    """Validate a persisted snapshot without comparing it to today's code."""

    if not isinstance(value, Mapping):
        raise ValueError("system snapshot must be a JSON object")
    snapshot = dict(value)
    required = {
        "schema_version",
        "architecture_mode",
        "topology",
        "role_specs",
        "tool_allowlists",
        "skill_files",
        "prompt_hashes",
        "startup_memory_sources",
        "gee_runtime",
        "code_hashes",
        "filesystem_runtime",
        "package_contracts",
        "runtime_versions",
        "limits",
    }
    missing = sorted(required - set(snapshot))
    if missing:
        raise ValueError("system snapshot is missing fields: " + ", ".join(missing))
    if snapshot["schema_version"] != SYSTEM_SNAPSHOT_SCHEMA:
        raise ValueError("system snapshot has the wrong schema_version")
    startup_memory = snapshot["startup_memory_sources"]
    if not isinstance(startup_memory, list):
        raise ValueError("system snapshot startup_memory_sources must be a list")
    for row in startup_memory:
        if not isinstance(row, Mapping):
            raise ValueError("system snapshot startup_memory_sources contains a non-object")
        source = row.get("source")
        if not isinstance(source, str) or not source.startswith("/memories/"):
            raise ValueError("system snapshot startup memory source must use /memories/")
        relative_path = row.get("relative_path")
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or "\\" in relative_path
            or relative_path.startswith("/")
            or Path(relative_path).drive
            or any(part in {"", ".", ".."} for part in relative_path.split("/"))
        ):
            raise ValueError("system snapshot startup memory path is not relative")
        digest = row.get("sha256")
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError("system snapshot startup memory has an invalid sha256")
        byte_count = row.get("bytes")
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise ValueError("system snapshot startup memory has an invalid byte count")
    gee_runtime = snapshot["gee_runtime"]
    if not isinstance(gee_runtime, Mapping):
        raise ValueError("system snapshot gee_runtime must be an object")
    if gee_runtime.get("credential_mode") not in {
        "ambient",
        "service_account",
        "incomplete_service_account",
    }:
        raise ValueError("system snapshot gee_runtime has an invalid credential mode")
    for fingerprint_name in ("project_fingerprint", "boundary_owner_fingerprint"):
        fingerprint = gee_runtime.get(fingerprint_name)
        if fingerprint is not None and (
            not isinstance(fingerprint, str) or len(fingerprint) != 64
        ):
            raise ValueError("system snapshot gee_runtime has an invalid fingerprint")
    mode = snapshot["architecture_mode"]
    if mode not in {"full", "single_agent"}:
        raise ValueError("system snapshot has an invalid architecture_mode")
    if architecture_mode is not None and mode != architecture_mode:
        raise ValueError("system snapshot architecture_mode does not match the run")
    topology = snapshot["topology"]
    if not isinstance(topology, Mapping):
        raise ValueError("system snapshot topology must be an object")
    expected_roles = (
        ["NTL_Engineer", "NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"]
        if mode == "full"
        else ["NTL_Engineer"]
    )
    if topology.get("active_roles") != expected_roles:
        raise ValueError("system snapshot active_roles do not match architecture_mode")
    if topology.get("general_purpose_subagent_enabled") is not False:
        raise ValueError("system snapshot must disable the implicit general-purpose subagent")
    resource_profile = topology.get("resource_profile", "standard")
    if resource_profile not in {"standard", "tools_prompt_only"}:
        raise ValueError("system snapshot has an invalid resource_profile")
    expected_resources = {
        "skills_enabled": resource_profile == "standard",
        "rag_enabled": resource_profile == "standard",
        "startup_memory_enabled": resource_profile == "standard",
    }
    for field, expected in expected_resources.items():
        if field in topology and topology[field] is not expected:
            raise ValueError(f"system snapshot {field} does not match resource_profile")
    if resource_profile == "tools_prompt_only" and startup_memory:
        raise ValueError("tools_prompt_only snapshot must not load startup memory")
    if not isinstance(topology.get("deepagents_harness_profile"), str) or not topology[
        "deepagents_harness_profile"
    ].strip():
        raise ValueError("system snapshot must identify the Deep Agents harness profile")
    supported_profiles = topology.get("deepagents_harness_profiles_supported")
    if (
        not isinstance(supported_profiles, list)
        or not supported_profiles
        or not all(isinstance(item, str) and item.strip() for item in supported_profiles)
        or topology["deepagents_harness_profile"] not in supported_profiles
    ):
        raise ValueError("system snapshot has invalid Deep Agents harness profiles")
    if model_name is not None:
        from model_config import get_api_model_name

        expected_profile = f"openai:{get_api_model_name(model_name)}"
        if topology["deepagents_harness_profile"] != expected_profile:
            raise ValueError("system snapshot harness profile does not match the tested model")
    if set(snapshot.get("tool_allowlists") or {}) != set(expected_roles):
        raise ValueError("system snapshot tool allowlists do not match active roles")
    if resource_profile == "tools_prompt_only":
        if snapshot["skill_files"]:
            raise ValueError("tools_prompt_only snapshot must not load Skill files")
        for record in snapshot["tool_allowlists"].values():
            if record.get("knowledge_tools"):
                raise ValueError("tools_prompt_only snapshot must not expose RAG tools")
    runtime_versions = snapshot.get("runtime_versions")
    if not isinstance(runtime_versions, Mapping) or set(runtime_versions) != set(
        _RUNTIME_DISTRIBUTIONS
    ):
        raise ValueError("system snapshot runtime versions are incomplete")
    if not all(
        isinstance(version, str) and bool(version.strip())
        for version in runtime_versions.values()
    ):
        raise ValueError("system snapshot runtime versions must be non-empty strings")
    filesystem_runtime = snapshot.get("filesystem_runtime")
    if not isinstance(filesystem_runtime, Mapping) or set(filesystem_runtime) != set(
        expected_roles
    ):
        raise ValueError("system snapshot filesystem runtime does not match active roles")
    for role_name, descriptor in filesystem_runtime.items():
        if not isinstance(descriptor, Mapping):
            raise ValueError(
                f"system snapshot filesystem runtime for {role_name} must be an object"
            )
        if not isinstance(descriptor.get("backend_type"), str) or not descriptor[
            "backend_type"
        ].strip():
            raise ValueError(
                f"system snapshot filesystem runtime for {role_name} has no backend type"
            )
        routes = descriptor.get("routes")
        if not isinstance(routes, Mapping) or not routes or not all(
            isinstance(route, str)
            and route.startswith("/")
            and isinstance(target, str)
            and bool(target.strip())
            for route, target in routes.items()
        ):
            raise ValueError(
                f"system snapshot filesystem runtime for {role_name} has invalid routes"
            )
        internal_state_paths = descriptor.get("internal_state_paths")
        if not isinstance(internal_state_paths, list) or not all(
            isinstance(path, str) and path.startswith("/")
            for path in internal_state_paths
        ):
            raise ValueError(
                f"system snapshot filesystem runtime for {role_name} has invalid internal paths"
            )
        permissions = descriptor.get("permissions")
        if not isinstance(permissions, list) or not permissions:
            raise ValueError(
                f"system snapshot filesystem runtime for {role_name} has no permissions"
            )
        for rule in permissions:
            if not isinstance(rule, Mapping):
                raise ValueError("system snapshot filesystem permission must be an object")
            if rule.get("mode") not in {"allow", "deny", "interrupt"}:
                raise ValueError("system snapshot filesystem permission has an invalid mode")
            operations = rule.get("operations")
            paths = rule.get("paths")
            if not isinstance(operations, list) or not operations or not set(
                operations
            ).issubset({"read", "write"}):
                raise ValueError(
                    "system snapshot filesystem permission has invalid operations"
                )
            if not isinstance(paths, list) or not paths or not all(
                isinstance(path, str) and path.startswith("/") for path in paths
            ):
                raise ValueError("system snapshot filesystem permission has invalid paths")
    for group_name in ("skill_files", "prompt_hashes", "code_hashes"):
        group = snapshot[group_name]
        rows = group.values() if isinstance(group, Mapping) else group
        if not isinstance(rows, (list, tuple)) and not hasattr(rows, "__iter__"):
            raise ValueError(f"system snapshot {group_name} must be iterable")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError(f"system snapshot {group_name} contains a non-object")
            digest = row.get("sha256")
            if not isinstance(digest, str) or len(digest) != 64:
                raise ValueError(f"system snapshot {group_name} contains an invalid sha256")
            if group_name == "prompt_hashes" and row.get("layer") != "caller_authored":
                raise ValueError(
                    "system snapshot prompt hashes must identify caller-authored prompts"
                )
            if group_name == "prompt_hashes" and row.get("surface") not in {
                "system_prompt",
                "tool_description",
            }:
                raise ValueError("system snapshot prompt hash has an invalid surface")
    code_hashes = snapshot["code_hashes"]
    if not isinstance(code_hashes, Mapping) or not code_hashes:
        raise ValueError("system snapshot code_hashes must be a non-empty object")
    code_paths = list(code_hashes)
    if code_paths != sorted(code_paths):
        raise ValueError("system snapshot code_hashes must use stable path ordering")
    for relative_path, row in code_hashes.items():
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or "\\" in relative_path
            or relative_path.startswith("/")
            or Path(relative_path).drive
            or any(part in {"", ".", ".."} for part in relative_path.split("/"))
        ):
            raise ValueError("system snapshot code_hashes contains a non-relative path")
        if row.get("relative_path") != relative_path:
            raise ValueError("system snapshot code_hashes path does not match its record")
    actual = system_snapshot_sha256(snapshot)
    if expected_sha256 is not None and actual != expected_sha256:
        raise ValueError("system snapshot sha256 does not match its declared digest")
    return snapshot


__all__ = [
    "SYSTEM_SNAPSHOT_SCHEMA",
    "build_system_snapshot",
    "canonical_snapshot_json",
    "system_snapshot_sha256",
    "validate_system_snapshot",
]

