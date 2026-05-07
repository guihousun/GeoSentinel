from __future__ import annotations

from contextlib import ExitStack
import os
from typing import Any

from deepagents import create_deep_agent
from deepagents.backends import CompositeBackend, FilesystemBackend, StoreBackend
from deepagents.backends.utils import create_file_data
from deepagents.middleware.skills import _list_skills
from langchain.chat_models import init_chat_model
from langchain_openai import ChatOpenAI
from langgraph.store.memory import InMemoryStore
from pathlib import Path
from pydantic import SecretStr

from agents.NTL_Code_Assistant import Code_Assistant_system_prompt_text
from agents.NTL_Data_Searcher import system_prompt_data_searcher
from agents.NTL_Engineer import system_prompt_text
from model_config import get_api_model_name, get_base_url, get_model_config
from runtime_governance import (
    ASSISTANT_ID,
    deepagents_memory_namespace,
    langgraph_postgres_url,
    memory_backend_mode,
    postgres_auto_setup_enabled,
)
from storage_manager import current_thread_id, storage_manager
from tools import Code_tools, Engineer_tools, data_searcher_tools

from langgraph.checkpoint.memory import MemorySaver

SKILLS_ROOT = Path(__file__).resolve().parent / ".ntl-gpt" / "skills"
SKILLS_SOURCE = "/skills/"
MEMORY_FILE_NAME = "NTL_AGENT_MEMORY.md"
MEMORY_VIRTUAL_KEY = f"/{MEMORY_FILE_NAME}"
_SEEDED_STORE_MEMORY: set[tuple[tuple[str, ...], str]] = set()


class ReadOnlyBackend:
    """Proxy backend that allows reads and blocks mutations for shared data."""

    def __init__(self, backend: Any, label: str = "read-only backend") -> None:
        self._backend = backend
        self._label = label

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)

    def write(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")

    async def awrite(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")

    def edit(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")

    async def aedit(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")

    def upload_files(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")

    async def aupload_files(self, *args: Any, **kwargs: Any) -> Any:
        raise PermissionError(f"{self._label} is read-only.")


def _enter_if_context_manager(resource: Any, stack: ExitStack) -> Any:
    if hasattr(resource, "__enter__") and hasattr(resource, "__exit__"):
        return stack.enter_context(resource)
    return resource


def _setup_if_available(resource: Any, label: str) -> None:
    setup = getattr(resource, "setup", None)
    if not callable(setup) or not postgres_auto_setup_enabled():
        return
    try:
        setup()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to initialize {label}. Check the Postgres URL and schema permissions.") from exc


def _build_persistence(postgres_url: str | None) -> tuple[Any, Any, ExitStack | None]:
    url = langgraph_postgres_url(postgres_url)
    if not url:
        return InMemoryStore(), MemorySaver(), None

    try:
        from langgraph.checkpoint.postgres import PostgresSaver
        from langgraph.store.postgres import PostgresStore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "NTL_LANGGRAPH_POSTGRES_URL is set, but Postgres persistence packages are unavailable. "
            "Install langgraph-checkpoint-postgres in the active conda environment."
        ) from exc

    stack = ExitStack()
    store = _enter_if_context_manager(PostgresStore.from_conn_string(url), stack)
    checkpointer = _enter_if_context_manager(PostgresSaver.from_conn_string(url), stack)
    _setup_if_available(store, "LangGraph PostgresStore")
    _setup_if_available(checkpointer, "LangGraph PostgresSaver")
    return store, checkpointer, stack


def _seed_local_memory_to_store(
    *,
    store: Any,
    namespace: tuple[str, ...],
    thread_memory_file: Path,
    project_memory_path: Path,
) -> None:
    seed_key = (namespace, MEMORY_VIRTUAL_KEY)
    if seed_key in _SEEDED_STORE_MEMORY:
        return

    source_path = thread_memory_file if thread_memory_file.exists() else project_memory_path
    if not source_path.exists():
        _SEEDED_STORE_MEMORY.add(seed_key)
        return

    try:
        if store.get(namespace, MEMORY_VIRTUAL_KEY) is None:
            content = source_path.read_text(encoding="utf-8")
            store.put(namespace, MEMORY_VIRTUAL_KEY, create_file_data(content))
        _SEEDED_STORE_MEMORY.add(seed_key)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to seed DeepAgents memory store namespace {namespace}.") from exc


def _build_llm(model_name: str, api_key: str, request_timeout_s: int):
    model_config = get_model_config(model_name)
    api_model = get_api_model_name(model_name)
    if model_config.provider in {"dashscope", "deepseek"}:
        return ChatOpenAI(
            api_key=SecretStr(api_key),
            base_url=get_base_url(model_name),
            model=api_model,
            timeout=request_timeout_s,
        )
    return init_chat_model(
        api_model,
        model_provider="openai",
        api_key=api_key,
        temperature=0,
        timeout=request_timeout_s,
        max_retries=3,
    )


def _validate_skill_sources(skill_sources: list[str]) -> None:
    missing: list[str] = []
    for src in sorted(set(skill_sources)):
        if not src.startswith("/skills/"):
            continue
        rel = src[len("/skills/") :].strip("/")
        source_dir = SKILLS_ROOT / rel if rel else SKILLS_ROOT
        if not source_dir.exists() or not source_dir.is_dir():
            missing.append(f"{src} -> missing directory: {source_dir}")
            continue
        has_skill = any((p / "SKILL.md").exists() for p in source_dir.iterdir() if p.is_dir())
        if not has_skill:
            missing.append(f"{src} -> no child skill directories with SKILL.md under {source_dir}")
    if missing:
        missing_text = "\n".join(missing)
        raise ValueError(f"Invalid Deep Agents skill sources (missing SKILL.md):\n{missing_text}")


def _validate_skill_runtime_discovery() -> None:
    """Fail fast if Deep Agents cannot discover skills from configured source."""
    probe_backend = CompositeBackend(
        default=FilesystemBackend(root_dir=SKILLS_ROOT, virtual_mode=True),
        routes={
            "/skills/": FilesystemBackend(root_dir=SKILLS_ROOT, virtual_mode=True),
        },
    )
    discovered = _list_skills(probe_backend, SKILLS_SOURCE)
    if not discovered:
        raise ValueError(
            f"Deep Agents skill discovery returned empty from {SKILLS_SOURCE}. "
            f"Check SKILL.md frontmatter and backend routing under {SKILLS_ROOT}."
        )




def build_ntl_graph(
    model_name: str,
    api_key: str,
    request_timeout_s: int = 120,
    graph_name: str = "NTL_Engineer",
    postgres_url: str | None = None,
):
    store, checkpointer, lifecycle_stack = _build_persistence(postgres_url)
    persistence_url = langgraph_postgres_url(postgres_url)
    use_store_memory = memory_backend_mode(persistence_url) == "store"

    llm = _build_llm(model_name=model_name, api_key=api_key, request_timeout_s=request_timeout_s)

    project_memory_path = Path(__file__).resolve().parent / ".ntl-gpt" / MEMORY_FILE_NAME

    def _backend_factory(runtime):
        config = getattr(runtime, "config", {}) or {}
        thread_id = storage_manager.get_thread_id_from_config(config) or current_thread_id.get()
        workspace = storage_manager.get_workspace(str(thread_id).strip() or "debug")
        thread_memory_dir = workspace / "memory"
        thread_memory_dir.mkdir(parents=True, exist_ok=True)

        # Seed per-thread runtime memory only on first use (preserve per-thread persistence).
        thread_memory_file = thread_memory_dir / MEMORY_FILE_NAME
        if project_memory_path.exists() and not thread_memory_file.exists():
            thread_memory_file.write_text(project_memory_path.read_text(encoding="utf-8"), encoding="utf-8")

        default_backend = FilesystemBackend(root_dir=workspace, virtual_mode=True)
        if use_store_memory:
            namespace = deepagents_memory_namespace(
                runtime,
                graph_name=graph_name or ASSISTANT_ID,
                fallback_thread_id=str(thread_id).strip() or "debug",
            )
            _seed_local_memory_to_store(
                store=store,
                namespace=namespace,
                thread_memory_file=thread_memory_file,
                project_memory_path=project_memory_path,
            )
            memories_backend = StoreBackend(
                runtime,
                namespace=lambda ctx: deepagents_memory_namespace(
                    ctx,
                    graph_name=graph_name or ASSISTANT_ID,
                    fallback_thread_id=str(thread_id).strip() or "debug",
                ),
            )
        else:
            memories_backend = FilesystemBackend(root_dir=thread_memory_dir, virtual_mode=True)
        shared_backend = ReadOnlyBackend(
            FilesystemBackend(root_dir=storage_manager.shared_dir, virtual_mode=True),
            label="/shared",
        )
        skills_backend = FilesystemBackend(root_dir=SKILLS_ROOT, virtual_mode=True)
        return CompositeBackend(
            default=default_backend,
            routes={
                "/memories/": memories_backend,
                "/shared/": shared_backend,
                "/skills/": skills_backend,
            },
        )


    data_searcher_subagent = {
        "name": "Data_Searcher",
        "description": "NTL data retrieval specialist: datasets, AOI, temporal coverage, and source validation.",
        "system_prompt": system_prompt_data_searcher,
        "tools": data_searcher_tools,
        "skills": [SKILLS_SOURCE],
    }

    code_assistant_subagent = {
        "name": "Code_Assistant",
        "description": "NTL code execution specialist: geospatial processing, statistics, and script runtime.",
        "system_prompt": Code_Assistant_system_prompt_text,
        "tools": Code_tools,
        "skills": [SKILLS_SOURCE],
    }

    configured_skill_sources = [
        *data_searcher_subagent.get("skills", []),
        *code_assistant_subagent.get("skills", []),
        SKILLS_SOURCE,
    ]
    _validate_skill_sources(configured_skill_sources)
    _validate_skill_runtime_discovery()

    engineer_prompt = getattr(system_prompt_text, "content", str(system_prompt_text))

    NTL_SYSTEM_PROMPT = f"""NTL Engineer: nighttime light analysis supervisor.

Workspace protocol (canonical):
- Read input data from `/inputs/` in the current thread workspace.
- Write generated artifacts to `/outputs/` in the current thread workspace.
- Always resolve paths via `storage_manager.resolve_input_path(...)` and `storage_manager.resolve_output_path(...)`.
- Never use absolute local paths.
- Treat `/shared/...` as read-only source data: read is allowed, write/overwrite is forbidden.
- Runtime note: `/shared/...` is a virtual Deep Agents path and is runtime-mapped to local `base_data/...` during script execution.
- For data discovery, prefer file tools first (for example `glob`) on virtual paths such as `/inputs/*` and `/shared/*` before asking for re-upload or re-download.
- For any chart/map PNG output, use the `geospatial-visualization-cjk` skill. Configure a CJK-capable Matplotlib font before plotting Chinese labels, save figures under `/outputs/`, and verify labels are not rendered as boxes.

Deep Agents virtual-path compatibility (alias mapping):
- `/data/raw/<file>` -> `inputs/<file>`
- `/data/processed/<file>` -> `outputs/<file>`
- `/memories/<file>` -> `memory/<file>`
- `/shared/<file>` -> `base_data/<file>`

Delegation policy:
- Use `Data_Searcher` for retrieval and metadata validation.
- Use `Code_Assistant` for code validation and execution.
- Keep delegation sequential (one subagent at a time).

{engineer_prompt}
"""

    ntl_agent = create_deep_agent(
        model=llm,
        tools=Engineer_tools,
        subagents=[data_searcher_subagent, code_assistant_subagent],
        system_prompt=NTL_SYSTEM_PROMPT,
        skills=[SKILLS_SOURCE],
        memory=["/memories/NTL_AGENT_MEMORY.md"],
        store=store,
        backend=_backend_factory,
        name=graph_name,
        checkpointer=checkpointer,
    )
    if lifecycle_stack is not None:
        setattr(ntl_agent, "_ntl_lifecycle_stack", lifecycle_stack)

    return ntl_agent

