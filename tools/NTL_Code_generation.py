from __future__ import annotations

import ast
import contextlib
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Literal, Optional, Tuple
from uuid import uuid4

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from dotenv import dotenv_values

from storage_manager import (
    current_gee_encrypted_refresh_token,
    current_gee_project_id,
    current_gee_token_scopes,
    current_thread_id,
    storage_manager,
)

DEFAULT_GEE_PROJECT = "empyrean-caster-430308-m2"


def _live_storage_context_value(name: str, imported_context_var: Any) -> str:
    """Read contextvars robustly even if tests reload storage_manager after import."""
    try:
        value = imported_context_var.get()
    except Exception:
        value = ""
    if str(value or "").strip():
        return str(value or "").strip()
    try:
        live_module = __import__("storage_manager")
        live_context_var = getattr(live_module, name, None)
        if live_context_var is not None and live_context_var is not imported_context_var:
            value = live_context_var.get()
    except Exception:
        value = ""
    return str(value or "").strip()


def _gee_project_id() -> str:
    context_project_id = _live_storage_context_value("current_gee_project_id", current_gee_project_id)
    if context_project_id:
        return context_project_id
    active_project_id = str(os.getenv("NTL_ACTIVE_GEE_PROJECT_ID", "") or "").strip()
    if active_project_id:
        return active_project_id
    dotenv_path = Path(__file__).resolve().parents[1] / ".env"
    project_id = ""
    if dotenv_path.exists():
        project_id = str(dotenv_values(dotenv_path).get("GEE_DEFAULT_PROJECT_ID") or "").strip()
    if not project_id:
        project_id = str(os.getenv("GEE_DEFAULT_PROJECT_ID") or "").strip()
    return project_id or DEFAULT_GEE_PROJECT


def _active_gee_credentials():
    encrypted = (
        _live_storage_context_value("current_gee_encrypted_refresh_token", current_gee_encrypted_refresh_token)
        or str(os.getenv("NTL_ACTIVE_GEE_ENCRYPTED_REFRESH_TOKEN", "") or "").strip()
    )
    if not encrypted:
        return None
    import gee_auth

    refresh_token = gee_auth.decrypt_refresh_token(encrypted)
    scopes_text = (
        _live_storage_context_value("current_gee_token_scopes", current_gee_token_scopes)
        or str(os.getenv("NTL_ACTIVE_GEE_TOKEN_SCOPES", "") or "").strip()
    )
    scopes = scopes_text.split() if scopes_text else None
    return gee_auth.credentials_from_refresh_token(refresh_token, scopes=scopes)


def _patch_ee_initialize_for_active_credentials(code_block: str) -> str:
    encrypted = (
        _live_storage_context_value("current_gee_encrypted_refresh_token", current_gee_encrypted_refresh_token)
        or str(os.getenv("NTL_ACTIVE_GEE_ENCRYPTED_REFRESH_TOKEN", "") or "").strip()
    )
    if not encrypted:
        return code_block
    code = str(code_block or "")
    code = re.sub(
        r"ee\.Initialize\(\s*project\s*=",
        "ee.Initialize(credentials=ntl_ee_credentials, project=",
        code,
    )
    code = re.sub(
        r"ee\.Initialize\(\s*\)",
        "ee.Initialize(credentials=ntl_ee_credentials, project=project_id)",
        code,
    )
    return code

GLOBAL_EXEC_CONTEXTS: Dict[str, Dict[str, Any]] = {}

# Known NTL datasets and expected bands for guardrail checks.
KNOWN_GEE_DATASETS: Dict[str, List[str]] = {
    "projects/sat-io/open-datasets/npp-viirs-ntl": ["b1"],
    "NOAA/VIIRS/DNB/ANNUAL_V21": ["average", "average_masked", "cf_cvg", "cvg", "maximum", "median", "minimum"],
    "NOAA/VIIRS/DNB/ANNUAL_V22": ["average", "average_masked", "cf_cvg", "cvg", "maximum", "median", "minimum"],
    "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG": ["avg_rad", "cf_cvg"],
    "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS": ["avg_vis", "stable_lights"],
    "NASA/VIIRS/002/VNP46A2": ["Gap_Filled_DNB_BRDF_Corrected_NTL", "DNB_BRDF_Corrected_NTL"],
    "NOAA/VIIRS/001/VNP46A1": ["DNB_At_Sensor_Radiance_500m"],
}

KNOWN_GEE_PREFIXES = (
    "projects/sat-io/open-datasets/",
    "projects/empyrean-caster-430308-m2/assets/",
)

KNOWN_GEE_PUBLIC_COLLECTION_PREFIXES = (
    "NOAA/",
    "NASA/",
    "FAO/",
    "COPERNICUS/",
    "USGS/",
    "MODIS/",
)

PATH_PROTOCOL_MODES = {"sandbox", "hybrid", "resolver"}

URI_SCHEME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
WINDOWS_ABS_LITERAL_PATTERN = re.compile(r"^[A-Za-z]:[\\/](?![\\/])")
UNIX_ABS_LITERAL_PATTERN = re.compile(r"^/(?:home|Users|mnt|tmp)/")

ABSOLUTE_PATH_EXTRACT_PATTERNS = [
    re.compile(r"[A-Za-z]:[\\/][^\s\"'`]+"),
    re.compile(r"/(?:home|Users|mnt|tmp)/[^\s\"'`]+"),
]

READ_PATTERNS = [
    r"rasterio\.open\(",
    r"gpd\.read_file\(",
    r"pd\.read_csv\(",
    r"pd\.read_excel\(",
]

WRITE_PATTERNS = [
    r"\.to_csv\(",
    r"\.to_file\(",
    r"plt\.savefig\(",
    r"rasterio\.open\([^\n]*['\"]w['\"]",
    r"open\([^\n]*,\s*['\"](?:w|a|x|wb|ab|xb)['\"]",
    r"\.write_text\(",
    r"\.write_bytes\(",
]

FORBIDDEN_SOURCE_TARGET_PATTERNS = [
    re.compile(r"['\"][^'\"]*[/\\]agents[/\\][^'\"]*['\"]"),
    re.compile(r"['\"][^'\"]*[/\\]tools[/\\][^'\"]*['\"]"),
    re.compile(r"['\"][^'\"]*[/\\]docs[/\\][^'\"]*['\"]"),
    re.compile(r"['\"][^'\"]*[/\\]tests[/\\][^'\"]*['\"]"),
    re.compile(r"['\"](?:app_agents\.py|app_logic\.py|app_ui\.py|Streamlit\.py|storage_manager\.py|requirements\.txt|installed_skill_meta\.json|\.env)['\"]"),
]

FORBIDDEN_COMMAND_PATTERNS = [
    re.compile(r"os\.system\([^\)]*git\s+", flags=re.IGNORECASE),
    re.compile(r"subprocess\.(?:run|Popen)\([^\)]*git\s+", flags=re.IGNORECASE),
    re.compile(r"apply_patch", flags=re.IGNORECASE),
]

VIRTUAL_INPUT_PREFIXES = ("/shared/", "/data/raw/", "/memories/")
VIRTUAL_OUTPUT_PREFIXES = ("/data/processed/",)


class GeoCodeCOTBlockInput(BaseModel):
    code_block: str = Field(
        ...,
        description=(
            "A minimal, self-contained Python code snippet that tests one specific logic unit "
            "from the original geospatial code, such as data loading, CRS matching, field access, "
            "masking, or regression modeling."
        ),
    )
    strict_mode: bool = Field(
        default=False,
        description=(
            "If True, block execution when preflight finds hard safety violations."
        ),
    )


class ExecuteScriptInput(BaseModel):
    script_name: str = Field(
        ...,
        description="Workspace-relative .py script filename in current thread inputs/outputs, for example analysis_plan.py.",
    )
    script_location: str = Field(
        default="auto",
        description="Script lookup location: auto|outputs|inputs. auto prefers outputs first, then inputs.",
    )
    strict_mode: bool = Field(
        default=False,
        description="If True, block execution when preflight finds hard safety violations.",
    )
    force_execute: bool = Field(
        default=False,
        description="If True, bypass dedupe skip and force re-execution even if script content is unchanged.",
    )

def _sanitize_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text or "")


def _extract_absolute_paths(text: str) -> List[str]:
    raw = str(text or "")
    seen: set[str] = set()
    out: List[str] = []
    for pattern in ABSOLUTE_PATH_EXTRACT_PATTERNS:
        for match in pattern.findall(raw):
            path = str(match).strip().strip("\"'`")
            path = path.rstrip(".,;:)]}>")
            if path and path not in seen:
                seen.add(path)
                out.append(path)
    return out

def _iter_string_literals_from_code(code: str) -> List[str]:
    values: List[str] = []
    try:
        tree = ast.parse(code)
    except Exception:
        return values

    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            values.append(node.value)
    return values


def _detect_absolute_path_literals(code: str) -> List[str]:
    """Detect literal absolute filesystem paths and ignore URI strings."""
    hits: List[str] = []
    seen: set[str] = set()
    for raw in _iter_string_literals_from_code(code):
        s = str(raw or "").strip()
        if not s:
            continue
        if URI_SCHEME_PATTERN.match(s):
            continue
        if WINDOWS_ABS_LITERAL_PATTERN.match(s) or UNIX_ABS_LITERAL_PATTERN.match(s):
            if s not in seen:
                seen.add(s)
                hits.append(s)
    return hits


def _dedupe_ordered(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        s = str(item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _empty_runtime_path_rewrite_report() -> Dict[str, Any]:
    return {
        "applied": False,
        "mapping_count": 0,
        "mappings": [],
    }


def _resolve_virtual_path_for_runtime(path_value: str, thread_id: str) -> Optional[str]:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    if any(raw.startswith(prefix) for prefix in VIRTUAL_INPUT_PREFIXES):
        return str(storage_manager.resolve_input_path(raw, thread_id=thread_id))
    if any(raw.startswith(prefix) for prefix in VIRTUAL_OUTPUT_PREFIXES):
        return str(storage_manager.resolve_output_path(raw, thread_id=thread_id))
    return None


def _rewrite_virtual_paths_for_runtime(code: str, thread_id: str) -> Tuple[str, Dict[str, Any]]:
    report = _empty_runtime_path_rewrite_report()
    if not code or not code.strip():
        return code, report

    try:
        tree = ast.parse(code)
    except Exception:
        return code, report

    mappings: List[Dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    class _VirtualPathRewriter(ast.NodeTransformer):
        def visit_Constant(self, node: ast.Constant) -> ast.AST:  # noqa: N802
            if not isinstance(node.value, str):
                return node
            resolved = _resolve_virtual_path_for_runtime(node.value, thread_id=thread_id)
            if not resolved or resolved == node.value:
                return node
            pair = (node.value, resolved)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                mappings.append({"from": node.value, "to": resolved})
            return ast.copy_location(ast.Constant(value=resolved), node)

    rewritten = _VirtualPathRewriter().visit(tree)
    ast.fix_missing_locations(rewritten)
    rewritten_code = ast.unparse(rewritten)
    if rewritten_code != code:
        report = {
            "applied": bool(mappings),
            "mapping_count": len(mappings),
            "mappings": mappings,
        }
    return rewritten_code, report


def _extract_shared_write_targets(code: str) -> List[str]:
    targets: List[str] = []
    seen: set[str] = set()
    try:
        tree = ast.parse(code)
    except Exception:
        return targets

    def _const_str(node: Optional[ast.AST]) -> Optional[str]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _mark_if_shared(path_value: Optional[str]) -> None:
        p = str(path_value or "").strip()
        if p.startswith("/shared/") and p not in seen:
            seen.add(p)
            targets.append(p)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        if isinstance(func, ast.Name) and func.id == "open":
            path_arg = _const_str(node.args[0]) if node.args else None
            mode_arg = _const_str(node.args[1]) if len(node.args) > 1 else None
            for kw in node.keywords:
                if kw.arg == "mode":
                    mode_arg = _const_str(kw.value) or mode_arg
            mode = str(mode_arg or "r").lower()
            if any(flag in mode for flag in ("w", "a", "x")):
                _mark_if_shared(path_arg)
            continue

        if isinstance(func, ast.Attribute):
            attr = func.attr
            if attr in {"to_csv", "to_file", "savefig", "write_text", "write_bytes"}:
                path_arg = _const_str(node.args[0]) if node.args else None
                if not path_arg:
                    for kw in node.keywords:
                        if kw.arg in {"path", "path_or_buf", "fname", "filename"}:
                            path_arg = _const_str(kw.value)
                            if path_arg:
                                break
                _mark_if_shared(path_arg)
                continue

            if attr == "open":
                path_arg = _const_str(node.args[0]) if node.args else None
                mode_arg = _const_str(node.args[1]) if len(node.args) > 1 else None
                for kw in node.keywords:
                    if kw.arg == "mode":
                        mode_arg = _const_str(kw.value) or mode_arg
                mode = str(mode_arg or "").lower()
                if any(flag in mode for flag in ("w", "a", "x")):
                    _mark_if_shared(path_arg)
                continue

    return targets



def _build_artifact_audit(stdout: str, thread_id: Optional[str] = None) -> Dict[str, Any]:
    tid = str(thread_id or current_thread_id.get() or "debug").strip() or "debug"
    workspace_outputs = (storage_manager.get_workspace(tid) / "outputs").resolve()
    workspace_outputs_norm = str(workspace_outputs).replace("/", "\\").lower()

    extracted_paths = _extract_absolute_paths(stdout or "")
    output_like_paths: List[str] = []
    out_of_workspace_paths: List[str] = []
    for raw_path in extracted_paths:
        candidate_norm = raw_path.replace("/", "\\").lower()
        if "\\outputs\\" not in candidate_norm:
            continue
        output_like_paths.append(raw_path)
        try:
            resolved = str(Path(raw_path).resolve())
        except Exception:
            resolved = os.path.abspath(raw_path)
        resolved_norm = resolved.replace("/", "\\").lower()
        in_workspace = resolved_norm == workspace_outputs_norm or resolved_norm.startswith(workspace_outputs_norm + "\\")
        if not in_workspace:
            out_of_workspace_paths.append(resolved)

    return {
        "thread_id": tid,
        "workspace_outputs_dir": str(workspace_outputs),
        "detected_output_paths": output_like_paths,
        "out_of_workspace_paths": out_of_workspace_paths,
        "pass": len(out_of_workspace_paths) == 0,
        "auto_migration_attempted": False,
        "auto_migration_success": False,
        "migrated_paths": [],
        "migration_failures": [],
    }


def _build_stdout_quality_audit(stdout: str) -> Dict[str, Any]:
    """Detect successful exits that actually report empty geospatial results."""
    text = str(stdout or "")
    warnings: List[str] = []
    patterns = [
        (r"\b0\s+regions?\b", "0 regions"),
        (r"\b0\s+features?\b", "0 features"),
        (r"\b0\s+records?\b", "0 records"),
        (r"\brows?\s*[:=]\s*0\b", "rows=0"),
        (r"\brow_count\s*[:=]\s*0\b", "row_count=0"),
        (r"\bfeatures?\s*[:=]\s*0\b", "features=0"),
        (r"\bregions?\s*[:=]\s*0\b", "regions=0"),
    ]
    for pattern, label in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            warnings.append(
                f"Execution log reports {label}; empty geospatial/statistical outputs must be treated as failure."
            )
    warnings = _dedupe_ordered(warnings)
    return {
        "pass": not warnings,
        "warnings": warnings,
        "error_type": "EmptyResultError" if warnings else None,
        "error_message": warnings[0] if warnings else None,
    }


@contextlib.contextmanager
def _thread_bound_storage_paths(thread_id: str):
    """Temporarily force storage_manager path resolvers to current thread when thread_id is omitted."""
    bound_tid = str(thread_id or current_thread_id.get() or "debug").strip() or "debug"
    original_resolve_output_path = storage_manager.resolve_output_path
    original_resolve_input_path = storage_manager.resolve_input_path

    def _resolve_output(filename: str, thread_id: Optional[str] = None) -> str:
        return original_resolve_output_path(filename, thread_id=bound_tid if thread_id is None else thread_id)

    def _resolve_input(filename: str, thread_id: Optional[str] = None) -> str:
        return original_resolve_input_path(filename, thread_id=bound_tid if thread_id is None else thread_id)

    storage_manager.resolve_output_path = _resolve_output
    storage_manager.resolve_input_path = _resolve_input
    try:
        yield
    finally:
        storage_manager.resolve_output_path = original_resolve_output_path
        storage_manager.resolve_input_path = original_resolve_input_path


@contextlib.contextmanager
def _thread_workspace_cwd(thread_id: str):
    """
    Temporarily switch CWD to current thread workspace.

    This provides backward compatibility for legacy scripts using relative
    paths like `inputs/...` and `outputs/...` while keeping thread isolation.
    """
    workspace = storage_manager.get_workspace(thread_id)
    previous = Path.cwd()
    os.chdir(workspace)
    try:
        yield workspace
    finally:
        os.chdir(previous)


def _auto_migrate_cross_workspace_outputs(
    out_paths: List[str],
    thread_id: str,
) -> Tuple[List[str], List[str]]:
    """Copy cross-workspace output artifacts back to current thread outputs."""
    dst_dir = storage_manager.get_workspace(thread_id) / "outputs"
    dst_dir.mkdir(parents=True, exist_ok=True)
    migrated: List[str] = []
    failures: List[str] = []

    seen: set[str] = set()
    for raw in out_paths:
        src_str = str(raw or "").strip().strip("\"'`")
        if not src_str or src_str in seen:
            continue
        seen.add(src_str)
        src_norm = src_str.replace("/", "\\").lower()
        if "\\outputs\\" not in src_norm:
            failures.append(f"{src_str} :: skipped_non_outputs_path")
            continue
        try:
            src = Path(src_str)
            if not src.exists():
                failures.append(f"{src_str} :: source_not_found")
                continue
            dst = dst_dir / src.name
            shutil.copy2(src, dst)
            migrated.append(str(dst.resolve()))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{src_str} :: {exc}")
    return migrated, failures


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sanitize_workspace_path_parts(parts: Tuple[str, ...]) -> List[str]:
    sanitized: List[str] = []
    for raw_part in parts:
        part = re.sub(r"[^A-Za-z0-9._-]", "_", str(raw_part or "")).strip("._")
        if part:
            sanitized.append(part)
    return sanitized


def _normalize_workspace_logical_path(
    path_value: Optional[str],
    *,
    default_root: str,
    auto_name_prefix: Optional[str] = None,
    required_suffix: Optional[str] = None,
    allow_roots: Tuple[str, ...] = ("inputs", "outputs"),
) -> str:
    raw = str(path_value or "").strip().replace("\\", "/")
    if not raw:
        if not auto_name_prefix:
            raise ValueError("Path is required.")
        raw = f"{auto_name_prefix}_{_timestamp()}_{uuid4().hex[:8]}{required_suffix or ''}"

    rel = storage_manager._safe_workspace_relative_path(raw.lstrip("/"))
    parts = list(rel.parts)
    root = default_root
    if parts and parts[0] in {"inputs", "outputs", "memory"}:
        root = parts[0]
        parts = parts[1:]
    if root not in allow_roots:
        raise PermissionError(f"Path root '{root}' is not allowed in this context.")

    sanitized_parts = _sanitize_workspace_path_parts(tuple(parts))
    if not sanitized_parts:
        if not auto_name_prefix:
            raise ValueError("Path is invalid after sanitization.")
        sanitized_parts = [f"{auto_name_prefix}_{_timestamp()}_{uuid4().hex[:8]}"]

    if required_suffix and not sanitized_parts[-1].lower().endswith(required_suffix.lower()):
        sanitized_parts[-1] = f"{sanitized_parts[-1]}{required_suffix}"

    return "/".join([root, *sanitized_parts])


def _normalize_script_request_path(path_value: str, *, thread_id: str) -> str:
    raw = str(path_value or "").strip().replace("\\", "/")
    if raw.startswith("/data/processed/"):
        raw = "outputs/" + raw[len("/data/processed/") :].strip("/")
    elif raw.startswith("/data/raw/"):
        raw = "inputs/" + raw[len("/data/raw/") :].strip("/")
    elif raw.startswith("/outputs/"):
        raw = "outputs/" + raw[len("/outputs/") :].strip("/")
    elif raw.startswith("/inputs/"):
        raw = "inputs/" + raw[len("/inputs/") :].strip("/")

    workspace = storage_manager.get_workspace(thread_id).resolve()
    is_windows_abs = bool(re.match(r"^[A-Za-z]:/", raw))
    if is_windows_abs or raw.startswith("/"):
        candidate = Path(raw).resolve()
        for root in ("outputs", "inputs"):
            root_dir = (workspace / root).resolve()
            try:
                rel = candidate.relative_to(root_dir)
                rel_text = str(rel).replace("\\", "/")
                return _normalize_workspace_logical_path(
                    f"{root}/{rel_text}",
                    default_root="outputs",
                    required_suffix=".py",
                    allow_roots=("inputs", "outputs"),
                )
            except ValueError:
                continue
        try:
            rel = candidate.relative_to(workspace)
            safe_rel = storage_manager._safe_workspace_relative_path(str(rel).replace("\\", "/"))
            sanitized_parts = _sanitize_workspace_path_parts(tuple(safe_rel.parts))
            if not sanitized_parts:
                raise ValueError("Path is invalid after sanitization.")
            if not sanitized_parts[-1].lower().endswith(".py"):
                sanitized_parts[-1] = f"{sanitized_parts[-1]}.py"
            return "/".join(["workspace", *sanitized_parts])
        except ValueError:
            raise ValueError("Script path must be workspace-relative or inside the current thread workspace.") from None

    return _normalize_workspace_logical_path(
        raw,
        default_root="outputs",
        required_suffix=".py",
        allow_roots=("inputs", "outputs"),
    )


def _workspace_logical_name(resolved_path: Path, *, thread_id: str, root: str) -> str:
    workspace = storage_manager.get_workspace(thread_id)
    root_dir = (workspace / root).resolve()
    try:
        relative = resolved_path.resolve().relative_to(root_dir)
    except Exception:
        return resolved_path.name
    return str(relative).replace("\\", "/")


def _persist_script(
    script_content: str,
    script_name: Optional[str] = None,
    *,
    prefix: str,
    default_root: str = "outputs",
    allow_roots: Tuple[str, ...] = ("outputs",),
    overwrite: bool = False,
) -> Tuple[str, str]:
    thread_id = str(current_thread_id.get() or "debug").strip() or "debug"
    logical_path = _normalize_workspace_logical_path(
        script_name,
        default_root=default_root,
        auto_name_prefix=prefix,
        required_suffix=".py",
        allow_roots=allow_roots,
    )
    script_path = storage_manager.resolve_workspace_relative_path(
        logical_path,
        thread_id=thread_id,
        default_root=default_root,
        create_parent=True,
        allow_memory="memory" in allow_roots,
    )

    if script_path.exists() and not overwrite and script_name:
        logical_path = _normalize_workspace_logical_path(
            None,
            default_root=default_root,
            auto_name_prefix=prefix,
            required_suffix=".py",
            allow_roots=allow_roots,
        )
        script_path = storage_manager.resolve_workspace_relative_path(
            logical_path,
            thread_id=thread_id,
            default_root=default_root,
            create_parent=True,
            allow_memory="memory" in allow_roots,
        )

    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(script_content, encoding="utf-8")
    logical_name = _workspace_logical_name(script_path, thread_id=thread_id, root=default_root)
    if default_root != "outputs":
        logical_name = f"{default_root}/{logical_name}"
    return logical_name, str(script_path)


def _append_run_history(event: Dict[str, Any]) -> None:
    try:
        history_path = Path(storage_manager.resolve_output_path("code_execution_history.jsonl"))
        history_path.parent.mkdir(parents=True, exist_ok=True)
        with history_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False))
            f.write("\n")
    except Exception:
        # History logging is best effort and must not interrupt tool execution.
        pass


def _runtime_code_guide_dir() -> Path:
    env_path = os.getenv("NTL_CODE_GUIDE_RUNTIME_DIR", "").strip()
    if env_path:
        return Path(env_path).resolve()
    repo_root = Path(__file__).resolve().parents[1]
    return (repo_root / "RAG" / "code_guide" / "tools_latest_runtime").resolve()


def _archive_success_script(
    script_content: str,
    *,
    source_tool: str,
    script_name: str,
    script_path: str,
    stdout: str = "",
) -> Dict[str, Any]:
    info: Dict[str, Any] = {"archived": False}
    if not script_content.strip():
        info["archive_error"] = "empty_script_content"
        return info

    try:
        runtime_dir = _runtime_code_guide_dir()
        runtime_dir.mkdir(parents=True, exist_ok=True)
        thread_id = str(current_thread_id.get() or "default")
        normalized = _normalize_whitespace(script_content)
        content_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()

        stem = re.sub(r"[^A-Za-z0-9._-]", "_", Path(script_name).stem).strip("._")
        if not stem:
            stem = "runtime_script"
        archive_script_name = f"{content_hash[:12]}_{stem}.py"
        archive_script_path = runtime_dir / archive_script_name
        if not archive_script_path.exists():
            archive_script_path.write_text(script_content, encoding="utf-8")

        metadata = {
            "timestamp_utc": _timestamp(),
            "source_tool": source_tool,
            "thread_id": thread_id,
            "original_script_name": script_name,
            "original_script_path": script_path,
            "content_hash": content_hash,
            "stdout_excerpt": (stdout or "")[:2000],
        }
        archive_meta_path = runtime_dir / f"{archive_script_path.stem}.meta.json"
        archive_meta_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        manifest_path = runtime_dir / "runtime_manifest.jsonl"
        manifest_entry = dict(metadata)
        manifest_entry["archive_script_name"] = archive_script_name
        manifest_entry["archive_script_path"] = str(archive_script_path)
        manifest_entry["archive_metadata_path"] = str(archive_meta_path)
        with manifest_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(manifest_entry, ensure_ascii=False))
            f.write("\n")

        info.update(
            {
                "archived": True,
                "archive_script_name": archive_script_name,
                "archive_script_path": str(archive_script_path),
                "archive_metadata_path": str(archive_meta_path),
                "archive_manifest_path": str(manifest_path),
                "content_hash": content_hash,
                "source_tool": source_tool,
            }
        )
        return info
    except Exception as exc:  # noqa: BLE001
        info["archive_error"] = str(exc)
        return info


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _path_protocol_mode() -> Literal["sandbox", "hybrid", "resolver"]:
    mode = str(os.getenv("NTL_PATH_PROTOCOL_MODE", "sandbox")).strip().lower()
    if mode not in PATH_PROTOCOL_MODES:
        return "sandbox"
    return mode  # type: ignore[return-value]


def _is_resolver_required(mode: str) -> bool:
    return mode == "resolver"


def _path_protocol_enforcement(mode: str) -> str:
    return "resolver_strict" if _is_resolver_required(mode) else "security_only"


def _detect_mode(code: str) -> str:
    has_gee = "import ee" in code or "ee." in code
    has_local = any(token in code for token in ("rasterio", "geopandas", "gpd.", "shapely"))
    if has_gee and has_local:
        return "hybrid"
    if has_gee:
        return "gee"
    if has_local:
        return "local"
    return "general"


def _extract_gee_assets(code: str) -> List[str]:
    assets: List[str] = []
    pattern = re.compile(r"ee\.(?:ImageCollection|FeatureCollection|Image)\(\s*['\"]([^'\"]+)['\"]\s*\)")
    for m in pattern.finditer(code):
        assets.append(m.group(1))
    return assets


def _extract_selected_bands(code: str) -> List[str]:
    bands: List[str] = []
    # select('band') or select(["band1", "band2"])  -> captures quoted names.
    for m in re.finditer(r"\.select\((.*?)\)", code, flags=re.DOTALL):
        inner = m.group(1)
        quoted = re.findall(r"['\"]([^'\"]+)['\"]", inner)
        bands.extend(quoted)
    return bands


def _check_reduce_region_arguments(code: str, warnings: List[str]) -> None:
    calls = re.findall(r"\.reduceRegions?\((.*?)\)", code, flags=re.DOTALL)
    for call in calls:
        normalized = re.sub(r"\s+", " ", call)
        if not any(token in normalized for token in ("scale=", "crs=", "crsTransform=")):
            warnings.append(
                "GEE reduceRegion/reduceRegions is missing explicit scale/crs. Set scale or crs to avoid implicit projection errors."
            )
        if "maxPixels" not in normalized and "maxPixelsPerRegion" not in normalized:
            warnings.append(
                "GEE reduction call is missing maxPixels/maxPixelsPerRegion. Large regions may fail unexpectedly."
            )
        if "bestEffort" not in normalized and "tileScale" not in normalized:
            warnings.append(
                "Consider bestEffort=True or tileScale>1 for large reductions to improve robustness."
            )


def _preflight_checks(code: str, strict_mode: bool) -> Dict[str, Any]:
    blocking_errors: List[str] = []
    warnings: List[str] = []
    recommendations: List[str] = []

    mode = _detect_mode(code)

    # Syntax check
    try:
        compile(code, "<geocode>", "exec")
    except SyntaxError as exc:
        blocking_errors.append(f"SyntaxError at line {exc.lineno}: {exc.msg}")

    path_mode = _path_protocol_mode()
    resolver_required = _is_resolver_required(path_mode)

    # Path protocol checks (non-blocking unless true security boundary is crossed).
    abs_literal_hits = _detect_absolute_path_literals(code)
    if abs_literal_hits:
        if resolver_required:
            warnings.append(
                "Detected absolute path literal(s). Prefer storage_manager.resolve_input_path/resolve_output_path for portability."
            )
        else:
            warnings.append(
                "Detected absolute path literal(s). Use sandbox-relative paths like inputs/... and outputs/... in thread workspace."
            )

    has_io_relative_literals = bool(re.search(r"['\"](?:inputs|outputs)/", code))
    if has_io_relative_literals and resolver_required:
        msg = "Detected hardcoded 'inputs/' or 'outputs/' path literal. Use storage_manager path resolvers instead."
        warnings.append(msg)

    has_storage_manager_import = "from storage_manager import storage_manager" in code
    has_resolve_input = "resolve_input_path(" in code
    has_resolve_output = "resolve_output_path(" in code

    uses_read_ops = any(re.search(p, code) for p in READ_PATTERNS)
    uses_write_ops = any(re.search(p, code) for p in WRITE_PATTERNS)

    if resolver_required:
        if (uses_read_ops or uses_write_ops) and not has_storage_manager_import:
            msg = "File I/O detected but missing `from storage_manager import storage_manager` import."
            warnings.append(msg)

        if uses_read_ops and not (has_resolve_input or has_resolve_output):
            msg = "Read operations detected but no resolve_input_path()/resolve_output_path() usage found."
            warnings.append(msg)

        if uses_write_ops and not has_resolve_output:
            msg = "Write operations detected but no resolve_output_path() usage found."
            warnings.append(msg)
    elif uses_read_ops or uses_write_ops:
        recommendations.append(
            "Sandbox path protocol active: relative inputs/... and outputs/... are allowed in current thread workspace."
        )
        recommendations.append(
            "Use storage_manager.resolve_* when you explicitly need shared/cross-thread portability."
        )

    has_source_write_target = any(p.search(code) for p in FORBIDDEN_SOURCE_TARGET_PATTERNS)
    if uses_write_ops and has_source_write_target:
        blocking_errors.append(
            "Detected attempt to write repository source/config files. Code_Assistant may only write analysis outputs."
        )

    shared_write_targets = _extract_shared_write_targets(code)
    if shared_write_targets:
        blocking_errors.append(
            "Detected write target under read-only /shared path. "
            f"Use workspace outputs/ or storage_manager.resolve_output_path(...). Targets: {shared_write_targets}"
        )

    if any(p.search(code) for p in FORBIDDEN_COMMAND_PATTERNS):
        blocking_errors.append(
            "Detected prohibited repo-modification command (git/system patch). Only geospatial analysis execution is allowed."
        )

    # GEE checks
    assets = _extract_gee_assets(code)
    selected_bands = _extract_selected_bands(code)
    if mode in {"gee", "hybrid"}:
        has_bbox = "ee.Geometry.Rectangle(" in code
        has_admin_boundary = any(
            token in code
            for token in (
                "get_administrative_division",
                "get_administrative_division_geoboundaries",
                "WM/geoLab/geoBoundaries/600/ADM0",
                "WM/geoLab/geoBoundaries/600/ADM1",
                "WM/geoLab/geoBoundaries/600/ADM2",
                "WM/geoLab/geoBoundaries/600/ADM3",
                "WM/geoLab/geoBoundaries/600/ADM4",
                "projects/empyrean-caster-430308-m2/assets/province",
                "projects/empyrean-caster-430308-m2/assets/city",
                "projects/empyrean-caster-430308-m2/assets/county",
            )
        )
        has_user_bbox_override = "AOI_CONFIRMED_BY_USER" in code

        if has_bbox and not has_admin_boundary and not has_user_bbox_override:
            msg = (
                "Detected bbox AOI without administrative-boundary confirmation. "
                "Use NTL_Engineer-confirmed boundary file/asset (via upstream data retrieval), "
                "or add `# AOI_CONFIRMED_BY_USER` if user explicitly provided coordinates."
            )
            warnings.append(msg)

        if "ee.Initialize(" not in code:
            warnings.append("GEE code found but no explicit ee.Initialize(...) call. Add initialization with project id.")

        if "ee.Initialize(" in code and "project=" not in code:
            warnings.append(
                f"ee.Initialize() missing explicit project parameter. Recommended: ee.Initialize(project='{_gee_project_id()}')."
            )

        for asset in assets:
            is_known_public = asset in KNOWN_GEE_DATASETS or asset.startswith(KNOWN_GEE_PREFIXES) or asset.startswith(KNOWN_GEE_PUBLIC_COLLECTION_PREFIXES)
            if not is_known_public:
                warnings.append(f"Unrecognized GEE asset id: {asset}. Verify availability and access permissions.")

        for asset, allowed_bands in KNOWN_GEE_DATASETS.items():
            if asset in assets and selected_bands:
                unknown = [b for b in selected_bands if b not in allowed_bands]
                if unknown:
                    warnings.append(
                        f"Selected band(s) {unknown} may not belong to {asset}. Expected one of {allowed_bands}."
                    )

        _check_reduce_region_arguments(code, warnings)

    # Local geospatial checks
    if "gdf.area" in code or ".area" in code and "gpd." in code:
        if "to_crs(" not in code:
            warnings.append("Area/length calculation detected without to_crs(). Reproject to a projected CRS before metric calculations.")

    if "sjoin(" in code and "predicate=" not in code and "op=" in code:
        warnings.append("GeoPandas sjoin uses deprecated `op=` in newer versions. Prefer `predicate=`.")

    if "is_valid" in code and "make_valid" not in code and "buffer(0)" not in code:
        recommendations.append("If invalid geometries are found, repair with shapely.make_valid (preferred) or geometry.buffer(0).")

    if mode in {"gee", "hybrid"} and "getInfo()" in code and "for " in code:
        warnings.append("Potential client-side loop with getInfo(). Prefer server-side map/reduce patterns in Earth Engine.")

    if mode in {"gee", "hybrid"}:
        recommendations.append("Prefer server-side aggregation (reduceRegion/reduceRegions) and export only final CSV values.")

    warnings = _dedupe_ordered(warnings)
    recommendations = _dedupe_ordered(recommendations)

    score = max(0, 100 - 30 * len(blocking_errors) - 8 * len(warnings))
    return {
        "mode": mode,
        "path_protocol_mode": path_mode,
        "path_protocol_enforcement": _path_protocol_enforcement(path_mode),
        "strict_mode": strict_mode,
        "score": score,
        "blocking_errors": blocking_errors,
        "warnings": warnings,
        "recommendations": recommendations,
        "detected": {
            "assets": assets,
            "selected_bands": selected_bands,
            "uses_storage_manager_import": has_storage_manager_import,
            "uses_resolve_input_path": has_resolve_input,
            "uses_resolve_output_path": has_resolve_output,
        },
    }


def _derive_fix_suggestions(error_type: Optional[str], error_message: Optional[str]) -> List[str]:
    msg = (error_message or "").lower()
    et = (error_type or "").lower()
    fixes: List[str] = []
    path_mode = _path_protocol_mode()

    if "filenotfounderror" in et or "no such file" in msg:
        if _is_resolver_required(path_mode):
            fixes.append("Use storage_manager.resolve_input_path() for existing input files.")
            fixes.append("For files generated in this session, read via storage_manager.resolve_output_path().")
        else:
            fixes.append("Use sandbox-relative paths from workspace root, e.g. inputs/xxx and outputs/yyy.")
            fixes.append("If portability is needed, switch to storage_manager.resolve_input_path/resolve_output_path.")

    if "emptyresulterror" in et or "0 regions" in msg or "rows=0" in msg:
        fixes.append("Treat empty geospatial results as failed validation, not success.")
        fixes.append("For reduceRegions outputs, verify boundary feature count and read reducer output property `mean`.")
        fixes.append("For China 34 province-level tasks, validate exactly 34 rows including Taiwan, Hong Kong, and Macau.")

    if "user_project_denied" in msg or "serviceusage.serviceusageconsumer" in msg:
        fixes.append(
            "GEE project authorization failed. Use an authorized GEE_DEFAULT_PROJECT_ID or grant the active account "
            "serviceusage.serviceUsageConsumer on the configured Google Cloud project."
        )
        fixes.append("Do not change datasets, bands, or analysis logic to work around this IAM/project error.")
    elif "eeexception" in et or "earth engine" in msg:
        fixes.append(f"Ensure ee.Initialize(project='{_gee_project_id()}') is called before GEE operations.")
        fixes.append("Verify asset IDs and band names against Earth Engine catalog before execution.")

    if "permission" in msg or "access" in msg:
        fixes.append("Check account permissions for the target GEE asset/project.")

    if "crs" in msg or "projection" in msg:
        fixes.append("Align CRS between raster/vector layers (e.g., vector.to_crs(raster_crs)).")

    if "memory" in msg or "too many pixels" in msg:
        fixes.append("For GEE reductions, set scale/maxPixels and consider bestEffort/tileScale.")
        fixes.append("For local rasters, process in chunks/windows instead of reading full arrays.")

    if "bbox aoi" in msg or "boundary" in msg:
        fixes.append(
            "Escalate to NTL_Engineer to obtain a verified administrative boundary "
            "(source, CRS, bounds) from upstream retrieval."
        )
        fixes.append("Avoid self-defined bbox for named regions unless user explicitly provided coordinates.")

    if not fixes:
        fixes.append("Run GeoCode_COT_Validation_tool with a smaller minimal block to isolate the failing step.")

    return fixes


def _build_error_handling_policy(
    error_type: Optional[str],
    error_message: Optional[str],
    *,
    preflight: Optional[Dict[str, Any]] = None,
    fix_suggestions: Optional[List[str]] = None,
) -> Dict[str, Any]:
    et = (error_type or "").lower()
    msg = (error_message or "").lower()
    suggestions = [s.lower() for s in (fix_suggestions or [])]
    preflight_errors = [str(x).lower() for x in (preflight or {}).get("blocking_errors", [])]

    hard_markers = [
        "boundary",
        "administrative",
        "permission",
        "access denied",
        "user_project_denied",
        "serviceusage.serviceusageconsumer",
        "serviceusageconsumer",
        "project",
        "authentication",
        "authorization",
        "credential",
        "asset",
        "dataset",
        "request data_searcher",
        "not found in current workspace",
        "missing input",
    ]

    simple_error_types = {
        "syntaxerror",
        "nameerror",
        "typeerror",
        "valueerror",
        "keyerror",
        "indexerror",
        "attributeerror",
        "filenotfounderror",
    }
    hard_error_types = {
        "preflighterror",
        "eeexception",
        "httperror",
        "permissionerror",
        "scriptpersisterror",
        "scriptnotfounderror",
    }

    marker_hit = any(marker in msg for marker in hard_markers)
    marker_hit = marker_hit or any(any(marker in s for marker in hard_markers) for s in suggestions)
    marker_hit = marker_hit or any(any(marker in e for marker in hard_markers) for e in preflight_errors)

    is_hard = et in hard_error_types or marker_hit
    if et in simple_error_types and not marker_hit:
        is_hard = False

    if is_hard:
        return {
            "severity": "hard",
            "should_handoff_to_engineer": True,
            "max_self_retries": 0,
            "recommendation": "handoff_to_engineer_with_context",
            "reason": (
                "The failure likely requires strategy/data/boundary or permission decision outside local code debugging."
            ),
            "decision_options": [
                "set_or_replace_GEE_DEFAULT_PROJECT_ID_with_an_authorized_project",
                "grant_serviceusage.serviceUsageConsumer_to_the_active_GEE_credential",
                "enable_required_Google_Cloud_and_Earth_Engine_APIs_for_the_project",
                "ask_engineer_for_additional_data_or_boundary_confirmation",
                "ask_engineer_to_switch_method_or_toolchain",
                "ask_engineer_to_request_user_clarification_if_constraints_are_missing",
            ],
        }

    return {
        "severity": "simple",
        "should_handoff_to_engineer": False,
        "max_self_retries": 1,
        "recommendation": "self_debug_then_retry",
        "reason": "The failure appears code-local and can usually be fixed with limited self-debug iterations.",
        "decision_options": [
            "fix_code_and_retry_within_budget",
            "if_still_failing_after_budget_then_handoff_to_engineer",
        ],
    }


def _get_thread_context() -> Dict[str, Any]:
    thread_id = current_thread_id.get() or "default"
    if thread_id not in GLOBAL_EXEC_CONTEXTS:
        GLOBAL_EXEC_CONTEXTS[thread_id] = {"__name__": "__main__"}
    return GLOBAL_EXEC_CONTEXTS[thread_id]


def _bind_thread_from_config(config: Optional[RunnableConfig]) -> Optional[Any]:
    """Bind current thread context from LangGraph/LangChain runnable config."""
    runtime_config: Optional[RunnableConfig] = None
    if isinstance(config, dict):
        runtime_config = config
    else:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if not isinstance(runtime_config, dict):
        return None
    try:
        thread_id = storage_manager.get_thread_id_from_config(runtime_config)
    except Exception:
        return None
    if not thread_id:
        return None
    return current_thread_id.set(str(thread_id))


def _execute_code(code_block: str) -> Tuple[bool, str, Optional[str], Optional[str], Optional[str]]:
    if _should_use_subprocess_sandbox():
        return _execute_code_in_subprocess_sandbox(code_block)

    user_globals = _get_thread_context()
    user_globals["_active_gee_credentials"] = _active_gee_credentials
    bootstrap = (
        "import ee\n"
        f"project_id = {_gee_project_id()!r}\n"
        "ntl_ee_credentials = _active_gee_credentials()\n"
        "try:\n"
        "    ee.Initialize(credentials=ntl_ee_credentials, project=project_id) if ntl_ee_credentials else ee.Initialize(project=project_id)\n"
        "except Exception:\n"
        "    pass\n"
    )
    code_block = _patch_ee_initialize_for_active_credentials(code_block)

    buf = io.StringIO()
    try:
        with _thread_bound_storage_paths(str(current_thread_id.get() or "debug")):
            with _thread_workspace_cwd(str(current_thread_id.get() or "debug")):
                with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                    exec(bootstrap, user_globals)
                    exec(code_block, user_globals)
        logs = _sanitize_ansi(buf.getvalue())
        return True, logs, None, None, None
    except Exception as exc:  # noqa: BLE001
        logs = _sanitize_ansi(buf.getvalue())
        return False, logs, type(exc).__name__, str(exc), traceback.format_exc()
    finally:
        buf.close()


def _should_use_subprocess_sandbox() -> bool:
    # Default ON for safer runtime execution. Set NTL_EXEC_SANDBOX=0 to disable.
    raw = str(os.getenv("NTL_EXEC_SANDBOX", "1")).strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _sandbox_timeout_seconds() -> int:
    raw = str(os.getenv("NTL_EXEC_SANDBOX_TIMEOUT_S", "900")).strip()
    try:
        value = int(raw)
    except Exception:
        return 900
    return max(30, min(value, 3600))


def _build_sandbox_env(thread_id: str, code_path: Path) -> Dict[str, str]:
    allowed_keys = {
        "PATH",
        "PATHEXT",
        "PYTHONPATH",
        "PYTHONHOME",
        "CONDA_PREFIX",
        "CONDA_DEFAULT_ENV",
        "VIRTUAL_ENV",
        "SYSTEMROOT",
        "WINDIR",
        "TMP",
        "TEMP",
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_CLOUD_PROJECT",
        "EE_PROJECT",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    }
    env = {k: v for k, v in os.environ.items() if k in allowed_keys}
    repo_root = str(Path(__file__).resolve().parent.parent)
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = repo_root if not existing_pythonpath else f"{repo_root}{os.pathsep}{existing_pythonpath}"
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONUTF8"] = "1"
    env["NTL_THREAD_ID"] = str(thread_id)
    env["NTL_CODE_PATH"] = str(code_path)
    active_project_id = str(current_gee_project_id.get() or "").strip()
    if active_project_id:
        env["NTL_ACTIVE_GEE_PROJECT_ID"] = active_project_id
    encrypted_refresh_token = str(current_gee_encrypted_refresh_token.get() or "").strip()
    if encrypted_refresh_token:
        env["NTL_ACTIVE_GEE_ENCRYPTED_REFRESH_TOKEN"] = encrypted_refresh_token
    token_scopes = str(current_gee_token_scopes.get() or "").strip()
    if token_scopes:
        env["NTL_ACTIVE_GEE_TOKEN_SCOPES"] = token_scopes
    return env


def _extract_error_type_and_message(logs: str) -> Tuple[Optional[str], Optional[str]]:
    text = str(logs or "").strip()
    if not text:
        return None, None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None, None
    last = lines[-1]
    if ":" in last:
        etype, emsg = last.split(":", 1)
        etype = etype.strip() or None
        emsg = emsg.strip() or None
        return etype, emsg
    return "RuntimeError", last


def _execute_code_in_subprocess_sandbox(
    code_block: str,
) -> Tuple[bool, str, Optional[str], Optional[str], Optional[str]]:
    thread_id = str(current_thread_id.get() or "debug").strip() or "debug"
    repo_root = Path(__file__).resolve().parent.parent
    workspace = storage_manager.get_workspace(thread_id)
    memory_dir = workspace / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)

    code_path = memory_dir / f"sandbox_exec_{uuid4().hex}.py"
    runner_path = memory_dir / f"sandbox_runner_{uuid4().hex}.py"

    bootstrap = (
        "import ee\n"
        "import os\n"
        f"project_id = {_gee_project_id()!r}\n"
        "ntl_ee_credentials = None\n"
        "encrypted = os.environ.get('NTL_ACTIVE_GEE_ENCRYPTED_REFRESH_TOKEN', '').strip()\n"
        "if encrypted:\n"
        "    import gee_auth\n"
        "    refresh_token = gee_auth.decrypt_refresh_token(encrypted)\n"
        "    scopes = os.environ.get('NTL_ACTIVE_GEE_TOKEN_SCOPES', '').split() or None\n"
        "    ntl_ee_credentials = gee_auth.credentials_from_refresh_token(refresh_token, scopes=scopes)\n"
        "try:\n"
        "    ee.Initialize(credentials=ntl_ee_credentials, project=project_id) if ntl_ee_credentials else ee.Initialize(project=project_id)\n"
        "except Exception:\n"
        "    pass\n"
    )
    code_block = _patch_ee_initialize_for_active_credentials(code_block)
    runner = (
        "import os\n"
        "import traceback\n"
        "from storage_manager import current_thread_id\n"
        "tid = os.environ.get('NTL_THREAD_ID', 'debug')\n"
        "current_thread_id.set(tid)\n"
        "code_path = os.environ['NTL_CODE_PATH']\n"
        "with open(code_path, 'r', encoding='utf-8') as f:\n"
        "    code_block = f.read()\n"
        "user_globals = {'__name__': '__main__'}\n"
        f"bootstrap = {bootstrap!r}\n"
        "try:\n"
        "    exec(bootstrap, user_globals)\n"
        "    exec(code_block, user_globals)\n"
        "except Exception:\n"
        "    traceback.print_exc()\n"
        "    raise\n"
    )

    try:
        code_path.write_text(code_block, encoding="utf-8")
        runner_path.write_text(runner, encoding="utf-8")

        env = _build_sandbox_env(thread_id, code_path)
        completed = subprocess.run(
            [sys.executable, str(runner_path)],
            # Run from thread workspace so legacy relative paths (inputs/outputs)
            # resolve into thread-isolated directories.
            cwd=str(workspace),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_sandbox_timeout_seconds(),
            env=env,
        )
        logs = _sanitize_ansi((completed.stdout or "") + (completed.stderr or ""))
        if completed.returncode == 0:
            return True, logs, None, None, None
        etype, emsg = _extract_error_type_and_message(logs)
        return False, logs, etype or "SubprocessExecutionError", emsg, logs
    except subprocess.TimeoutExpired as exc:
        timeout_msg = f"Subprocess sandbox timed out after {_sandbox_timeout_seconds()} seconds."
        return False, timeout_msg, "TimeoutError", str(exc), traceback.format_exc()
    except Exception as exc:  # noqa: BLE001
        return False, "", type(exc).__name__, str(exc), traceback.format_exc()
    finally:
        with contextlib.suppress(Exception):
            code_path.unlink(missing_ok=True)
        with contextlib.suppress(Exception):
            runner_path.unlink(missing_ok=True)


def GEE_GeoCode_COT_Validation(
    code_block: str,
    strict_mode: bool = False,
    config: Optional[RunnableConfig] = None,
) -> str:
    token = _bind_thread_from_config(config)
    try:
        path_mode = _path_protocol_mode()
        path_protocol_enforcement = _path_protocol_enforcement(path_mode)
        thread_id = str(current_thread_id.get() or "debug").strip() or "debug"
        runtime_path_rewrite = _empty_runtime_path_rewrite_report()
        try:
            block_script_name, block_script_path = _persist_script(
                code_block,
                prefix="cot_block",
                default_root="memory",
                allow_roots=("memory",),
                overwrite=False,
            )
        except Exception as exc:  # noqa: BLE001
            policy = _build_error_handling_policy(
                "ScriptPersistError",
                str(exc),
                preflight=None,
                fix_suggestions=None,
            )
            return json.dumps(
                {
                    "status": "fail",
                    "stdout": "",
                    "error_type": "ScriptPersistError",
                    "error_message": str(exc),
                    "traceback": traceback.format_exc(),
                    "code_block": code_block,
                    "path_protocol_mode": path_mode,
                    "path_protocol_enforcement": path_protocol_enforcement,
                    "error_handling_policy": policy,
                    "execution_skipped": True,
                    "runtime_path_rewrite": runtime_path_rewrite,
                },
                indent=2,
                ensure_ascii=False,
            )

        preflight = _preflight_checks(code_block, strict_mode=strict_mode)
        # Security and integrity blocking errors are always enforced.
        preflight_blocking_enabled = bool(preflight.get("blocking_errors"))

        if preflight["blocking_errors"] and preflight_blocking_enabled:
            policy = _build_error_handling_policy(
                "PreflightError",
                preflight["blocking_errors"][0],
                preflight=preflight,
                fix_suggestions=preflight["recommendations"],
            )
            report = {
                "status": "fail",
                "stdout": "",
                "error_type": "PreflightError",
                "error_message": preflight["blocking_errors"][0],
                "traceback": None,
                "code_block": code_block,
                "script_name": block_script_name,
                "script_path": block_script_path,
                "preflight": preflight,
                "fix_suggestions": preflight["recommendations"],
                "error_handling_policy": policy,
                "path_protocol_mode": path_mode,
                "path_protocol_enforcement": path_protocol_enforcement,
                "execution_skipped": True,
                "runtime_path_rewrite": runtime_path_rewrite,
            }
            _append_run_history(
                {
                    "timestamp": _timestamp(),
                    "tool": "GeoCode_COT_Validation_tool",
                    "status": "fail",
                    "reason": "preflight",
                    "script_name": block_script_name,
                    "script_path": block_script_path,
                    "thread_id": current_thread_id.get(),
                }
            )
            return json.dumps(report, indent=2, ensure_ascii=False)
        runtime_code, runtime_path_rewrite = _rewrite_virtual_paths_for_runtime(code_block, thread_id=thread_id)
        ok, logs, etype, emsg, tb = _execute_code(runtime_code)
        stdout_quality_audit = _build_stdout_quality_audit(logs)
        if ok and not stdout_quality_audit.get("pass", True):
            ok = False
            etype = str(stdout_quality_audit.get("error_type") or "EmptyResultError")
            emsg = str(stdout_quality_audit.get("error_message") or "Execution produced an empty result.")
            tb = None
        fix_suggestions = _derive_fix_suggestions(etype, emsg) + preflight["recommendations"]
        report = {
            "status": "pass" if ok else "fail",
            "stdout": logs,
            "error_type": etype,
            "error_message": emsg,
            "traceback": tb,
            "code_block": code_block,
            "script_name": block_script_name,
            "script_path": block_script_path,
            "preflight": preflight,
            "fix_suggestions": fix_suggestions,
            "path_protocol_mode": path_mode,
            "path_protocol_enforcement": path_protocol_enforcement,
            "error_handling_policy": (
                _build_error_handling_policy(etype, emsg, preflight=preflight, fix_suggestions=fix_suggestions)
                if not ok
                else None
            ),
            "execution_skipped": False,
            "runtime_path_rewrite": runtime_path_rewrite,
            "stdout_quality_audit": stdout_quality_audit,
        }
        _append_run_history(
            {
                "timestamp": _timestamp(),
                "tool": "GeoCode_COT_Validation_tool",
                "status": report["status"],
                "script_name": block_script_name,
                "script_path": block_script_path,
                "thread_id": current_thread_id.get(),
            }
        )
        return json.dumps(report, indent=2, ensure_ascii=False)
    finally:
        if token is not None:
            current_thread_id.reset(token)


def execute_geospatial_script(
    script_name: str,
    script_location: str = "auto",
    strict_mode: bool = False,
    force_execute: bool = False,
    config: Optional[RunnableConfig] = None,
) -> str:
    token = _bind_thread_from_config(config)
    try:
        path_mode = _path_protocol_mode()
        path_protocol_enforcement = _path_protocol_enforcement(path_mode)
        # strict_mode controls audit strictness; hard safety boundaries are always blocked.
        enforced_strict_mode = bool(strict_mode)
        thread_id = str(current_thread_id.get() or "debug").strip() or "debug"
        empty_audit = _build_artifact_audit("", thread_id=thread_id)
        no_runtime_rewrite = _empty_runtime_path_rewrite_report()

        if not script_name or not script_name.strip():
            policy = _build_error_handling_policy(
                "InvalidScriptName",
                "script_name is required.",
                preflight=None,
                fix_suggestions=None,
            )
            return json.dumps(
                {
                    "status": "fail",
                    "error_type": "InvalidScriptName",
                    "error_message": "script_name is required.",
                    "error_handling_policy": policy,
                    "path_protocol_mode": path_mode,
                    "path_protocol_enforcement": path_protocol_enforcement,
                    "artifact_audit": empty_audit,
                    "runtime_path_rewrite": no_runtime_rewrite,
                },
                indent=2,
                ensure_ascii=False,
            )
        requested_script = str(script_name or "").strip()
        request_for_root_detection = requested_script.replace("\\", "/").lstrip("/")
        has_explicit_script_root = (
            request_for_root_detection.startswith(("outputs/", "inputs/", "data/processed/", "data/raw/"))
            or requested_script.replace("\\", "/").startswith(
                ("/outputs/", "/inputs/", "/data/processed/", "/data/raw/")
            )
            or bool(re.match(r"^[A-Za-z]:[\\/]", requested_script))
            or (requested_script.startswith(("/", "\\")) and not requested_script.startswith(("/data/", "/outputs/", "/inputs/")))
        )
        try:
            normalized_request = _normalize_script_request_path(
                requested_script,
                thread_id=thread_id,
            )
        except Exception as exc:  # noqa: BLE001
            policy = _build_error_handling_policy(
                "InvalidScriptName",
                str(exc),
                preflight=None,
                fix_suggestions=None,
            )
            return json.dumps(
                {
                    "status": "fail",
                    "error_type": "InvalidScriptName",
                    "error_message": str(exc),
                    "error_handling_policy": policy,
                    "path_protocol_mode": path_mode,
                    "path_protocol_enforcement": path_protocol_enforcement,
                    "artifact_audit": empty_audit,
                    "runtime_path_rewrite": no_runtime_rewrite,
                },
                indent=2,
                ensure_ascii=False,
            )

        requested_root, _, logical_script_name = normalized_request.partition("/")
        requested_location = str(script_location or "auto").strip().lower()
        if requested_location not in {"auto", "inputs", "outputs"}:
            requested_location = "auto"
        effective_location = (
            requested_root
            if has_explicit_script_root and requested_root in {"inputs", "outputs", "workspace"}
            else requested_location
        )
        thread_ctx = _get_thread_context()
        workspace = storage_manager.get_workspace(thread_id)

        output_script_path = storage_manager.resolve_workspace_relative_path(
            logical_script_name,
            thread_id=thread_id,
            default_root="outputs",
            allow_memory=False,
        )
        input_script_path = storage_manager.resolve_workspace_relative_path(
            logical_script_name,
            thread_id=thread_id,
            default_root="inputs",
            allow_memory=False,
        )
        candidates = {
            "outputs": output_script_path,
            "inputs": input_script_path,
        }
        workspace_script_path: Optional[Path] = None
        try:
            workspace_root = workspace.resolve()
            workspace_candidate = (workspace / logical_script_name).resolve()
            if (
                workspace_candidate != (workspace_root / "inputs").resolve()
                and workspace_candidate != (workspace_root / "outputs").resolve()
                and str(workspace_candidate).startswith(str(workspace_root) + os.sep)
                and not str(workspace_candidate).startswith(str((workspace_root / "inputs").resolve()) + os.sep)
                and not str(workspace_candidate).startswith(str((workspace_root / "outputs").resolve()) + os.sep)
                and not str(workspace_candidate).startswith(str((workspace_root / "memory").resolve()) + os.sep)
            ):
                workspace_script_path = workspace_candidate
                candidates["workspace"] = workspace_script_path
        except Exception:
            workspace_script_path = None
        if effective_location == "outputs":
            ordered_locations = ["outputs"]
        elif effective_location == "inputs":
            ordered_locations = ["inputs"]
        elif effective_location == "workspace":
            ordered_locations = ["workspace"]
        else:
            ordered_locations = ["outputs", "inputs", "workspace"]

        script_path: Optional[Path] = None
        resolved_location = ""
        for loc in ordered_locations:
            candidate = candidates.get(loc)
            if candidate is not None and candidate.exists():
                script_path = candidate
                resolved_location = loc
                break

        if script_path is None:
            available_scripts = [
                str(p.relative_to(workspace / "outputs")).replace("\\", "/")
                for p in sorted((workspace / "outputs").rglob("*.py"))
            ]
            available_scripts.extend(
                [
                    str(p.relative_to(workspace / "inputs")).replace("\\", "/")
                    for p in sorted((workspace / "inputs").rglob("*.py"))
                ]
            )
            available_scripts.extend(
                [
                    str(p.relative_to(workspace)).replace("\\", "/")
                    for p in sorted(workspace.glob("*.py"))
                ]
            )
            last_saved_script_name = str(thread_ctx.get("__ntl_last_saved_script_name") or "").strip() or None
            policy = _build_error_handling_policy(
                "ScriptNotFoundError",
                f"Script '{logical_script_name}' was not found in current workspace inputs/outputs.",
                preflight=None,
                fix_suggestions=None,
            )
            return json.dumps(
                {
                    "status": "fail",
                    "error_type": "ScriptNotFoundError",
                    "error_message": f"Script '{logical_script_name}' was not found in current workspace inputs/outputs.",
                    "script_name": logical_script_name,
                    "script_location": effective_location,
                    "available_scripts": available_scripts,
                    "resolved_candidates": {k: str(v) for k, v in candidates.items()},
                    "last_saved_script_name": last_saved_script_name,
                    "recovery_suggestion": (
                        "Persist the draft script first (write/save tool), then execute by exact saved filename."
                    ),
                    "error_handling_policy": policy,
                    "path_protocol_mode": path_mode,
                    "path_protocol_enforcement": path_protocol_enforcement,
                    "artifact_audit": empty_audit,
                    "runtime_path_rewrite": no_runtime_rewrite,
                },
                indent=2,
                ensure_ascii=False,
            )

        script_content = script_path.read_text(encoding="utf-8-sig")
        if not script_content.strip():
            policy = _build_error_handling_policy(
                "EmptyScriptError",
                f"Script '{logical_script_name}' is empty.",
                preflight=None,
                fix_suggestions=["Write a complete Python script before execution."],
            )
            result = {
                "status": "fail",
                "stdout": "",
                "error_type": "EmptyScriptError",
                "error_message": f"Script '{logical_script_name}' is empty.",
                "traceback": None,
                "script_name": logical_script_name,
                "script_path": str(script_path),
                "script_location": resolved_location or effective_location,
                "code": script_content,
                "fix_suggestions": ["Write a complete Python script before execution."],
                "error_handling_policy": policy,
                "path_protocol_mode": path_mode,
                "path_protocol_enforcement": path_protocol_enforcement,
                "execution_skipped": True,
                "artifact_audit": empty_audit,
                "runtime_path_rewrite": no_runtime_rewrite,
            }
            _append_run_history(
                {
                    "timestamp": _timestamp(),
                    "tool": "execute_geospatial_script_tool",
                    "status": "fail",
                    "reason": "empty_script",
                    "script_name": logical_script_name,
                    "script_path": str(script_path),
                    "thread_id": current_thread_id.get(),
                }
            )
            return json.dumps(result, indent=2, ensure_ascii=False)
        preflight = _preflight_checks(script_content, strict_mode=enforced_strict_mode)
        # Security and integrity blocking errors are always enforced.
        preflight_blocking_enabled = bool(preflight.get("blocking_errors"))
        normalized_script_hash = hashlib.sha256(_normalize_whitespace(script_content).encode("utf-8")).hexdigest()

        if (
            (not bool(force_execute))
            and resolved_location in {"outputs", "inputs"}
            and
            thread_ctx.get("__ntl_last_executed_success_hash") == normalized_script_hash
            and thread_ctx.get("__ntl_last_executed_success_script_path") == str(script_path)
        ):
            # Avoid repeated execution of unchanged successful scripts.
            result = {
                "status": "success",
                "stdout": "[dedupe] Identical script already executed successfully in this thread. Skipped re-execution.",
                "script_name": logical_script_name,
                "script_path": str(script_path),
                "script_location": resolved_location or effective_location,
                "already_executed": True,
                "execution_skipped": True,
                "code": script_content,
                "preflight": preflight,
                "next_action_hint": "return_to_supervisor_auto",
                "path_protocol_mode": path_mode,
                "path_protocol_enforcement": path_protocol_enforcement,
                "artifact_audit": empty_audit,
                "runtime_path_rewrite": no_runtime_rewrite,
            }
            _append_run_history(
                {
                    "timestamp": _timestamp(),
                    "tool": "execute_geospatial_script_tool",
                    "status": "success_cached",
                    "script_name": logical_script_name,
                    "script_path": str(script_path),
                    "thread_id": current_thread_id.get(),
                }
            )
            return json.dumps(result, indent=2, ensure_ascii=False)

        if preflight["blocking_errors"] and preflight_blocking_enabled:
            policy = _build_error_handling_policy(
                "PreflightError",
                preflight["blocking_errors"][0],
                preflight=preflight,
                fix_suggestions=preflight["recommendations"],
            )
            result = {
                "status": "fail",
                "stdout": "",
                "error_type": "PreflightError",
                "error_message": preflight["blocking_errors"][0],
                "traceback": None,
                "script_name": logical_script_name,
                "script_path": str(script_path),
                "script_location": resolved_location or effective_location,
                "code": script_content,
                "preflight": preflight,
                "fix_suggestions": preflight["recommendations"],
                "error_handling_policy": policy,
                "path_protocol_mode": path_mode,
                "path_protocol_enforcement": path_protocol_enforcement,
                "execution_skipped": True,
                "artifact_audit": empty_audit,
                "runtime_path_rewrite": no_runtime_rewrite,
            }
            _append_run_history(
                {
                    "timestamp": _timestamp(),
                    "tool": "execute_geospatial_script_tool",
                    "status": "fail",
                    "reason": "preflight",
                    "script_name": logical_script_name,
                    "script_path": str(script_path),
                    "thread_id": current_thread_id.get(),
                }
            )
            return json.dumps(result, indent=2, ensure_ascii=False)

        runtime_code, runtime_path_rewrite = _rewrite_virtual_paths_for_runtime(script_content, thread_id=thread_id)
        ok, logs, etype, emsg, tb = _execute_code(runtime_code)
        artifact_audit = _build_artifact_audit(logs, thread_id=thread_id)
        stdout_quality_audit = _build_stdout_quality_audit(logs)
        if ok and not stdout_quality_audit.get("pass", True):
            ok = False
            etype = str(stdout_quality_audit.get("error_type") or "EmptyResultError")
            emsg = str(stdout_quality_audit.get("error_message") or "Execution produced an empty result.")
            tb = None
        if ok:
            warning_messages: List[str] = []
            warning_policy: Optional[Dict[str, Any]] = None
            if not artifact_audit.get("pass", True):
                out_paths = artifact_audit.get("out_of_workspace_paths") or []
                migrated_paths, migration_failures = _auto_migrate_cross_workspace_outputs(out_paths, thread_id)
                artifact_audit["auto_migration_attempted"] = True
                artifact_audit["migrated_paths"] = migrated_paths
                artifact_audit["migration_failures"] = migration_failures
                artifact_audit["auto_migration_success"] = bool(out_paths) and not migration_failures
                if artifact_audit["auto_migration_success"]:
                    artifact_audit["pass"] = True
                else:
                    message = (
                        "Detected output paths outside current thread workspace outputs and auto-migration failed. "
                        "Use sandbox-relative outputs/<filename> or storage_manager.resolve_output_path(...) for every generated file."
                    )
                    if out_paths:
                        message = f"{message} Offending paths: {out_paths}"
                    fix_suggestions = [
                        "Replace hardcoded absolute output paths with sandbox-relative outputs/<filename>.",
                        "Or use storage_manager.resolve_output_path('filename') when portability is required.",
                        "Do not write to other thread folders such as user_data/debug/outputs.",
                    ]
                    warning_policy = _build_error_handling_policy(
                        "CrossWorkspaceOutputWarning",
                        message,
                        preflight=preflight,
                        fix_suggestions=fix_suggestions,
                    )
                    warning_messages.append(
                        "Execution succeeded, but detected output paths outside current workspace and auto-migration failed."
                    )
                    warning_messages.append(message)

            thread_ctx["__ntl_last_executed_success_hash"] = normalized_script_hash
            thread_ctx["__ntl_last_executed_success_script_name"] = logical_script_name
            thread_ctx["__ntl_last_executed_success_script_path"] = str(script_path)
            fail_counts = thread_ctx.setdefault("__ntl_execute_failure_signature_counts", {})
            for sig in list(fail_counts.keys()):
                if sig.startswith(f"{normalized_script_hash}:"):
                    fail_counts.pop(sig, None)
            archive_info = _archive_success_script(
                script_content,
                source_tool="execute_geospatial_script_tool",
                script_name=logical_script_name,
                script_path=str(script_path),
                stdout=logs,
            )
            result = {
                "status": "success_with_warnings" if warning_messages else "success",
                "stdout": logs,
                "script_name": logical_script_name,
                "script_path": str(script_path),
                "script_location": resolved_location or effective_location,
                "code_guide_archive": archive_info,
                "code": script_content,
                "preflight": preflight,
                "execution_skipped": False,
                "path_protocol_mode": path_mode,
                "path_protocol_enforcement": path_protocol_enforcement,
                "cross_workspace_recovered": bool(artifact_audit.get("auto_migration_success")),
                "auto_migrated_files": list(artifact_audit.get("migrated_paths") or []),
                "recovery_note": (
                    "Cross-thread outputs were auto-migrated to current thread outputs."
                    if artifact_audit.get("auto_migration_success")
                    else ""
                ),
                "artifact_audit": artifact_audit,
                "runtime_path_rewrite": runtime_path_rewrite,
                "stdout_quality_audit": stdout_quality_audit,
            }
            if warning_messages:
                result["warnings"] = warning_messages
                result["warning_type"] = "CrossWorkspaceOutputWarning"
                if warning_policy:
                    result["warning_handling_policy"] = warning_policy
            _append_run_history(
                {
                    "timestamp": _timestamp(),
                    "tool": "execute_geospatial_script_tool",
                    "status": (
                        "success_recovered"
                        if artifact_audit.get("auto_migration_success")
                        else ("success_warning" if warning_messages else "success")
                    ),
                    "script_name": logical_script_name,
                    "script_path": str(script_path),
                    "code_guide_archive": archive_info,
                    "thread_id": current_thread_id.get(),
                }
            )
            return json.dumps(result, indent=2, ensure_ascii=False)

        fix_suggestions = _derive_fix_suggestions(etype, emsg) + preflight["recommendations"]
        failure_signature = (
            f"{normalized_script_hash}:{etype or 'UnknownError'}:{_normalize_whitespace(emsg or '')[:180]}"
        )
        fail_counts = thread_ctx.setdefault("__ntl_execute_failure_signature_counts", {})
        repeated_failure_count = int(fail_counts.get(failure_signature, 0)) + 1
        fail_counts[failure_signature] = repeated_failure_count
        policy = _build_error_handling_policy(
            etype,
            emsg,
            preflight=preflight,
            fix_suggestions=fix_suggestions,
        )
        if repeated_failure_count >= 2 and policy.get("severity") == "simple":
            policy = dict(policy)
            policy.update(
                {
                    "severity": "hard",
                    "should_handoff_to_engineer": True,
                    "max_self_retries": 0,
                    "recommendation": "handoff_to_engineer_with_context",
                    "reason": (
                        f"Repeated identical execution failure detected ({repeated_failure_count} times). "
                        "Escalate to NTL_Engineer instead of continued self-debug."
                    ),
                }
            )
        result = {
            "status": "fail",
            "stdout": logs,
            "error_type": etype,
            "error_message": emsg,
            "traceback": tb,
            "script_name": logical_script_name,
            "script_path": str(script_path),
            "script_location": resolved_location or effective_location,
            "code": script_content,
            "preflight": preflight,
            "fix_suggestions": fix_suggestions,
            "error_handling_policy": policy,
            "path_protocol_mode": path_mode,
            "path_protocol_enforcement": path_protocol_enforcement,
            "repeated_failure_signature_count": repeated_failure_count,
            "execution_skipped": False,
            "artifact_audit": artifact_audit,
            "runtime_path_rewrite": runtime_path_rewrite,
            "stdout_quality_audit": stdout_quality_audit,
        }
        _append_run_history(
            {
                "timestamp": _timestamp(),
                "tool": "execute_geospatial_script_tool",
                "status": "fail",
                "script_name": logical_script_name,
                "script_path": str(script_path),
                "thread_id": current_thread_id.get(),
                "error_type": etype,
            }
        )
        return json.dumps(result, indent=2, ensure_ascii=False)
    finally:
        if token is not None:
            current_thread_id.reset(token)


GeoCode_COT_Validation_tool = StructuredTool.from_function(
    GEE_GeoCode_COT_Validation,
    name="GeoCode_COT_Validation_tool",
    description=(
        "Enhanced Geo-CodeCoT validator for geospatial Python. Performs preflight static checks "
        "(syntax, sandbox-first path protocol, GEE initialization, dataset/band and CRS risk checks), then executes "
        "one minimal code block and returns structured JSON with pass/fail, logs, traceback, and fix suggestions."
    ),
    args_schema=GeoCodeCOTBlockInput,
)


execute_geospatial_script_tool = StructuredTool.from_function(
    execute_geospatial_script,
    name="execute_geospatial_script_tool",
    description=(
        "Execute a previously saved .py geospatial script by filename under thread workspace sandbox. "
        "Returns structured JSON with status, logs, script metadata, and traceback when failed."
    ),
    args_schema=ExecuteScriptInput,
)
