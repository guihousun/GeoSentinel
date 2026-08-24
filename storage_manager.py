import contextvars
import json
import os
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Dict, Iterator, Optional

from runtime_governance import thread_workspace_quota_bytes, user_workspace_quota_bytes

# Thread-scoped context used across LangGraph/Deep Agents execution.
current_thread_id = contextvars.ContextVar("thread_id", default="debug")
current_gee_project_id = contextvars.ContextVar("gee_project_id", default="")
current_gee_encrypted_refresh_token = contextvars.ContextVar("gee_encrypted_refresh_token", default="")
current_gee_token_scopes = contextvars.ContextVar("gee_token_scopes", default="")


class StorageQuotaExceededError(OSError):
    """Raised when a managed write would exceed the thread workspace quota."""


class StorageManager:
    def __init__(self, base_dir: str = "user_data", shared_dir: str = "base_data"):
        self.base_dir = self._resolve_root_dir(
            configured=base_dir,
            env_key="NTL_USER_DATA_DIR",
            default_name="user_data",
        )
        self.shared_dir = self._resolve_root_dir(
            configured=shared_dir,
            env_key="NTL_SHARED_DATA_DIR",
            default_name="base_data",
        )
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self._workspace_locks: dict[str, threading.RLock] = {}
        self._workspace_locks_guard = threading.Lock()

    @staticmethod
    def _safe_thread_id(thread_id: Any) -> str:
        tid = str(thread_id or "").strip()
        if (
            not tid
            or tid in {".", ".."}
            or "/" in tid
            or "\\" in tid
            or "\x00" in tid
            or len(tid) > 160
        ):
            raise ValueError("Invalid thread_id; expected one workspace identifier without path separators.")
        return tid

    @staticmethod
    def _resolve_root_dir(*, configured: str, env_key: str, default_name: str) -> Path:
        """
        Resolve stable storage roots across environments.

        Priority:
        1) Environment variable override (recommended for deployment).
        2) Explicit non-default constructor argument.
        3) Existing user-home default folder (e.g. C:\\Users\\<user>\\user_data).
        4) Repository/runtime relative default (resolved current path).
        """
        env_value = str(os.getenv(env_key, "") or "").strip()
        if env_value:
            return Path(env_value).resolve()

        if configured != default_name:
            return Path(configured).resolve()

        home_candidate = (Path.home() / default_name).resolve()
        if home_candidate.exists():
            return home_candidate

        return Path(configured).resolve()

    def get_workspace(self, thread_id: Optional[str] = None) -> Path:
        if thread_id is None:
            thread_id = current_thread_id.get()
        tid = self._safe_thread_id(thread_id)
        # Some deployment shells accidentally point NTL_USER_DATA_DIR at a thread workspace
        # (e.g. .../user_data/<tid>) instead of user_data root. In that case, appending tid
        # again causes duplicated paths like .../user_data/<tid>/user_data/<tid>/inputs.
        if self._is_thread_workspace_dir(self.base_dir, tid):
            workspace = self.base_dir
        else:
            workspace = self.base_dir / tid
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / "inputs").mkdir(exist_ok=True)
        (workspace / "outputs").mkdir(exist_ok=True)
        (workspace / "memory").mkdir(exist_ok=True)
        return workspace

    @staticmethod
    def _is_thread_workspace_dir(path_obj: Path, tid: str) -> bool:
        """Best-effort detection for misconfigured base_dir that already equals a thread workspace."""
        try:
            p = path_obj.resolve()
        except Exception:
            p = path_obj
        if p.name != str(tid).strip():
            return False
        return all((p / d).exists() for d in ("inputs", "outputs", "memory"))

    @staticmethod
    def _is_deepagents_virtual_path(path_value: str) -> bool:
        if not isinstance(path_value, str):
            return False
        return path_value.startswith(("/data/raw/", "/data/processed/", "/memories/", "/shared/"))

    @staticmethod
    def _safe_virtual_tail(full_path: str, prefix: str) -> PurePosixPath:
        tail = full_path[len(prefix) :].strip("/")
        rel = PurePosixPath(tail)
        if not tail or rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"Unsafe or empty virtual path tail: {full_path}")
        return rel

    @staticmethod
    def _safe_workspace_relative_path(path_value: str) -> PurePosixPath:
        raw = str(path_value or "").strip().replace("\\", "/")
        rel = PurePosixPath(raw)
        if not raw or rel.is_absolute() or PureWindowsPath(raw).is_absolute() or ".." in rel.parts:
            raise ValueError(f"Unsafe workspace-relative path: {path_value}")
        return rel

    @staticmethod
    def _is_within_root(path_obj: Path, root: Path) -> bool:
        try:
            resolved_root = root.resolve()
            return os.path.commonpath((str(path_obj.resolve()), str(resolved_root))) == str(resolved_root)
        except (OSError, ValueError):
            return False

    def _workspace_lock(self, thread_id: Optional[str] = None) -> threading.RLock:
        tid = self._safe_thread_id(thread_id if thread_id is not None else current_thread_id.get())
        with self._workspace_locks_guard:
            return self._workspace_locks.setdefault(tid, threading.RLock())

    @contextmanager
    def workspace_write_lock(self, thread_id: Optional[str] = None) -> Iterator[None]:
        """Serialize managed writes and script executions within one thread workspace."""
        lock = self._workspace_lock(thread_id)
        with lock:
            yield

    def resolve_deepagents_path(self, deep_path: str, thread_id: Optional[str] = None) -> Path:
        workspace = self.get_workspace(thread_id)
        should_create_parent = True
        if deep_path.startswith("/data/raw/"):
            rel = self._safe_virtual_tail(deep_path, "/data/raw/")
            allowed_root = (workspace / "inputs").resolve()
            target = allowed_root / Path(*rel.parts)
        elif deep_path.startswith("/data/processed/"):
            rel = self._safe_virtual_tail(deep_path, "/data/processed/")
            allowed_root = (workspace / "outputs").resolve()
            target = allowed_root / Path(*rel.parts)
        elif deep_path.startswith("/memories/"):
            rel = self._safe_virtual_tail(deep_path, "/memories/")
            allowed_root = (workspace / "memory").resolve()
            target = allowed_root / Path(*rel.parts)
        elif deep_path.startswith("/shared/"):
            rel = self._safe_virtual_tail(deep_path, "/shared/")
            allowed_root = self.shared_dir.resolve()
            target = allowed_root / Path(*rel.parts)
            should_create_parent = False
        else:
            raise ValueError(f"Unknown Deep Agents virtual path: {deep_path}")
        resolved = target.resolve()
        if not self._is_within_root(resolved, allowed_root):
            raise PermissionError("Resolved virtual path escaped its allowed storage root.")
        if should_create_parent:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved

    def resolve_workspace_relative_path(
        self,
        path_value: str,
        thread_id: Optional[str] = None,
        *,
        default_root: str = "outputs",
        create_parent: bool = False,
        allow_memory: bool = True,
        allowed_roots: Optional[tuple[str, ...]] = None,
    ) -> Path:
        if self._is_deepagents_virtual_path(path_value):
            if self._is_shared_virtual_path(path_value) and create_parent:
                raise PermissionError("Shared dataset path is read-only.")
            virtual_roots = {
                "/data/raw/": "inputs",
                "/data/processed/": "outputs",
                "/memories/": "memory",
                "/shared/": "shared",
            }
            selected_root = next(root for prefix, root in virtual_roots.items() if path_value.startswith(prefix))
            if allowed_roots is not None and selected_root not in allowed_roots:
                raise PermissionError(f"Path root '{selected_root}' is not allowed in this context.")
            return self.resolve_deepagents_path(path_value, thread_id)

        workspace = self.get_workspace(thread_id)
        rel = self._safe_workspace_relative_path(path_value)
        roots = {
            "inputs": (workspace / "inputs").resolve(),
            "outputs": (workspace / "outputs").resolve(),
            "memory": (workspace / "memory").resolve(),
        }
        if default_root not in roots:
            raise ValueError(f"Unsupported default_root: {default_root}")
        permitted_roots = set(allowed_roots or roots)
        if not permitted_roots.issubset(roots):
            raise ValueError(f"Unsupported allowed_roots: {sorted(permitted_roots - set(roots))}")

        parts = rel.parts
        if parts and parts[0] in roots:
            if parts[0] not in permitted_roots:
                raise PermissionError(f"Path root '{parts[0]}' is not allowed in this context.")
            if parts[0] == "memory" and not allow_memory:
                raise PermissionError("Memory path is not allowed in this context.")
            target = roots[parts[0]].joinpath(*parts[1:])
            allowed_root = roots[parts[0]]
        else:
            if default_root not in permitted_roots:
                raise PermissionError(f"Path root '{default_root}' is not allowed in this context.")
            target = roots[default_root].joinpath(*parts)
            allowed_root = roots[default_root]

        resolved = target.resolve()
        if not self._is_within_root(resolved, allowed_root):
            raise PermissionError("Resolved path escaped the allowed workspace root.")
        if create_parent:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved

    @staticmethod
    def _is_shared_virtual_path(path_value: str) -> bool:
        return isinstance(path_value, str) and path_value.startswith("/shared/")

    def resolve_input_path(self, filename: str, thread_id: Optional[str] = None) -> str:
        if thread_id is None:
            thread_id = current_thread_id.get()
        tid = str(thread_id).strip()

        if self._is_deepagents_virtual_path(filename):
            path_obj = self.resolve_deepagents_path(filename, tid)
            return str(path_obj)

        if not str(filename or "").strip():
            return str((self.get_workspace(tid) / "inputs").resolve())
        rel = self._safe_workspace_relative_path(filename)
        if rel.parts and rel.parts[0] == "inputs":
            rel = PurePosixPath(*rel.parts[1:])
        if not rel.parts:
            raise ValueError("Input path must identify a file.")
        user_input = self.resolve_workspace_relative_path(
            str(rel),
            thread_id=tid,
            default_root="inputs",
            allowed_roots=("inputs",),
        )
        shared_input = (self.shared_dir / Path(*rel.parts)).resolve()
        if not self._is_within_root(shared_input, self.shared_dir):
            raise PermissionError("Resolved shared input escaped the shared storage root.")

        if user_input.exists():
            return str(user_input)
        if shared_input.exists():
            return str(shared_input)
        return str(user_input)

    def resolve_output_path(self, filename: str, thread_id: Optional[str] = None) -> str:
        if thread_id is None:
            thread_id = current_thread_id.get()
        tid = str(thread_id).strip()

        if self._is_deepagents_virtual_path(filename):
            if self._is_shared_virtual_path(filename):
                raise PermissionError(
                    "Shared dataset path is read-only. "
                    "Use resolve_input_path('/shared/...') for reading and resolve_output_path(...) for workspace outputs."
                )
            path_obj = self.resolve_workspace_relative_path(
                filename,
                thread_id=tid,
                default_root="outputs",
                create_parent=True,
                allow_memory=False,
                allowed_roots=("outputs",),
            )
            return str(path_obj)

        output_path = self.resolve_workspace_relative_path(
            filename,
            thread_id=tid,
            default_root="outputs",
            create_parent=True,
            allow_memory=False,
            allowed_roots=("outputs",),
        )
        return str(output_path)

    def _quota_delta_for_write(self, path: Path, new_size_bytes: int) -> int:
        existing_size = 0
        try:
            if path.is_file():
                existing_size = int(path.stat().st_size)
        except OSError:
            existing_size = 0
        return max(0, int(new_size_bytes) - existing_size)

    def ensure_thread_quota(
        self,
        thread_id: Optional[str] = None,
        *,
        additional_bytes: int = 0,
    ) -> Dict[str, int | bool]:
        snapshot = self.thread_quota_snapshot(thread_id, additional_bytes=additional_bytes)
        if not snapshot["allowed"]:
            raise StorageQuotaExceededError(
                "Thread workspace quota exceeded: "
                f"projected={snapshot['projected_bytes']} bytes, limit={snapshot['limit_bytes']} bytes."
            )
        return snapshot

    def atomic_write_text(
        self,
        path_value: str,
        content: str,
        thread_id: Optional[str] = None,
        *,
        default_root: str = "outputs",
        allow_memory: bool = True,
        encoding: str = "utf-8",
    ) -> Path:
        tid = str(thread_id if thread_id is not None else current_thread_id.get()).strip()
        payload = str(content).encode(encoding)
        with self.workspace_write_lock(tid):
            target = self.resolve_workspace_relative_path(
                path_value,
                thread_id=tid,
                default_root=default_root,
                create_parent=True,
                allow_memory=allow_memory,
                allowed_roots=(default_root,),
            )
            delta = self._quota_delta_for_write(target, len(payload))
            self.ensure_thread_quota(tid, additional_bytes=delta)
            temp_path: Optional[Path] = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=str(target.parent),
                    prefix=f".{target.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as handle:
                    temp_path = Path(handle.name)
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_path, target)
            finally:
                if temp_path is not None and temp_path.exists():
                    temp_path.unlink(missing_ok=True)
            return target

    def atomic_write_json(
        self,
        path_value: str,
        payload: Any,
        thread_id: Optional[str] = None,
        *,
        default_root: str = "outputs",
        allow_memory: bool = True,
    ) -> Path:
        return self.atomic_write_text(
            path_value,
            json.dumps(payload, ensure_ascii=False, indent=2),
            thread_id=thread_id,
            default_root=default_root,
            allow_memory=allow_memory,
        )

    def append_jsonl(
        self,
        path_value: str,
        event: Dict[str, Any],
        thread_id: Optional[str] = None,
        *,
        default_root: str = "outputs",
        allow_memory: bool = True,
    ) -> Path:
        tid = str(thread_id if thread_id is not None else current_thread_id.get()).strip()
        line = (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
        with self.workspace_write_lock(tid):
            target = self.resolve_workspace_relative_path(
                path_value,
                thread_id=tid,
                default_root=default_root,
                create_parent=True,
                allow_memory=allow_memory,
                allowed_roots=(default_root,),
            )
            self.ensure_thread_quota(tid, additional_bytes=len(line))
            with target.open("ab") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            return target

    def list_workspace(self, thread_id: Optional[str] = None) -> Dict[str, list[str]]:
        workspace = self.get_workspace(thread_id)
        return {
            "inputs": [p.name for p in (workspace / "inputs").glob("*") if p.is_file()],
            "outputs": [p.name for p in (workspace / "outputs").glob("*") if p.is_file()],
            "memory": [p.name for p in (workspace / "memory").glob("*") if p.is_file()],
        }

    @staticmethod
    def _tree_size_bytes(root: Path) -> int:
        total = 0
        if not root.exists():
            return 0
        for path in root.rglob("*"):
            try:
                if path.is_file():
                    total += int(path.stat().st_size)
            except Exception:
                continue
        return total

    def workspace_usage_bytes(self, thread_id: Optional[str] = None) -> int:
        workspace = self.get_workspace(thread_id)
        return self._tree_size_bytes(workspace)

    def aggregate_thread_usage_bytes(self, thread_ids: list[str]) -> int:
        total = 0
        seen: set[str] = set()
        for raw_thread_id in thread_ids:
            tid = str(raw_thread_id or "").strip()
            if not tid or tid in seen:
                continue
            seen.add(tid)
            total += self.workspace_usage_bytes(tid)
        return total

    def thread_quota_snapshot(self, thread_id: Optional[str] = None, *, additional_bytes: int = 0) -> Dict[str, int | bool]:
        usage = self.workspace_usage_bytes(thread_id)
        limit = max(0, int(thread_workspace_quota_bytes() or 0))
        projected = usage + max(0, int(additional_bytes or 0))
        return {
            "usage_bytes": usage,
            "limit_bytes": limit,
            "projected_bytes": projected,
            "allowed": (limit <= 0) or (projected <= limit),
        }

    def user_quota_snapshot(self, thread_ids: list[str], *, additional_bytes: int = 0) -> Dict[str, int | bool]:
        usage = self.aggregate_thread_usage_bytes(thread_ids)
        limit = max(0, int(user_workspace_quota_bytes() or 0))
        projected = usage + max(0, int(additional_bytes or 0))
        return {
            "usage_bytes": usage,
            "limit_bytes": limit,
            "projected_bytes": projected,
            "allowed": (limit <= 0) or (projected <= limit),
        }

    @staticmethod
    def get_thread_id_from_config(config: Dict[str, Any]) -> str:
        return config.get("configurable", {}).get("thread_id", "")


storage_manager = StorageManager()
