import contextvars
import os
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional

from runtime_governance import thread_workspace_quota_bytes, user_workspace_quota_bytes

# Thread-scoped context used across LangGraph/Deep Agents execution.
current_thread_id = contextvars.ContextVar("thread_id", default="debug")
current_gee_project_id = contextvars.ContextVar("gee_project_id", default="")
current_gee_encrypted_refresh_token = contextvars.ContextVar("gee_encrypted_refresh_token", default="")
current_gee_token_scopes = contextvars.ContextVar("gee_token_scopes", default="")


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
        if not thread_id or not str(thread_id).strip():
            raise ValueError("No valid thread_id available in context or argument.")

        tid = str(thread_id).strip()
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
        if not raw or rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"Unsafe workspace-relative path: {path_value}")
        return rel

    def resolve_deepagents_path(self, deep_path: str, thread_id: Optional[str] = None) -> Path:
        workspace = self.get_workspace(thread_id)
        should_create_parent = True
        if deep_path.startswith("/data/raw/"):
            rel = self._safe_virtual_tail(deep_path, "/data/raw/")
            target = workspace / "inputs" / Path(*rel.parts)
        elif deep_path.startswith("/data/processed/"):
            rel = self._safe_virtual_tail(deep_path, "/data/processed/")
            target = workspace / "outputs" / Path(*rel.parts)
        elif deep_path.startswith("/memories/"):
            rel = self._safe_virtual_tail(deep_path, "/memories/")
            target = workspace / "memory" / Path(*rel.parts)
        elif deep_path.startswith("/shared/"):
            rel = self._safe_virtual_tail(deep_path, "/shared/")
            target = self.shared_dir / Path(*rel.parts)
            should_create_parent = False
        else:
            raise ValueError(f"Unknown Deep Agents virtual path: {deep_path}")
        if should_create_parent:
            target.parent.mkdir(parents=True, exist_ok=True)
        return target.resolve()

    def resolve_workspace_relative_path(
        self,
        path_value: str,
        thread_id: Optional[str] = None,
        *,
        default_root: str = "outputs",
        create_parent: bool = False,
        allow_memory: bool = True,
    ) -> Path:
        if self._is_deepagents_virtual_path(path_value):
            if self._is_shared_virtual_path(path_value) and create_parent:
                raise PermissionError("Shared dataset path is read-only.")
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

        parts = rel.parts
        if parts and parts[0] in roots:
            if parts[0] == "memory" and not allow_memory:
                raise PermissionError("Memory path is not allowed in this context.")
            target = roots[parts[0]].joinpath(*parts[1:])
            allowed_root = roots[parts[0]]
        else:
            target = roots[default_root].joinpath(*parts)
            allowed_root = roots[default_root]

        resolved = target.resolve()
        if resolved != allowed_root and not str(resolved).startswith(str(allowed_root) + os.sep):
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
        print(f"[StorageManager] Resolving '{filename}' for thread_id='{tid}'")

        if self._is_deepagents_virtual_path(filename):
            path_obj = self.resolve_deepagents_path(filename, tid)
            print(f"[StorageManager] Resolved virtual path to: {path_obj}")
            return str(path_obj)

        safe_filename = os.path.basename(filename)
        workspace = self.get_workspace(tid)
        user_input = workspace / "inputs" / safe_filename
        shared_input = self.shared_dir / safe_filename

        if user_input.exists():
            print(f"[StorageManager] Found in user workspace: {user_input}")
            return str(user_input.absolute())
        if shared_input.exists():
            print(f"[StorageManager] Found in shared directory: {shared_input}")
            return str(shared_input.absolute())
        print(f"[StorageManager] Not found. Defaulting to user workspace path: {user_input}")
        return str(user_input.absolute())

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
            path_obj = self.resolve_deepagents_path(filename, tid)
            return str(path_obj)

        workspace = self.get_workspace(tid)
        safe_filename = os.path.basename(filename)
        output_path = workspace / "outputs" / safe_filename
        return str(output_path.absolute())

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
