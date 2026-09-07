"""System-owned identity binding for workspace-local contract artifacts.

Models declare only a local path and semantic metadata.  The benchmark runner
registers immutable staged-input identities for one exact thread/run/task
scope, while save tools measure outputs inside that same workspace.  This
keeps checksums and byte counts out of model control without weakening the
post-run integrity gate.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import stat
from threading import RLock
from typing import Any, Iterator, Mapping


_DIGEST_PATTERN_LENGTH = 64
_IDENTITY_FIELDS = frozenset({"sha256", "bytes", "sha256_status", "bytes_status"})
_ARTIFACT_COLLECTION_FIELDS = frozenset(
    {
        "analysis_ready_artifacts",
        "artifacts",
        "representative_artifacts",
    }
)
_SCOPES_LOCK = RLock()


class ArtifactIdentityError(ValueError):
    """Raised when a local artifact cannot be bound to trusted system identity."""


@dataclass(frozen=True)
class ArtifactIdentity:
    relative_path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class ArtifactScope:
    thread_id: str
    run_id: str
    task_id: str
    workspace: Path
    staged_inputs: Mapping[str, ArtifactIdentity]


_SCOPES: dict[tuple[str, str, str], ArtifactScope] = {}


def _scope_component(value: Any, *, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ArtifactIdentityError(f"{name} is required for artifact identity binding")
    return normalized


def _scope_key(*, thread_id: str, run_id: str, task_id: str) -> tuple[str, str, str]:
    return (
        _scope_component(thread_id, name="thread_id"),
        _scope_component(run_id, name="run_id"),
        _scope_component(task_id, name="task_id"),
    )


def _is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((str(path.resolve()), str(root.resolve()))) == str(root.resolve())
    except (OSError, ValueError):
        return False


def _path_is_linklike(path: Path) -> bool:
    try:
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            return True
    except OSError:
        return True
    try:
        attributes = int(getattr(os.lstat(path), "st_file_attributes", 0))
    except (FileNotFoundError, OSError):
        return False
    reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse)


def _validated_workspace(workspace: str | Path) -> Path:
    root = Path(workspace).resolve(strict=True)
    if not root.is_dir() or _path_is_linklike(root):
        raise ArtifactIdentityError("artifact workspace must be a real directory")
    for name in ("inputs", "outputs"):
        child = root / name
        if not child.is_dir() or _path_is_linklike(child):
            raise ArtifactIdentityError(f"artifact workspace {name}/ root is unsafe or missing")
        if not _is_within(child.resolve(strict=True), root):
            raise ArtifactIdentityError(f"artifact workspace {name}/ root escaped its workspace")
    return root


def _normalize_local_path(path_value: Any, *, force_output_relative: bool = False) -> tuple[str, str] | None:
    if not isinstance(path_value, str):
        if force_output_relative:
            raise ArtifactIdentityError("local artifact path must be text")
        return None
    raw = path_value.strip().replace("\\", "/")
    if not raw or "\x00" in raw:
        if force_output_relative:
            raise ArtifactIdentityError("local artifact path is empty or unsafe")
        return None
    if raw.startswith("package/"):
        return None
    if raw.startswith("/data/raw/"):
        root = "inputs"
        tail = raw[len("/data/raw/") :]
    elif raw.startswith("/inputs/"):
        root = "inputs"
        tail = raw[len("/inputs/") :]
    elif raw.startswith("inputs/"):
        root = "inputs"
        tail = raw[len("inputs/") :]
    elif raw.startswith("/data/processed/"):
        root = "outputs"
        tail = raw[len("/data/processed/") :]
    elif raw.startswith("/outputs/"):
        root = "outputs"
        tail = raw[len("/outputs/") :]
    elif raw.startswith("outputs/"):
        root = "outputs"
        tail = raw[len("outputs/") :]
    elif force_output_relative:
        root = "outputs"
        tail = raw
    else:
        return None

    windows_path = PureWindowsPath(raw)
    relative = PurePosixPath(tail)
    if (
        windows_path.is_absolute()
        or windows_path.drive
        or not tail
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ArtifactIdentityError("local artifact path is absolute or contains traversal")
    return root, f"{root}/{relative.as_posix()}"


def _valid_digest(value: Any) -> bool:
    text = str(value or "")
    return len(text) == _DIGEST_PATTERN_LENGTH and all(char in "0123456789abcdef" for char in text)


def _identity_from_record(value: Mapping[str, Any]) -> ArtifactIdentity:
    normalized = _normalize_local_path(value.get("relative_path"))
    if normalized is None or normalized[0] != "inputs":
        raise ArtifactIdentityError("staged input registry paths must remain under inputs/")
    relative_path = normalized[1]
    digest = str(value.get("sha256") or "").strip().casefold()
    byte_count = value.get("bytes")
    if not _valid_digest(digest):
        raise ArtifactIdentityError("staged input registry contains an invalid SHA-256")
    if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
        raise ArtifactIdentityError("staged input registry contains an invalid byte count")
    return ArtifactIdentity(relative_path=relative_path, sha256=digest, bytes=byte_count)


@contextmanager
def bind_artifact_scope(
    *,
    thread_id: str,
    run_id: str,
    task_id: str,
    workspace: str | Path,
    staged_inputs: list[Mapping[str, Any]],
) -> Iterator[ArtifactScope]:
    """Bind runner-captured input identities to one exact runtime scope."""

    key = _scope_key(thread_id=thread_id, run_id=run_id, task_id=task_id)
    workspace_root = _validated_workspace(workspace)
    registered: dict[str, ArtifactIdentity] = {}
    for raw in staged_inputs:
        identity = _identity_from_record(raw)
        registry_key = identity.relative_path.casefold()
        if registry_key in registered:
            raise ArtifactIdentityError("duplicate staged input path in artifact registry")
        registered[registry_key] = identity
    scope = ArtifactScope(
        thread_id=key[0],
        run_id=key[1],
        task_id=key[2],
        workspace=workspace_root,
        staged_inputs=registered,
    )
    with _SCOPES_LOCK:
        if key in _SCOPES:
            raise ArtifactIdentityError("artifact identity scope is already bound")
        _SCOPES[key] = scope
    try:
        yield scope
    finally:
        with _SCOPES_LOCK:
            if _SCOPES.get(key) is scope:
                _SCOPES.pop(key, None)


def current_artifact_scope(*, thread_id: str, run_id: str, task_id: str) -> ArtifactScope | None:
    key = _scope_key(thread_id=thread_id, run_id=run_id, task_id=task_id)
    with _SCOPES_LOCK:
        return _SCOPES.get(key)


def _safe_file(workspace: Path, relative_path: str) -> Path:
    normalized = _normalize_local_path(relative_path)
    if normalized is None:
        raise ArtifactIdentityError("unsupported local artifact path")
    root_name, canonical = normalized
    root = workspace / root_name
    if not root.is_dir() or _path_is_linklike(root):
        raise ArtifactIdentityError("local artifact root is unsafe or missing")
    tail = PurePosixPath(canonical[len(root_name) + 1 :])
    cursor = root
    for part in tail.parts:
        cursor = cursor / part
        if _path_is_linklike(cursor):
            raise ArtifactIdentityError("local artifact path traverses a link or junction")
    try:
        resolved = cursor.resolve(strict=True)
    except OSError as exc:
        raise ArtifactIdentityError("local artifact path does not exist") from exc
    if not _is_within(resolved, root) or not resolved.is_file():
        raise ArtifactIdentityError("local artifact escaped its declared workspace root")
    if resolved.stat().st_nlink > 1:
        raise ArtifactIdentityError("local artifacts must not be hard links")
    return resolved


def _measure_file(path: Path) -> tuple[str, int]:
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise ArtifactIdentityError("local artifact changed while its identity was measured")
    return digest.hexdigest(), int(after.st_size)


def _system_identity(
    relative_path: str,
    *,
    scope: ArtifactScope | None,
    fallback_workspace: str | Path | None,
) -> ArtifactIdentity:
    normalized = _normalize_local_path(relative_path)
    if normalized is None:
        raise ArtifactIdentityError("unsupported local artifact path")
    root_name, canonical = normalized
    if scope is None:
        if root_name == "inputs":
            raise ArtifactIdentityError("input artifact is not registered for the current runtime scope")
        if fallback_workspace is None:
            raise ArtifactIdentityError("artifact workspace is not bound to the current runtime scope")
        workspace = _validated_workspace(fallback_workspace)
    else:
        workspace = scope.workspace

    if root_name == "inputs":
        assert scope is not None
        identity = scope.staged_inputs.get(canonical.casefold())
        if identity is None:
            raise ArtifactIdentityError("input artifact is unknown in the current runtime scope")
        path = _safe_file(workspace, identity.relative_path)
        actual_sha256, actual_bytes = _measure_file(path)
        if (actual_sha256, actual_bytes) != (identity.sha256, identity.bytes):
            raise ArtifactIdentityError("registered input artifact identity has drifted")
        return identity

    path = _safe_file(workspace, canonical)
    digest, byte_count = _measure_file(path)
    return ArtifactIdentity(relative_path=canonical, sha256=digest, bytes=byte_count)


def _is_package_reference(
    value: Mapping[str, Any],
    path_value: Any,
    *,
    allow_expanded: bool = False,
) -> bool:
    if isinstance(path_value, str) and path_value.strip().startswith("package/"):
        return True
    # Only trusted Python ContractEnvelope inputs may already contain an
    # expanded PackageRef.  A model-authored free-form dictionary must not be
    # able to add package-shaped keys around an inputs/outputs path to bypass
    # system checksum binding.
    return allow_expanded and {
        "artifact_id",
        "artifact_type",
        "path",
        "sha256",
    }.issubset(value)


def hydrate_local_artifact_identities(
    value: Any,
    *,
    thread_id: str,
    run_id: str,
    task_id: str,
    fallback_workspace: str | Path | None = None,
    reject_supplied_identity: bool = True,
) -> Any:
    """Copy a contract draft and inject identity for every local artifact ref."""

    scope = current_artifact_scope(thread_id=thread_id, run_id=run_id, task_id=task_id)

    def walk(item: Any, *, force_artifact: bool = False) -> Any:
        if isinstance(item, Mapping):
            path_value = item.get("path")
            normalized = _normalize_local_path(path_value, force_output_relative=force_artifact)
            if normalized is not None and not _is_package_reference(
                item,
                path_value,
                allow_expanded=not reject_supplied_identity,
            ):
                supplied = _IDENTITY_FIELDS.intersection(item)
                if reject_supplied_identity and supplied:
                    raise ArtifactIdentityError(
                        "local artifact sha256/bytes are system-owned and must be omitted"
                    )
                hydrated = {
                    str(key): walk(nested)
                    for key, nested in item.items()
                    if key not in _IDENTITY_FIELDS
                }
                identity = _system_identity(
                    normalized[1],
                    scope=scope,
                    fallback_workspace=fallback_workspace,
                )
                hydrated["path"] = identity.relative_path
                hydrated["sha256"] = identity.sha256
                hydrated["bytes"] = identity.bytes
                return hydrated

            copied: dict[str, Any] = {}
            for key, nested in item.items():
                child_force = str(key) in _ARTIFACT_COLLECTION_FIELDS
                if child_force and isinstance(nested, list):
                    copied[str(key)] = [walk(child, force_artifact=True) for child in nested]
                else:
                    copied[str(key)] = walk(nested)
            return copied
        if isinstance(item, list):
            return [walk(nested, force_artifact=force_artifact) for nested in item]
        return item

    return walk(value)


def strip_model_facing_local_artifact_identity(value: Any) -> Any:
    """Remove system checksum/size fields from model-facing local references."""

    def walk(item: Any, *, force_artifact: bool = False) -> Any:
        if isinstance(item, Mapping):
            path_value = item.get("path")
            normalized = _normalize_local_path(path_value, force_output_relative=force_artifact)
            if normalized is not None and not _is_package_reference(item, path_value):
                return {
                    str(key): walk(nested)
                    for key, nested in item.items()
                    if key not in _IDENTITY_FIELDS
                }
            copied: dict[str, Any] = {}
            for key, nested in item.items():
                child_force = str(key) in _ARTIFACT_COLLECTION_FIELDS
                if child_force and isinstance(nested, list):
                    copied[str(key)] = [walk(child, force_artifact=True) for child in nested]
                else:
                    copied[str(key)] = walk(nested)
            return copied
        if isinstance(item, list):
            return [walk(nested, force_artifact=force_artifact) for nested in item]
        return item

    return walk(value)


__all__ = [
    "ArtifactIdentityError",
    "bind_artifact_scope",
    "current_artifact_scope",
    "hydrate_local_artifact_identities",
    "strip_model_facing_local_artifact_identity",
]

