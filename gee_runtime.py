from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from dotenv import dotenv_values


GEE_PROJECT_ENV = "GEE_DEFAULT_PROJECT_ID"
GEE_BOUNDARY_ASSET_PROJECT_ENV = "GEE_BOUNDARY_ASSET_PROJECT_ID"
REPO_DOTENV_PATH = Path(__file__).resolve().parent / ".env"

# The Earth Engine Python client keeps process-global state.  This lock prevents
# concurrent Initialize calls from interleaving, but it does *not* provide
# credential isolation between users.  The current runtime binds only a project
# identifier and deliberately does not load per-user OAuth refresh tokens.
_EE_INITIALIZE_LOCK = threading.RLock()

GEE_RUNTIME_DESCRIPTOR = {
    "client_scope": "process_global",
    "initialize_lock": "process_rlock",
    "project_binding": "single_deployment_project",
    "credential_source": "deployment_service_account_or_ambient_credentials",
    "credential_isolation": "not_guaranteed",
}


class GEERuntimeError(RuntimeError):
    """Base class for stable Earth Engine runtime failures."""


class GEEProjectConfigurationError(GEERuntimeError):
    """Raised when no explicit Earth Engine project can be resolved."""


class GEEInitializationError(GEERuntimeError):
    """Earth Engine initialization failed after project resolution."""

    def __init__(self, *, project_id: str, category: str, cause: BaseException):
        self.project_id = str(project_id)
        self.category = str(category)
        self.cause_type = type(cause).__name__
        super().__init__(
            "Earth Engine initialization failed "
            f"(category={self.category}, cause={self.cause_type})."
        )


def _clean_project_id(value: Any) -> str:
    return str(value or "").strip()


def _context_project_id() -> str:
    # Lazy import avoids a module cycle: storage_manager imports runtime_governance,
    # while runtime_governance delegates project lookup to this module.
    try:
        from storage_manager import current_gee_project_id

        return _clean_project_id(current_gee_project_id.get())
    except (ImportError, AttributeError, LookupError):
        return ""


def _dotenv_value(name: str, path: Path | None = None) -> str:
    dotenv_path = Path(path or REPO_DOTENV_PATH)
    if not dotenv_path.is_file():
        return ""
    try:
        values = dotenv_values(dotenv_path)
    except (OSError, UnicodeError, ValueError):
        return ""
    return _clean_project_id(values.get(name))


def _dotenv_project_id(path: Path | None = None) -> str:
    return _dotenv_value(GEE_PROJECT_ENV, path)


def resolve_gee_project_id(explicit_project_id: str | None = None) -> str:
    """Resolve one GEE project using the canonical, fail-closed precedence.

    Precedence: explicit argument, bound runtime context, process environment,
    then the repository ``.env`` file.  There is intentionally no hard-coded
    real project and no project-less fallback.
    """

    candidates = (
        _clean_project_id(explicit_project_id),
        _context_project_id(),
        _clean_project_id(os.getenv(GEE_PROJECT_ENV)),
        _dotenv_project_id(),
    )
    for project_id in candidates:
        if project_id:
            return project_id
    raise GEEProjectConfigurationError(
        "Earth Engine project is not configured. Set GEE_DEFAULT_PROJECT_ID, "
        "configure it in the repository .env, or pass an explicit project_id."
    )


def resolve_gee_boundary_asset_project_id(explicit_project_id: str | None = None) -> str:
    """Resolve the project that owns NTL-GPT's private boundary assets.

    The quota/runtime project and an Earth Engine asset owner are distinct
    identities.  Deployments that keep ``province/city/county`` in a different
    project must set ``GEE_BOUNDARY_ASSET_PROJECT_ID`` explicitly.  For
    backward compatibility, an unset value falls back to the resolved runtime
    project, but never to a hard-coded real project.
    """

    explicit = _clean_project_id(explicit_project_id)
    if explicit:
        return explicit
    configured = _clean_project_id(os.getenv(GEE_BOUNDARY_ASSET_PROJECT_ENV))
    if not configured:
        configured = _clean_project_id(_dotenv_value(GEE_BOUNDARY_ASSET_PROJECT_ENV))
    return configured or resolve_gee_project_id()


def _classify_initialization_error(exc: BaseException) -> str:
    detail = f"{type(exc).__name__}: {exc}".lower()
    if any(marker in detail for marker in ("modulenotfounderror", "no module named 'ee'", 'no module named "ee"')):
        return "dependency"
    if any(marker in detail for marker in ("ssl", "tls", "certificate", "unexpected_eof")):
        return "transport_tls"
    if any(marker in detail for marker in ("timeout", "timed out", "connection refused", "connection reset")):
        return "transport_connectivity"
    if any(marker in detail for marker in ("quota", "resource_exhausted", "429")):
        return "quota"
    if any(marker in detail for marker in ("permission", "forbidden", "403", "not enabled")):
        return "project_access"
    if any(
        marker in detail
        for marker in (
            "credential",
            "unauthorized",
            "invalid_grant",
            "401",
            "token",
            "please authorize access",
            "authenticate",
            "persistent_credentials",
        )
    ):
        return "credentials"
    return "unknown"


def initialize_ee(
    explicit_project_id: str | None = None,
    credentials: Any = None,
    ee_module: Any = None,
) -> str:
    """Initialize Earth Engine once under a process lock and return project id.

    ``ee.Authenticate`` is never invoked here.  Authentication is an operator
    setup concern; runtime requests fail with a classified error instead of
    starting an interactive flow or retrying without a project.
    """

    project_id = resolve_gee_project_id(explicit_project_id)
    if ee_module is None:
        try:
            import ee as ee_module  # type: ignore[no-redef]
        except Exception as exc:  # noqa: BLE001 - normalize optional dependency failure
            raise GEEInitializationError(
                project_id=project_id,
                category=_classify_initialization_error(exc),
                cause=exc,
            ) from exc

    resolved_credentials = credentials
    if resolved_credentials is None:
        service_account = _clean_project_id(os.getenv("EE_SERVICE_ACCOUNT"))
        private_key_json = str(os.getenv("EE_PRIVATE_KEY_JSON") or "").strip()
        if bool(service_account) != bool(private_key_json):
            raise GEERuntimeError(
                "Earth Engine service-account configuration is incomplete. "
                "Set both EE_SERVICE_ACCOUNT and EE_PRIVATE_KEY_JSON, or neither."
            )
        if service_account:
            try:
                resolved_credentials = ee_module.ServiceAccountCredentials(
                    service_account,
                    key_data=private_key_json,
                )
            except Exception as exc:  # noqa: BLE001
                raise GEEInitializationError(
                    project_id=project_id,
                    category="credentials",
                    cause=exc,
                ) from exc

    kwargs: dict[str, Any] = {"project": project_id}
    if resolved_credentials is not None:
        kwargs["credentials"] = resolved_credentials

    try:
        with _EE_INITIALIZE_LOCK:
            ee_module.Initialize(**kwargs)
    except Exception as exc:  # noqa: BLE001 - normalize third-party failures
        raise GEEInitializationError(
            project_id=project_id,
            category=_classify_initialization_error(exc),
            cause=exc,
        ) from exc
    return project_id


@contextmanager
def bind_gee_runtime(project_id: str, profile_source: str) -> Iterator[str]:
    """Bind project/source ContextVars for one agent run and reset on exit.

    A local-only run is allowed to start without GEE configuration.  In that
    case the empty binding is preserved and any GEE tool later invoked fails
    closed in :func:`resolve_gee_project_id`.
    """

    candidate = _clean_project_id(project_id)
    if candidate:
        resolved_project_id = resolve_gee_project_id(candidate)
    else:
        try:
            resolved_project_id = resolve_gee_project_id()
        except GEEProjectConfigurationError:
            resolved_project_id = ""
    resolved_source = str(profile_source or "deployment_default").strip() or "deployment_default"

    from storage_manager import current_gee_profile_source, current_gee_project_id

    project_token = current_gee_project_id.set(resolved_project_id)
    source_token = current_gee_profile_source.set(resolved_source)
    try:
        yield resolved_project_id
    finally:
        current_gee_profile_source.reset(source_token)
        current_gee_project_id.reset(project_token)

