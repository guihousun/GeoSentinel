from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import bcrypt

from runtime_governance import (
    ASSISTANT_ID,
    default_gee_project_id,
    history_db_url,
    langgraph_postgres_url,
    sanitize_namespace_part,
)
from storage_manager import storage_manager

BASE_DIR = storage_manager.base_dir
USERS_DIR = BASE_DIR / "_users"
RESERVED_USER_IDS = {"guest", "debug", "default", "anonymous"}
_DB_READY: set[str] = set()
USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,39}$")
MIN_PASSWORD_LENGTH = 8
DEFAULT_THREAD_LIST_LIMIT = 8
GEE_PIPELINE_MODES = {"default", "user"}
DEFAULT_DB_CONNECT_TIMEOUT_S = 5
USER_ROLE_USER = "user"
USER_ROLE_ADMIN = "admin"


def _now_ts() -> int:
    return int(time.time())


def _admin_username_keys_from_env() -> set[str]:
    raw = str(os.getenv("NTL_ADMIN_USERNAMES", "") or "")
    return {normalize_user_id(item) for item in raw.split(",") if normalize_user_id(item)}


def _initial_role_for_username(username_key: str) -> str:
    return USER_ROLE_ADMIN if str(username_key or "").strip() in _admin_username_keys_from_env() else USER_ROLE_USER


def _safe_read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _safe_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_user_id(user_name: str) -> str:
    raw = (user_name or "").strip().lower()
    if not raw:
        return "guest"
    value = re.sub(r"\s+", "_", raw)
    value = re.sub(r"[^a-z0-9_\-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_-")
    return value[:40] or "guest"


def is_reserved_user_id(user_id: str) -> bool:
    return normalize_user_id(user_id) in RESERVED_USER_IDS


def is_reserved_user_name(user_name: str) -> bool:
    return is_reserved_user_id(user_name)


def generate_anonymous_user_id() -> str:
    return f"anon-{uuid.uuid4().hex[:8]}"


def generate_thread_id(user_id: str) -> str:
    uid = normalize_user_id(user_id)[:8]
    return f"{uid}-{uuid.uuid4().hex[:6]}"


def _user_dir(user_id: str) -> Path:
    return USERS_DIR / normalize_user_id(user_id)


def _threads_index_path(user_id: str) -> Path:
    return _user_dir(user_id) / "threads_index.json"


def _history_dir(thread_id: str) -> Path:
    path = BASE_DIR / str(thread_id) / "history"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _chat_jsonl_path(thread_id: str) -> Path:
    return _history_dir(thread_id) / "chat_history.jsonl"


def _turn_summary_jsonl_path(thread_id: str) -> Path:
    return _history_dir(thread_id) / "turn_summary.jsonl"


def _context_index_path(thread_id: str) -> Path:
    return _history_dir(thread_id) / "context_index.json"


def _db_url() -> str:
    return str(history_db_url() or "").strip()


def _db_enabled() -> bool:
    return bool(_db_url())


def _db_kind() -> str:
    url = _db_url()
    if url.startswith("sqlite:///"):
        return "sqlite"
    if url.startswith("postgresql://") or url.startswith("postgres://"):
        return "postgres"
    return ""


def _db_connect_timeout_s() -> int:
    raw = str(os.getenv("NTL_HISTORY_DB_CONNECT_TIMEOUT_S", "") or "").strip()
    if not raw:
        raw = str(os.getenv("NTL_LANGGRAPH_POSTGRES_CONNECT_TIMEOUT_S", "") or "").strip()
    try:
        return max(1, int(raw or DEFAULT_DB_CONNECT_TIMEOUT_S))
    except ValueError:
        return DEFAULT_DB_CONNECT_TIMEOUT_S


def _sqlite_path() -> Path:
    raw = _db_url()[len("sqlite:///") :]
    return Path(raw)


def _context_row_key(row: Dict[str, Any]) -> str:
    return f"{row.get('source_file','')}|{row.get('signature','')}|{row.get('chunk_idx','')}"


def _thread_title_from_question(question: str) -> str:
    text = " ".join(str(question or "").strip().split())
    if len(text) <= 80:
        return text
    return text[:77].rstrip() + "..."


@contextmanager
def _db_cursor() -> Iterator[tuple[Any, Any]]:
    kind = _db_kind()
    if kind == "sqlite":
        db_path = _sqlite_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            yield conn, cur
            conn.commit()
        finally:
            conn.close()
        return
    if kind == "postgres":
        import psycopg

        try:
            conn = psycopg.connect(
                _db_url(),
                autocommit=True,
                connect_timeout=_db_connect_timeout_s(),
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "History database is unavailable. Start the PostgreSQL Docker container "
                "`ntl-langgraph-postgres` and retry."
            ) from exc
        try:
            cur = conn.cursor()
            yield conn, cur
        finally:
            conn.close()
        return
    raise RuntimeError("History DB is not configured.")


def _db_execute(sql_sqlite: str, sql_postgres: str | None = None, params: tuple[Any, ...] = ()) -> None:
    sql = sql_sqlite if _db_kind() == "sqlite" else (sql_postgres or sql_sqlite)
    with _db_cursor() as (_conn, cur):
        cur.execute(sql, params)


def _db_fetchone(sql_sqlite: str, sql_postgres: str | None = None, params: tuple[Any, ...] = ()) -> Any:
    sql = sql_sqlite if _db_kind() == "sqlite" else (sql_postgres or sql_sqlite)
    with _db_cursor() as (_conn, cur):
        cur.execute(sql, params)
        return cur.fetchone()


def _db_fetchall(sql_sqlite: str, sql_postgres: str | None = None, params: tuple[Any, ...] = ()) -> list[Any]:
    sql = sql_sqlite if _db_kind() == "sqlite" else (sql_postgres or sql_sqlite)
    with _db_cursor() as (_conn, cur):
        cur.execute(sql, params)
        return cur.fetchall()


def _db_setup() -> None:
    if not _db_enabled():
        return
    url = _db_url()
    if url in _DB_READY:
        return

    if _db_kind() == "sqlite":
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                username_key TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                legacy_migrated_from TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_login_at INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                role TEXT NOT NULL DEFAULT 'user',
                disabled_at INTEGER NOT NULL DEFAULT 0,
                disabled_reason TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_key ON users(username_key)",
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_gee_profiles (
                user_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'default',
                gee_project_id TEXT NOT NULL DEFAULT '',
                google_email TEXT NOT NULL DEFAULT '',
                encrypted_refresh_token TEXT NOT NULL DEFAULT '',
                token_scopes TEXT NOT NULL DEFAULT '',
                token_updated_at INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'unvalidated',
                last_error TEXT NOT NULL DEFAULT '',
                validated_at INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_threads (
                thread_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                workspace TEXT NOT NULL,
                thread_title TEXT DEFAULT '',
                thread_title_manual INTEGER NOT NULL DEFAULT 0,
                last_question TEXT DEFAULT '',
                last_answer_excerpt TEXT DEFAULT ''
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_user_threads_user_id_updated ON user_threads(user_id, updated_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS chat_records (
                row_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                role TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_chat_records_thread_ts ON chat_records(thread_id, ts ASC)",
            """
            CREATE TABLE IF NOT EXISTS turn_summaries (
                row_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_turn_summaries_thread_ts ON turn_summaries(thread_id, ts ASC)",
            """
            CREATE TABLE IF NOT EXISTS injected_context (
                thread_id TEXT NOT NULL,
                row_key TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (thread_id, row_key)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_injected_context_thread_updated ON injected_context(thread_id, updated_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS admin_audit_logs (
                row_id TEXT PRIMARY KEY,
                ts INTEGER NOT NULL,
                admin_user_id TEXT NOT NULL,
                target_user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_admin_audit_logs_ts ON admin_audit_logs(ts DESC)",
        ]
    else:
        stmts = [
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                username_key TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                legacy_migrated_from TEXT NOT NULL DEFAULT '',
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                last_login_at BIGINT NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                role TEXT NOT NULL DEFAULT 'user',
                disabled_at BIGINT NOT NULL DEFAULT 0,
                disabled_reason TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_key ON users(username_key)",
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                user_name TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_gee_profiles (
                user_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'default',
                gee_project_id TEXT NOT NULL DEFAULT '',
                google_email TEXT NOT NULL DEFAULT '',
                encrypted_refresh_token TEXT NOT NULL DEFAULT '',
                token_scopes TEXT NOT NULL DEFAULT '',
                token_updated_at BIGINT NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'unvalidated',
                last_error TEXT NOT NULL DEFAULT '',
                validated_at BIGINT NOT NULL DEFAULT 0,
                updated_at BIGINT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_threads (
                thread_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                workspace TEXT NOT NULL,
                thread_title TEXT DEFAULT '',
                thread_title_manual BOOLEAN NOT NULL DEFAULT FALSE,
                last_question TEXT DEFAULT '',
                last_answer_excerpt TEXT DEFAULT ''
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_user_threads_user_id_updated ON user_threads(user_id, updated_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS chat_records (
                row_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                ts BIGINT NOT NULL,
                role TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_chat_records_thread_ts ON chat_records(thread_id, ts ASC)",
            """
            CREATE TABLE IF NOT EXISTS turn_summaries (
                row_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                ts BIGINT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_turn_summaries_thread_ts ON turn_summaries(thread_id, ts ASC)",
            """
            CREATE TABLE IF NOT EXISTS injected_context (
                thread_id TEXT NOT NULL,
                row_key TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (thread_id, row_key)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_injected_context_thread_updated ON injected_context(thread_id, updated_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS admin_audit_logs (
                row_id TEXT PRIMARY KEY,
                ts BIGINT NOT NULL,
                admin_user_id TEXT NOT NULL,
                target_user_id TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_admin_audit_logs_ts ON admin_audit_logs(ts DESC)",
        ]

    for stmt in stmts:
        _db_execute(stmt)
    _db_migrate_user_admin_columns_if_needed()
    _db_migrate_thread_columns_if_needed()
    _db_migrate_gee_profile_columns_if_needed()
    _db_sync_admin_roles_from_env()
    _DB_READY.add(url)


def _db_migrate_user_admin_columns_if_needed() -> None:
    if _db_kind() == "sqlite":
        alter_statements = [
            "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
            "ALTER TABLE users ADD COLUMN disabled_at INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN disabled_reason TEXT NOT NULL DEFAULT ''",
        ]
    elif _db_kind() == "postgres":
        alter_statements = [
            "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
            "ALTER TABLE users ADD COLUMN disabled_at BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN disabled_reason TEXT NOT NULL DEFAULT ''",
        ]
    else:
        alter_statements = []
    for stmt in alter_statements:
        try:
            _db_execute(stmt)
        except Exception:
            continue


def _db_sync_admin_roles_from_env() -> None:
    admin_keys = sorted(_admin_username_keys_from_env())
    if not admin_keys:
        return
    ts = _now_ts()
    for username_key in admin_keys:
        _db_execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE username_key = ?",
            "UPDATE users SET role = %s, updated_at = %s WHERE username_key = %s",
            (USER_ROLE_ADMIN, ts, username_key),
        )


def _db_migrate_thread_columns_if_needed() -> None:
    alter_statements = []
    if _db_kind() == "sqlite":
        alter_statements = [
            "ALTER TABLE user_threads ADD COLUMN thread_title TEXT DEFAULT ''",
            "ALTER TABLE user_threads ADD COLUMN thread_title_manual INTEGER NOT NULL DEFAULT 0",
        ]
    elif _db_kind() == "postgres":
        alter_statements = [
            "ALTER TABLE user_threads ADD COLUMN thread_title TEXT DEFAULT ''",
            "ALTER TABLE user_threads ADD COLUMN thread_title_manual BOOLEAN NOT NULL DEFAULT FALSE",
        ]
    for stmt in alter_statements:
        try:
            _db_execute(stmt)
        except Exception:
            continue


def _db_migrate_gee_profile_columns_if_needed() -> None:
    if _db_kind() == "sqlite":
        alter_statements = [
            "ALTER TABLE user_gee_profiles ADD COLUMN google_email TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN encrypted_refresh_token TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN token_scopes TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN token_updated_at INTEGER NOT NULL DEFAULT 0",
        ]
    elif _db_kind() == "postgres":
        alter_statements = [
            "ALTER TABLE user_gee_profiles ADD COLUMN google_email TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN encrypted_refresh_token TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN token_scopes TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE user_gee_profiles ADD COLUMN token_updated_at BIGINT NOT NULL DEFAULT 0",
        ]
    else:
        alter_statements = []
    for stmt in alter_statements:
        try:
            _db_execute(stmt)
        except Exception:
            continue


def _db_auth_required() -> None:
    if not _db_enabled():
        raise RuntimeError("User authentication requires NTL_HISTORY_DB_URL or NTL_LANGGRAPH_POSTGRES_URL.")
    _db_setup()


def _db_user_from_row(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {
        "user_id": str(row[0] or ""),
        "username": str(row[1] or ""),
        "username_key": str(row[2] or ""),
        "password_hash": str(row[3] or ""),
        "legacy_migrated_from": str(row[4] or ""),
        "created_at": int(row[5] or 0),
        "updated_at": int(row[6] or 0),
        "last_login_at": int(row[7] or 0),
        "is_active": bool(row[8]),
        "role": str(row[9] or USER_ROLE_USER),
        "is_admin": str(row[9] or USER_ROLE_USER) == USER_ROLE_ADMIN,
        "disabled_at": int(row[10] or 0),
        "disabled_reason": str(row[11] or ""),
    }


def _db_get_user_by_username_key(username_key: str) -> Optional[Dict[str, Any]]:
    row = _db_fetchone(
        """
        SELECT user_id, username, username_key, password_hash, legacy_migrated_from,
               created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason
        FROM users
        WHERE username_key = ?
        LIMIT 1
        """,
        """
        SELECT user_id, username, username_key, password_hash, legacy_migrated_from,
               created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason
        FROM users
        WHERE username_key = %s
        LIMIT 1
        """,
        (username_key,),
    )
    return _db_user_from_row(row)


def _db_get_user_by_user_id(user_id: str) -> Optional[Dict[str, Any]]:
    row = _db_fetchone(
        """
        SELECT user_id, username, username_key, password_hash, legacy_migrated_from,
               created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason
        FROM users
        WHERE user_id = ?
        LIMIT 1
        """,
        """
        SELECT user_id, username, username_key, password_hash, legacy_migrated_from,
               created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason
        FROM users
        WHERE user_id = %s
        LIMIT 1
        """,
        (user_id,),
    )
    return _db_user_from_row(row)


def _db_insert_user(
    *,
    user_id: str,
    username: str,
    username_key: str,
    password_hash: str,
    legacy_migrated_from: str = "",
) -> Dict[str, Any]:
    ts = _now_ts()
    role = _initial_role_for_username(username_key)
    _db_execute(
        """
        INSERT INTO users(user_id, username, username_key, password_hash, legacy_migrated_from, created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        """
        INSERT INTO users(user_id, username, username_key, password_hash, legacy_migrated_from, created_at, updated_at, last_login_at, is_active, role, disabled_at, disabled_reason)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (user_id, username, username_key, password_hash, legacy_migrated_from, ts, ts, 0, True, role, 0, ""),
    )
    return _db_get_user_by_user_id(user_id) or {}


def _db_mark_user_login(user_id: str) -> None:
    ts = _now_ts()
    _db_execute(
        "UPDATE users SET last_login_at = ?, updated_at = ? WHERE user_id = ?",
        "UPDATE users SET last_login_at = %s, updated_at = %s WHERE user_id = %s",
        (ts, ts, user_id),
    )


def _db_set_legacy_migrated_from(user_id: str, legacy_user_id: str) -> None:
    ts = _now_ts()
    _db_execute(
        "UPDATE users SET legacy_migrated_from = ?, updated_at = ? WHERE user_id = ?",
        "UPDATE users SET legacy_migrated_from = %s, updated_at = %s WHERE user_id = %s",
        (legacy_user_id, ts, user_id),
    )


def _db_profile_exists(user_id: str) -> bool:
    row = _db_fetchone(
        "SELECT 1 FROM user_profiles WHERE user_id = ? LIMIT 1",
        "SELECT 1 FROM user_profiles WHERE user_id = %s LIMIT 1",
        (user_id,),
    )
    return row is not None


def _db_thread_count_for_user(user_id: str) -> int:
    row = _db_fetchone(
        "SELECT COUNT(*) FROM user_threads WHERE user_id = ?",
        "SELECT COUNT(*) FROM user_threads WHERE user_id = %s",
        (user_id,),
    )
    return int((row[0] if row else 0) or 0)


def _db_chat_count(thread_id: str) -> int:
    row = _db_fetchone(
        "SELECT COUNT(*) FROM chat_records WHERE thread_id = ?",
        "SELECT COUNT(*) FROM chat_records WHERE thread_id = %s",
        (thread_id,),
    )
    return int((row[0] if row else 0) or 0)


def _db_turn_summary_count(thread_id: str) -> int:
    row = _db_fetchone(
        "SELECT COUNT(*) FROM turn_summaries WHERE thread_id = ?",
        "SELECT COUNT(*) FROM turn_summaries WHERE thread_id = %s",
        (thread_id,),
    )
    return int((row[0] if row else 0) or 0)


def _db_injected_context_count(thread_id: str) -> int:
    row = _db_fetchone(
        "SELECT COUNT(*) FROM injected_context WHERE thread_id = ?",
        "SELECT COUNT(*) FROM injected_context WHERE thread_id = %s",
        (thread_id,),
    )
    return int((row[0] if row else 0) or 0)


def _db_upsert_user_profile(user_id: str, user_name: str, *, created_at: int, updated_at: int) -> None:
    _db_execute(
        """
        INSERT INTO user_profiles(user_id, user_name, created_at, updated_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            user_name=excluded.user_name,
            updated_at=excluded.updated_at
        """,
        """
        INSERT INTO user_profiles(user_id, user_name, created_at, updated_at)
        VALUES(%s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            user_name=EXCLUDED.user_name,
            updated_at=EXCLUDED.updated_at
        """,
        (user_id, user_name, created_at, updated_at),
    )


def _normalize_gee_pipeline_mode(mode: str, gee_project_id: str = "") -> str:
    normalized = str(mode or "").strip().lower()
    if normalized not in GEE_PIPELINE_MODES:
        normalized = "default"
    if normalized == "user" and not str(gee_project_id or "").strip():
        return "default"
    return normalized


def _clean_gee_project_id(project_id: str) -> str:
    return str(project_id or "").strip()


def _db_gee_profile_from_row(row: Any, user_id: str) -> Dict[str, Any]:
    default_project = default_gee_project_id()
    if row:
        mode = _normalize_gee_pipeline_mode(str(row[1] or ""), str(row[2] or ""))
        user_project = _clean_gee_project_id(str(row[2] or ""))
        google_email = str(row[3] or "")
        encrypted_refresh_token = str(row[4] or "")
        token_scopes = str(row[5] or "")
        token_updated_at = int(row[6] or 0)
        status = str(row[7] or "unvalidated")
        last_error = str(row[8] or "")
        validated_at = int(row[9] or 0)
        updated_at = int(row[10] or 0)
    else:
        mode = "default"
        user_project = ""
        google_email = ""
        encrypted_refresh_token = ""
        token_scopes = ""
        token_updated_at = 0
        status = "unvalidated"
        last_error = ""
        validated_at = 0
        updated_at = 0

    effective_project = user_project if mode == "user" and user_project else default_project
    source = "user" if mode == "user" and user_project else "default"
    return {
        "user_id": str(user_id or "").strip(),
        "mode": source,
        "gee_project_id": user_project,
        "effective_project_id": effective_project,
        "source": source,
        "google_email": google_email,
        "encrypted_refresh_token": encrypted_refresh_token,
        "token_scopes": token_scopes,
        "token_updated_at": token_updated_at,
        "oauth_connected": bool(encrypted_refresh_token),
        "status": status,
        "last_error": last_error,
        "validated_at": validated_at,
        "updated_at": updated_at,
        "default_project_id": default_project,
        "user_project_configured": bool(user_project),
    }


def _db_get_user_gee_profile(user_id: str) -> Dict[str, Any]:
    row = _db_fetchone(
        """
        SELECT user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes, token_updated_at, status, last_error, validated_at, updated_at
        FROM user_gee_profiles
        WHERE user_id = ?
        LIMIT 1
        """,
        """
        SELECT user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes, token_updated_at, status, last_error, validated_at, updated_at
        FROM user_gee_profiles
        WHERE user_id = %s
        LIMIT 1
        """,
        (user_id,),
    )
    return _db_gee_profile_from_row(row, user_id)


def _db_save_user_gee_profile(
    user_id: str,
    *,
    mode: str,
    gee_project_id: str = "",
    status: str = "unvalidated",
    last_error: str = "",
    validated_at: int = 0,
) -> Dict[str, Any]:
    project_id = _clean_gee_project_id(gee_project_id)
    normalized_mode = _normalize_gee_pipeline_mode(mode, project_id)
    ts = _now_ts()
    _db_execute(
        """
        INSERT INTO user_gee_profiles(user_id, mode, gee_project_id, status, last_error, validated_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            mode=excluded.mode,
            gee_project_id=excluded.gee_project_id,
            status=excluded.status,
            last_error=excluded.last_error,
            validated_at=excluded.validated_at,
            updated_at=excluded.updated_at
        """,
        """
        INSERT INTO user_gee_profiles(user_id, mode, gee_project_id, status, last_error, validated_at, updated_at)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            mode=EXCLUDED.mode,
            gee_project_id=EXCLUDED.gee_project_id,
            status=EXCLUDED.status,
            last_error=EXCLUDED.last_error,
            validated_at=EXCLUDED.validated_at,
            updated_at=EXCLUDED.updated_at
        """,
        (user_id, normalized_mode, project_id, str(status or "unvalidated"), str(last_error or ""), int(validated_at or 0), ts),
    )
    return _db_get_user_gee_profile(user_id)


def _db_save_user_gee_oauth_token(
    user_id: str,
    *,
    google_email: str,
    encrypted_refresh_token: str,
    scopes: list[str] | tuple[str, ...],
) -> Dict[str, Any]:
    existing = _db_get_user_gee_profile(user_id)
    ts = _now_ts()
    scope_text = " ".join(str(scope).strip() for scope in scopes if str(scope).strip())
    _db_execute(
        """
        INSERT INTO user_gee_profiles(
            user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes,
            token_updated_at, status, last_error, validated_at, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            google_email=excluded.google_email,
            encrypted_refresh_token=excluded.encrypted_refresh_token,
            token_scopes=excluded.token_scopes,
            token_updated_at=excluded.token_updated_at,
            status=excluded.status,
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        """,
        """
        INSERT INTO user_gee_profiles(
            user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes,
            token_updated_at, status, last_error, validated_at, updated_at
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            google_email=EXCLUDED.google_email,
            encrypted_refresh_token=EXCLUDED.encrypted_refresh_token,
            token_scopes=EXCLUDED.token_scopes,
            token_updated_at=EXCLUDED.token_updated_at,
            status=EXCLUDED.status,
            last_error=EXCLUDED.last_error,
            updated_at=EXCLUDED.updated_at
        """,
        (
            user_id,
            str(existing.get("mode") or "default"),
            str(existing.get("gee_project_id") or ""),
            str(google_email or ""),
            str(encrypted_refresh_token or ""),
            scope_text,
            ts,
            "connected",
            "",
            int(existing.get("validated_at") or 0),
            ts,
        ),
    )
    return _db_get_user_gee_profile(user_id)


def _db_upsert_user_thread(user_id: str, thread_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    ts = _now_ts()
    meta = dict(meta or {})
    existing = _db_fetchone(
        """
        SELECT created_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt
        FROM user_threads
        WHERE thread_id = ?
        LIMIT 1
        """,
        """
        SELECT created_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt
        FROM user_threads
        WHERE thread_id = %s
        LIMIT 1
        """,
        (str(thread_id),),
    )
    if existing:
        existing_meta = {
            "created_at": int(existing[0] or ts),
            "workspace": str(existing[1] or ""),
            "thread_title": str(existing[2] or ""),
            "thread_title_manual": bool(existing[3]),
            "last_question": str(existing[4] or ""),
            "last_answer_excerpt": str(existing[5] or ""),
        }
        for key, value in existing_meta.items():
            meta.setdefault(key, value)
    created_at = int(meta.get("created_at", ts) or ts)
    updated_at = int(meta.get("updated_at", ts) or ts)
    workspace = str(meta.get("workspace") or (BASE_DIR / str(thread_id)).as_posix())
    thread_title = str(meta.get("thread_title", "") or "")
    thread_title_manual = bool(meta.get("thread_title_manual", False))
    last_question = str(meta.get("last_question", "") or "")
    last_answer_excerpt = str(meta.get("last_answer_excerpt", "") or "")
    _db_execute(
        """
        INSERT INTO user_threads(thread_id, user_id, created_at, updated_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(thread_id) DO UPDATE SET
            user_id=excluded.user_id,
            updated_at=excluded.updated_at,
            workspace=excluded.workspace,
            thread_title=excluded.thread_title,
            thread_title_manual=excluded.thread_title_manual,
            last_question=excluded.last_question,
            last_answer_excerpt=excluded.last_answer_excerpt
        """,
        """
        INSERT INTO user_threads(thread_id, user_id, created_at, updated_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(thread_id) DO UPDATE SET
            user_id=EXCLUDED.user_id,
            updated_at=EXCLUDED.updated_at,
            workspace=EXCLUDED.workspace,
            thread_title=EXCLUDED.thread_title,
            thread_title_manual=EXCLUDED.thread_title_manual,
            last_question=EXCLUDED.last_question,
            last_answer_excerpt=EXCLUDED.last_answer_excerpt
        """,
        (
            thread_id,
            user_id,
            created_at,
            updated_at,
            workspace,
            thread_title,
            thread_title_manual,
            last_question,
            last_answer_excerpt,
        ),
    )


def _db_insert_chat_record(thread_id: str, row: Dict[str, Any]) -> None:
    _db_execute(
        "INSERT OR IGNORE INTO chat_records(row_id, thread_id, ts, role, kind, content) VALUES(?, ?, ?, ?, ?, ?)",
        """
        INSERT INTO chat_records(row_id, thread_id, ts, role, kind, content)
        VALUES(%s, %s, %s, %s, %s, %s)
        ON CONFLICT(row_id) DO NOTHING
        """,
        (
            str(row.get("row_id") or uuid.uuid4().hex),
            thread_id,
            int(row.get("ts", _now_ts()) or _now_ts()),
            str(row.get("role", "") or ""),
            str(row.get("kind", "text") or "text"),
            str(row.get("content", "") or ""),
        ),
    )


def _db_insert_turn_summary(thread_id: str, row: Dict[str, Any]) -> None:
    _db_execute(
        "INSERT OR IGNORE INTO turn_summaries(row_id, thread_id, ts, payload_json) VALUES(?, ?, ?, ?)",
        """
        INSERT INTO turn_summaries(row_id, thread_id, ts, payload_json)
        VALUES(%s, %s, %s, %s)
        ON CONFLICT(row_id) DO NOTHING
        """,
        (
            str(row.get("row_id") or uuid.uuid4().hex),
            thread_id,
            int(row.get("ts", _now_ts()) or _now_ts()),
            json.dumps(row, ensure_ascii=False),
        ),
    )


def _db_upsert_context_item(thread_id: str, row: Dict[str, Any]) -> None:
    payload_json = json.dumps(row, ensure_ascii=False)
    row_key = _context_row_key(row)
    updated_at = int(row.get("created_at", _now_ts()) or _now_ts())
    _db_execute(
        """
        INSERT INTO injected_context(thread_id, row_key, updated_at, payload_json)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(thread_id, row_key) DO UPDATE SET
            updated_at=excluded.updated_at,
            payload_json=excluded.payload_json
        """,
        """
        INSERT INTO injected_context(thread_id, row_key, updated_at, payload_json)
        VALUES(%s, %s, %s, %s)
        ON CONFLICT(thread_id, row_key) DO UPDATE SET
            updated_at=EXCLUDED.updated_at,
            payload_json=EXCLUDED.payload_json
        """,
        (thread_id, row_key, updated_at, payload_json),
    )


def _validate_username_or_raise(username: str) -> tuple[str, str]:
    value = str(username or "").strip()
    username_key = normalize_user_id(value)
    if not value:
        raise ValueError("Username is required.")
    if is_reserved_user_id(username_key):
        raise ValueError("That username is reserved.")
    if not USERNAME_RE.fullmatch(value):
        raise ValueError("Username must be 3-40 characters using English letters, numbers, '_' or '-'.")
    if username_key != normalize_user_id(value):
        raise ValueError("Username normalization failed.")
    return value, username_key


def _validate_password_or_raise(password: str) -> str:
    value = str(password or "")
    if len(value) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")
    return value


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(str(password or "").encode("utf-8"), str(password_hash or "").encode("utf-8"))
    except ValueError:
        return False


def _db_thread_ids_for_user(user_id: str) -> list[str]:
    rows = _db_fetchall(
        "SELECT thread_id FROM user_threads WHERE user_id = ? ORDER BY updated_at DESC",
        "SELECT thread_id FROM user_threads WHERE user_id = %s ORDER BY updated_at DESC",
        (user_id,),
    )
    return [str(row[0] or "").strip() for row in rows if row and str(row[0] or "").strip()]


def _legacy_user_has_data(user_id: str) -> bool:
    uid = normalize_user_id(user_id)
    if _db_enabled():
        _db_setup()
        if _db_profile_exists(uid):
            return True
        if _db_thread_count_for_user(uid) > 0:
            return True
    idx_path = _threads_index_path(uid)
    profile_path = _user_dir(uid) / "profile.json"
    return idx_path.exists() or profile_path.exists()


def _migrate_thread_artifacts_to_db(thread_ids: list[str]) -> None:
    for thread_id in thread_ids:
        _migrate_chat_from_file_if_needed(thread_id)
        _migrate_turn_summaries_from_file_if_needed(thread_id)
        _migrate_context_from_file_if_needed(thread_id)


def _db_move_thread_ownership(old_user_id: str, new_user_id: str) -> None:
    _db_execute(
        "UPDATE user_threads SET user_id = ? WHERE user_id = ?",
        "UPDATE user_threads SET user_id = %s WHERE user_id = %s",
        (new_user_id, old_user_id),
    )


def _db_delete_user_profile(user_id: str) -> None:
    _db_execute(
        "DELETE FROM user_profiles WHERE user_id = ?",
        "DELETE FROM user_profiles WHERE user_id = %s",
        (user_id,),
    )


def _move_legacy_user_files(old_user_id: str, new_user_id: str) -> None:
    old_dir = _user_dir(old_user_id)
    new_dir = _user_dir(new_user_id)
    if not old_dir.exists() or old_dir == new_dir:
        return
    new_dir.mkdir(parents=True, exist_ok=True)
    for child in old_dir.iterdir():
        target = new_dir / child.name
        if target.exists():
            continue
        try:
            shutil.move(str(child), str(target))
        except Exception:
            continue
    try:
        old_dir.rmdir()
    except OSError:
        pass


def _migrate_deepagents_store_memory(old_user_id: str, new_user_id: str) -> None:
    postgres_url = str(langgraph_postgres_url() or "").strip()
    if not postgres_url:
        return
    try:
        from langgraph.store.postgres import PostgresStore
    except Exception:
        return

    old_prefix = (sanitize_namespace_part(ASSISTANT_ID, ASSISTANT_ID), sanitize_namespace_part(old_user_id, "guest"))
    new_user_part = sanitize_namespace_part(new_user_id, "guest")
    with PostgresStore.from_conn_string(postgres_url) as store:
        namespaces = store.list_namespaces(prefix=old_prefix, max_depth=3, limit=1000)
        for namespace in namespaces:
            if len(namespace) < 2 or tuple(namespace[:2]) != old_prefix:
                continue
            new_namespace = (namespace[0], new_user_part, *namespace[2:])
            for item in store.search(tuple(namespace), limit=1000):
                store.put(new_namespace, item.key, item.value)
                store.delete(tuple(namespace), item.key)


def _migrate_legacy_user_to_account(new_user_id: str, username: str) -> str:
    legacy_user_id = normalize_user_id(username)
    if legacy_user_id == normalize_user_id(new_user_id):
        return ""

    if not _legacy_user_has_data(legacy_user_id):
        return ""

    _migrate_profile_from_file_if_needed(legacy_user_id)
    _migrate_threads_from_file_if_needed(legacy_user_id)
    thread_ids = _db_thread_ids_for_user(legacy_user_id)
    _migrate_thread_artifacts_to_db(thread_ids)
    _db_move_thread_ownership(legacy_user_id, new_user_id)
    _db_delete_user_profile(legacy_user_id)
    _move_legacy_user_files(legacy_user_id, new_user_id)
    _migrate_deepagents_store_memory(legacy_user_id, new_user_id)
    ensure_user_profile(new_user_id, username)
    _db_set_legacy_migrated_from(new_user_id, legacy_user_id)
    return legacy_user_id


def _db_delete_thread_rows(thread_id: str, user_id: str) -> bool:
    deleted = False
    row = _db_fetchone(
        "SELECT 1 FROM user_threads WHERE thread_id = ? AND user_id = ? LIMIT 1",
        "SELECT 1 FROM user_threads WHERE thread_id = %s AND user_id = %s LIMIT 1",
        (thread_id, user_id),
    )
    if row is not None:
        deleted = True
    _db_execute(
        "DELETE FROM user_threads WHERE thread_id = ? AND user_id = ?",
        "DELETE FROM user_threads WHERE thread_id = %s AND user_id = %s",
        (thread_id, user_id),
    )
    for table in ("chat_records", "turn_summaries", "injected_context"):
        _db_execute(
            f"DELETE FROM {table} WHERE thread_id = ?",
            f"DELETE FROM {table} WHERE thread_id = %s",
            (thread_id,),
        )
    return deleted


def _migrate_profile_from_file_if_needed(user_id: str) -> None:
    if not _db_enabled():
        return
    _db_setup()
    uid = normalize_user_id(user_id)
    if _db_profile_exists(uid):
        return
    payload = _safe_read_json(_user_dir(uid) / "profile.json", {})
    ts = _now_ts()
    _db_upsert_user_profile(
        uid,
        str(payload.get("user_name", "") or uid).strip() or uid,
        created_at=int(payload.get("created_at", ts) or ts),
        updated_at=int(payload.get("updated_at", ts) or ts),
    )


def _migrate_threads_from_file_if_needed(user_id: str) -> None:
    if not _db_enabled():
        return
    _db_setup()
    uid = normalize_user_id(user_id)
    if _db_thread_count_for_user(uid) > 0:
        return
    rows = _safe_read_json(_threads_index_path(uid), {"threads": []}).get("threads", [])
    if not isinstance(rows, list):
        return
    for item in rows:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("thread_id", "")).strip()
        if not tid:
            continue
        _db_upsert_user_thread(uid, tid, meta=item)


def _migrate_chat_from_file_if_needed(thread_id: str) -> None:
    if not _db_enabled():
        return
    _db_setup()
    if _db_chat_count(thread_id) > 0:
        return
    path = _chat_jsonl_path(thread_id)
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    _db_insert_chat_record(thread_id, obj)
    except Exception:
        return


def _migrate_turn_summaries_from_file_if_needed(thread_id: str) -> None:
    if not _db_enabled():
        return
    _db_setup()
    if _db_turn_summary_count(thread_id) > 0:
        return
    path = _turn_summary_jsonl_path(thread_id)
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    _db_insert_turn_summary(thread_id, obj)
    except Exception:
        return


def _migrate_context_from_file_if_needed(thread_id: str) -> None:
    if not _db_enabled():
        return
    _db_setup()
    if _db_injected_context_count(thread_id) > 0:
        return
    payload = _safe_read_json(_context_index_path(thread_id), {"items": []})
    items = payload.get("items", [])
    if not isinstance(items, list):
        return
    for item in items:
        if isinstance(item, dict):
            _db_upsert_context_item(thread_id, item)


def ensure_user_profile(user_id: str, user_name: str = "") -> Path:
    uid = normalize_user_id(user_id)
    if _db_enabled():
        _db_setup()
        _migrate_profile_from_file_if_needed(uid)
        ts = _now_ts()
        _db_upsert_user_profile(uid, (user_name or uid).strip() or uid, created_at=ts, updated_at=ts)
        return _user_dir(uid)

    udir = _user_dir(uid)
    udir.mkdir(parents=True, exist_ok=True)
    profile_path = udir / "profile.json"
    profile = _safe_read_json(profile_path, {})
    profile.update(
        {
            "user_id": uid,
            "user_name": (user_name or uid).strip() or uid,
            "updated_at": _now_ts(),
        }
    )
    if "created_at" not in profile:
        profile["created_at"] = _now_ts()
    _safe_write_json(profile_path, profile)
    if not _threads_index_path(uid).exists():
        _safe_write_json(_threads_index_path(uid), {"threads": []})
    return udir


def resolve_thread_title(row: Dict[str, Any]) -> str:
    title = str(row.get("thread_title", "") or "").strip()
    if title:
        return title
    question = str(row.get("last_question", "") or "").strip()
    if question:
        return _thread_title_from_question(question)
    return str(row.get("thread_id", "") or "").strip()


def get_registered_user(user_id: str) -> Optional[Dict[str, Any]]:
    _db_auth_required()
    return _db_get_user_by_user_id(str(user_id or "").strip())


def is_admin_user(user_id: str) -> bool:
    _db_auth_required()
    user = _db_get_user_by_user_id(str(user_id or "").strip())
    return bool(user and user.get("is_active") and str(user.get("role") or "") == USER_ROLE_ADMIN)


def _append_admin_audit(
    *,
    admin_user_id: str,
    target_user_id: str,
    action: str,
    reason: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    _db_execute(
        """
        INSERT INTO admin_audit_logs(row_id, ts, admin_user_id, target_user_id, action, reason, payload_json)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        """
        INSERT INTO admin_audit_logs(row_id, ts, admin_user_id, target_user_id, action, reason, payload_json)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            uuid.uuid4().hex,
            _now_ts(),
            str(admin_user_id or ""),
            str(target_user_id or ""),
            str(action or ""),
            str(reason or ""),
            json.dumps(payload or {}, ensure_ascii=False),
        ),
    )


def set_user_disabled(
    user_id: str,
    *,
    disabled: bool,
    reason: str = "",
    admin_user_id: str = "",
) -> Dict[str, Any]:
    _db_auth_required()
    uid = str(user_id or "").strip()
    if not _db_get_user_by_user_id(uid):
        raise ValueError("Unknown user_id.")
    ts = _now_ts()
    active_value = not bool(disabled)
    disabled_at = ts if disabled else 0
    disabled_reason = str(reason or "") if disabled else ""
    _db_execute(
        """
        UPDATE users
        SET is_active = ?, disabled_at = ?, disabled_reason = ?, updated_at = ?
        WHERE user_id = ?
        """,
        """
        UPDATE users
        SET is_active = %s, disabled_at = %s, disabled_reason = %s, updated_at = %s
        WHERE user_id = %s
        """,
        (active_value, disabled_at, disabled_reason, ts, uid),
    )
    if admin_user_id:
        _append_admin_audit(
            admin_user_id=admin_user_id,
            target_user_id=uid,
            action="disable_user" if disabled else "enable_user",
            reason=reason,
        )
    return _db_get_user_by_user_id(uid) or {}


def reset_user_password(
    user_id: str,
    new_password: str,
    *,
    admin_user_id: str = "",
    reason: str = "",
) -> Dict[str, Any]:
    _db_auth_required()
    uid = str(user_id or "").strip()
    if not _db_get_user_by_user_id(uid):
        raise ValueError("Unknown user_id.")
    password_value = _validate_password_or_raise(new_password)
    ts = _now_ts()
    _db_execute(
        """
        UPDATE users
        SET password_hash = ?, updated_at = ?
        WHERE user_id = ?
        """,
        """
        UPDATE users
        SET password_hash = %s, updated_at = %s
        WHERE user_id = %s
        """,
        (_hash_password(password_value), ts, uid),
    )
    if admin_user_id:
        _append_admin_audit(
            admin_user_id=admin_user_id,
            target_user_id=uid,
            action="reset_user_password",
            reason=reason,
            payload={"password_changed": True},
        )
    return _db_get_user_by_user_id(uid) or {}


def list_admin_users(limit: int = 100) -> List[Dict[str, Any]]:
    _db_auth_required()
    rows = _db_fetchall(
        """
        SELECT
            u.user_id, u.username, u.username_key, u.role, u.is_active, u.created_at, u.updated_at,
            u.last_login_at, u.disabled_at, u.disabled_reason,
            COALESCE(t.thread_count, 0),
            COALESCE(g.mode, 'default'), COALESCE(g.gee_project_id, ''),
            COALESCE(g.google_email, ''), COALESCE(g.encrypted_refresh_token, ''),
            COALESCE(g.status, 'unvalidated'), COALESCE(g.last_error, ''), COALESCE(g.validated_at, 0),
            COALESCE(g.updated_at, 0)
        FROM users u
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS thread_count
            FROM user_threads
            GROUP BY user_id
        ) t ON t.user_id = u.user_id
        LEFT JOIN user_gee_profiles g ON g.user_id = u.user_id
        ORDER BY u.updated_at DESC
        LIMIT ?
        """,
        """
        SELECT
            u.user_id, u.username, u.username_key, u.role, u.is_active, u.created_at, u.updated_at,
            u.last_login_at, u.disabled_at, u.disabled_reason,
            COALESCE(t.thread_count, 0),
            COALESCE(g.mode, 'default'), COALESCE(g.gee_project_id, ''),
            COALESCE(g.google_email, ''), COALESCE(g.encrypted_refresh_token, ''),
            COALESCE(g.status, 'unvalidated'), COALESCE(g.last_error, ''), COALESCE(g.validated_at, 0),
            COALESCE(g.updated_at, 0)
        FROM users u
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS thread_count
            FROM user_threads
            GROUP BY user_id
        ) t ON t.user_id = u.user_id
        LEFT JOIN user_gee_profiles g ON g.user_id = u.user_id
        ORDER BY u.updated_at DESC
        LIMIT %s
        """,
        (max(1, int(limit or 100)),),
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        gee_mode = str(row[11] or "default")
        gee_project_id = str(row[12] or "")
        out.append(
            {
                "user_id": str(row[0] or ""),
                "username": str(row[1] or ""),
                "username_key": str(row[2] or ""),
                "role": str(row[3] or USER_ROLE_USER),
                "is_admin": str(row[3] or USER_ROLE_USER) == USER_ROLE_ADMIN,
                "is_active": bool(row[4]),
                "created_at": int(row[5] or 0),
                "updated_at": int(row[6] or 0),
                "last_login_at": int(row[7] or 0),
                "disabled_at": int(row[8] or 0),
                "disabled_reason": str(row[9] or ""),
                "thread_count": int(row[10] or 0),
                "gee_mode": "user" if gee_mode == "user" and gee_project_id else "default",
                "gee_project_id": gee_project_id,
                "google_email": str(row[13] or ""),
                "oauth_connected": bool(str(row[14] or "")),
                "gee_status": str(row[15] or "unvalidated"),
                "gee_last_error": str(row[16] or ""),
                "gee_validated_at": int(row[17] or 0),
                "gee_updated_at": int(row[18] or 0),
            }
        )
    return out


def reset_user_gee_pipeline(user_id: str, *, admin_user_id: str = "", reason: str = "") -> Dict[str, Any]:
    _db_auth_required()
    uid = str(user_id or "").strip()
    if not _db_get_user_by_user_id(uid):
        raise ValueError("Unknown user_id.")
    ts = _now_ts()
    _db_execute(
        """
        INSERT INTO user_gee_profiles(
            user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes,
            token_updated_at, status, last_error, validated_at, updated_at
        )
        VALUES(?, 'default', '', '', '', '', 0, 'default', '', 0, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            mode='default',
            gee_project_id='',
            google_email='',
            encrypted_refresh_token='',
            token_scopes='',
            token_updated_at=0,
            status='default',
            last_error='',
            validated_at=0,
            updated_at=excluded.updated_at
        """,
        """
        INSERT INTO user_gee_profiles(
            user_id, mode, gee_project_id, google_email, encrypted_refresh_token, token_scopes,
            token_updated_at, status, last_error, validated_at, updated_at
        )
        VALUES(%s, 'default', '', '', '', '', 0, 'default', '', 0, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            mode='default',
            gee_project_id='',
            google_email='',
            encrypted_refresh_token='',
            token_scopes='',
            token_updated_at=0,
            status='default',
            last_error='',
            validated_at=0,
            updated_at=EXCLUDED.updated_at
        """,
        (uid, ts),
    )
    if admin_user_id:
        _append_admin_audit(
            admin_user_id=admin_user_id,
            target_user_id=uid,
            action="reset_gee_pipeline",
            reason=reason,
        )
    return _db_get_user_gee_profile(uid)


def get_user_gee_profile(user_id: str) -> Dict[str, Any]:
    _db_auth_required()
    uid = normalize_user_id(str(user_id or "").strip())
    if not uid:
        uid = "guest"
    return _db_get_user_gee_profile(uid)


def save_user_gee_profile(
    user_id: str,
    *,
    mode: str,
    gee_project_id: str = "",
    status: str = "unvalidated",
    last_error: str = "",
    validated_at: int = 0,
) -> Dict[str, Any]:
    _db_auth_required()
    uid = normalize_user_id(str(user_id or "").strip())
    if not _db_get_user_by_user_id(uid):
        raise ValueError("Unknown user_id.")
    return _db_save_user_gee_profile(
        uid,
        mode=mode,
        gee_project_id=gee_project_id,
        status=status,
        last_error=last_error,
        validated_at=validated_at,
    )


def save_user_gee_oauth_token(
    user_id: str,
    *,
    google_email: str,
    encrypted_refresh_token: str,
    scopes: list[str] | tuple[str, ...],
) -> Dict[str, Any]:
    _db_auth_required()
    uid = normalize_user_id(str(user_id or "").strip())
    if not _db_get_user_by_user_id(uid):
        raise ValueError("Unknown user_id.")
    return _db_save_user_gee_oauth_token(
        uid,
        google_email=google_email,
        encrypted_refresh_token=encrypted_refresh_token,
        scopes=scopes,
    )


def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    _db_auth_required()
    _validate_password_or_raise(password)
    _username, username_key = _validate_username_or_raise(username)
    user = _db_get_user_by_username_key(username_key)
    if not user or not user.get("is_active"):
        return None
    if not _verify_password(password, str(user.get("password_hash") or "")):
        return None
    _db_mark_user_login(str(user.get("user_id") or ""))
    ensure_user_profile(str(user.get("user_id") or ""), str(user.get("username") or username))
    return _db_get_user_by_user_id(str(user.get("user_id") or ""))


def register_user(username: str, password: str) -> Dict[str, Any]:
    _db_auth_required()
    normalized_username, username_key = _validate_username_or_raise(username)
    password_value = _validate_password_or_raise(password)
    if _db_get_user_by_username_key(username_key):
        raise ValueError("That username is already registered.")

    user_id = uuid.uuid4().hex
    user = _db_insert_user(
        user_id=user_id,
        username=normalized_username,
        username_key=username_key,
        password_hash=_hash_password(password_value),
    )
    ensure_user_profile(user_id, normalized_username)
    legacy_user_id = _migrate_legacy_user_to_account(user_id, normalized_username)
    _db_mark_user_login(user_id)
    refreshed = _db_get_user_by_user_id(user_id) or user
    refreshed["legacy_migrated_from"] = legacy_user_id or str(refreshed.get("legacy_migrated_from") or "")
    return refreshed


def bind_thread_to_user(user_id: str, thread_id: str, meta: Optional[Dict[str, Any]] = None) -> None:
    uid = normalize_user_id(user_id)
    if _db_enabled():
        ensure_user_profile(uid)
        _migrate_threads_from_file_if_needed(uid)
        _db_upsert_user_thread(uid, str(thread_id), meta=meta)
        return

    ensure_user_profile(uid)
    idx_path = _threads_index_path(uid)
    payload = _safe_read_json(idx_path, {"threads": []})
    rows = payload.get("threads", [])
    if not isinstance(rows, list):
        rows = []
    ts = _now_ts()
    entry = None
    for item in rows:
        if isinstance(item, dict) and str(item.get("thread_id", "")) == str(thread_id):
            entry = item
            break
    if entry is None:
        entry = {"thread_id": str(thread_id), "created_at": ts}
        rows.append(entry)
    entry["updated_at"] = ts
    entry["workspace"] = str((BASE_DIR / str(thread_id)).as_posix())
    if meta:
        for key, value in meta.items():
            entry[key] = value
    payload["threads"] = rows
    _safe_write_json(idx_path, payload)


def thread_belongs_to_user(user_id: str, thread_id: str) -> bool:
    uid = normalize_user_id(user_id)
    tid = str(thread_id or "").strip()
    if not uid or not tid:
        return False
    if _db_enabled():
        _db_setup()
        row = _db_fetchone(
            "SELECT 1 FROM user_threads WHERE thread_id = ? AND user_id = ? LIMIT 1",
            "SELECT 1 FROM user_threads WHERE thread_id = %s AND user_id = %s LIMIT 1",
            (tid, uid),
        )
        return row is not None
    rows = _safe_read_json(_threads_index_path(uid), {"threads": []}).get("threads", [])
    if not isinstance(rows, list):
        return False
    return any(isinstance(item, dict) and str(item.get("thread_id", "")).strip() == tid for item in rows)


def touch_thread_activity(
    user_id: str,
    thread_id: str,
    last_question: str = "",
    last_answer_excerpt: str = "",
) -> None:
    current = None
    rows = list_user_threads(user_id, limit=0)
    for row in rows:
        if str(row.get("thread_id", "")).strip() == str(thread_id).strip():
            current = row
            break

    meta = {}
    if current:
        for key in ("created_at", "workspace", "thread_title", "thread_title_manual", "last_question", "last_answer_excerpt"):
            if key in current:
                meta[key] = current.get(key)
    if last_question:
        normalized_question = str(last_question)[:240]
        meta["last_question"] = normalized_question
        title_manual = bool(meta.get("thread_title_manual", False))
        current_title = str(meta.get("thread_title", "") or "").strip()
        if (not title_manual) and (not current_title):
            meta["thread_title"] = _thread_title_from_question(normalized_question)
            meta["thread_title_manual"] = False
    if last_answer_excerpt:
        meta["last_answer_excerpt"] = str(last_answer_excerpt)[:400]
    bind_thread_to_user(user_id, thread_id, meta=meta)


def list_user_threads(user_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    uid = normalize_user_id(user_id)
    if _db_enabled():
        ensure_user_profile(uid)
        _migrate_threads_from_file_if_needed(uid)
        sql_sqlite = """
            SELECT thread_id, user_id, created_at, updated_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt
            FROM user_threads
            WHERE user_id = ?
            ORDER BY updated_at DESC
        """
        sql_postgres = """
            SELECT thread_id, user_id, created_at, updated_at, workspace, thread_title, thread_title_manual, last_question, last_answer_excerpt
            FROM user_threads
            WHERE user_id = %s
            ORDER BY updated_at DESC
        """
        params: tuple[Any, ...] = (uid,)
        if limit > 0:
            sql_sqlite += " LIMIT ?"
            sql_postgres += " LIMIT %s"
            params = (uid, int(limit))
        rows = _db_fetchall(sql_sqlite, sql_postgres, params)
        return [
            {
                "thread_id": str(row[0]),
                "user_id": str(row[1]),
                "created_at": int(row[2] or 0),
                "updated_at": int(row[3] or 0),
                "workspace": str(row[4] or ""),
                "thread_title": str(row[5] or ""),
                "thread_title_manual": int(bool(row[6])),
                "last_question": str(row[7] or ""),
                "last_answer_excerpt": str(row[8] or ""),
            }
            for row in rows
        ]

    rows = _safe_read_json(_threads_index_path(uid), {"threads": []}).get("threads", [])
    if not isinstance(rows, list):
        return []
    normalized = [item for item in rows if isinstance(item, dict) and item.get("thread_id")]
    normalized.sort(key=lambda x: int(x.get("updated_at", 0) or 0), reverse=True)
    for item in normalized:
        item.setdefault("thread_title", "")
        item["thread_title_manual"] = int(bool(item.get("thread_title_manual", False)))
    if limit > 0:
        normalized = normalized[:limit]
    return normalized


def rename_user_thread(user_id: str, thread_id: str, title: str) -> Dict[str, Any]:
    uid = normalize_user_id(user_id)
    tid = str(thread_id or "").strip()
    new_title = " ".join(str(title or "").strip().split())
    if not tid:
        raise ValueError("Thread ID is required.")
    if not new_title:
        raise ValueError("Thread title cannot be empty.")
    if len(new_title) > 120:
        new_title = new_title[:117].rstrip() + "..."

    rows = list_user_threads(uid, limit=0)
    target = None
    for row in rows:
        if str(row.get("thread_id", "")).strip() == tid:
            target = row
            break
    if target is None:
        raise ValueError("Thread not found.")

    meta = {
        "created_at": target.get("created_at"),
        "workspace": target.get("workspace"),
        "thread_title": new_title,
        "thread_title_manual": True,
        "last_question": target.get("last_question", ""),
        "last_answer_excerpt": target.get("last_answer_excerpt", ""),
    }
    bind_thread_to_user(uid, tid, meta=meta)

    refreshed = list_user_threads(uid, limit=0)
    for row in refreshed:
        if str(row.get("thread_id", "")).strip() == tid:
            return row
    raise ValueError("Thread rename did not persist.")


def delete_user_thread(user_id: str, thread_id: str, delete_workspace: bool = True) -> Dict[str, Any]:
    uid = normalize_user_id(user_id)
    tid = str(thread_id or "").strip()
    result = {
        "deleted": False,
        "thread_id": tid,
        "index_removed": False,
        "workspace_removed": False,
        "workspace_path": str((BASE_DIR / tid).as_posix()) if tid else "",
    }
    if not tid:
        return result

    if _db_enabled():
        _db_setup()
        _migrate_threads_from_file_if_needed(uid)
        result["index_removed"] = _db_delete_thread_rows(tid, uid)
    else:
        idx_path = _threads_index_path(uid)
        payload = _safe_read_json(idx_path, {"threads": []})
        rows = payload.get("threads", [])
        if not isinstance(rows, list):
            rows = []
        new_rows = [
            item
            for item in rows
            if not (isinstance(item, dict) and str(item.get("thread_id", "")).strip() == tid)
        ]
        if len(new_rows) != len(rows):
            payload["threads"] = new_rows
            _safe_write_json(idx_path, payload)
            result["index_removed"] = True

    if delete_workspace:
        workspace = BASE_DIR / tid
        if workspace.exists():
            try:
                shutil.rmtree(workspace, ignore_errors=False)
                result["workspace_removed"] = not workspace.exists()
            except Exception:
                result["workspace_removed"] = False

    result["deleted"] = bool(result["index_removed"] or result["workspace_removed"])
    return result


def append_chat_record(thread_id: str, role: str, content: str, kind: str = "text") -> None:
    row = {
        "row_id": uuid.uuid4().hex,
        "ts": _now_ts(),
        "role": str(role),
        "kind": str(kind),
        "content": str(content or ""),
    }
    if _db_enabled():
        _db_setup()
        _migrate_chat_from_file_if_needed(thread_id)
        _db_insert_chat_record(thread_id, row)
        return
    _safe_append_jsonl(_chat_jsonl_path(thread_id), row)


def load_chat_records(thread_id: str, limit: int = 300) -> List[Dict[str, Any]]:
    if _db_enabled():
        _db_setup()
        _migrate_chat_from_file_if_needed(thread_id)
        rows = _db_fetchall(
            """
            SELECT ts, role, kind, content
            FROM chat_records
            WHERE thread_id = ?
            ORDER BY ts ASC
            """,
            """
            SELECT ts, role, kind, content
            FROM chat_records
            WHERE thread_id = %s
            ORDER BY ts ASC
            """,
            (thread_id,),
        )
        out = [
            {"ts": int(row[0] or 0), "role": str(row[1] or ""), "kind": str(row[2] or ""), "content": str(row[3] or "")}
            for row in rows
        ]
        if limit > 0 and len(out) > limit:
            return out[-limit:]
        return out

    path = _chat_jsonl_path(thread_id)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                    if isinstance(obj, dict):
                        rows.append(obj)
                except Exception:
                    continue
    except Exception:
        return []
    if limit > 0 and len(rows) > limit:
        return rows[-limit:]
    return rows


def append_turn_summary(thread_id: str, summary: Dict[str, Any]) -> None:
    row = {"row_id": uuid.uuid4().hex, "ts": _now_ts()}
    row.update(summary or {})
    if _db_enabled():
        _db_setup()
        _migrate_turn_summaries_from_file_if_needed(thread_id)
        _db_insert_turn_summary(thread_id, row)
        return
    _safe_append_jsonl(_turn_summary_jsonl_path(thread_id), row)


def load_turn_summaries(thread_id: str, limit: int = 12) -> List[Dict[str, Any]]:
    tid = str(thread_id or "").strip()
    if not tid:
        return []
    if _db_enabled():
        _db_setup()
        _migrate_turn_summaries_from_file_if_needed(tid)
        rows = _db_fetchall(
            """
            SELECT payload_json
            FROM turn_summaries
            WHERE thread_id = ?
            ORDER BY ts ASC
            """,
            """
            SELECT payload_json
            FROM turn_summaries
            WHERE thread_id = %s
            ORDER BY ts ASC
            """,
            (tid,),
        )
        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(str(row[0] or ""))
            except Exception:
                continue
            if isinstance(payload, dict):
                out.append(payload)
        if limit > 0 and len(out) > limit:
            return out[-limit:]
        return out

    path = _turn_summary_jsonl_path(tid)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                    if isinstance(obj, dict):
                        rows.append(obj)
                except Exception:
                    continue
    except Exception:
        return []
    if limit > 0 and len(rows) > limit:
        return rows[-limit:]
    return rows


def load_injected_context_items(thread_id: str) -> List[Dict[str, Any]]:
    if _db_enabled():
        _db_setup()
        _migrate_context_from_file_if_needed(thread_id)
        rows = _db_fetchall(
            """
            SELECT payload_json
            FROM injected_context
            WHERE thread_id = ?
            ORDER BY updated_at DESC
            """,
            """
            SELECT payload_json
            FROM injected_context
            WHERE thread_id = %s
            ORDER BY updated_at DESC
            """,
            (thread_id,),
        )
        items: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(str(row[0] or ""))
            except Exception:
                continue
            if isinstance(payload, dict) and str(payload.get("text", "")).strip():
                items.append(payload)
        return items

    payload = _safe_read_json(_context_index_path(thread_id), {"items": []})
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict) and str(x.get("text", "")).strip()]


def save_injected_context_items(thread_id: str, items: List[Dict[str, Any]]) -> None:
    if _db_enabled():
        _db_setup()
        _db_execute(
            "DELETE FROM injected_context WHERE thread_id = ?",
            "DELETE FROM injected_context WHERE thread_id = %s",
            (thread_id,),
        )
        for row in items:
            if isinstance(row, dict):
                _db_upsert_context_item(thread_id, row)
        return

    payload = {"updated_at": _now_ts(), "items": items}
    _safe_write_json(_context_index_path(thread_id), payload)


def clear_injected_context(thread_id: str) -> None:
    save_injected_context_items(thread_id, [])


def upsert_injected_context_items(thread_id: str, items: List[Dict[str, Any]]) -> Dict[str, int]:
    current = load_injected_context_items(thread_id)
    key_to_row: Dict[str, Dict[str, Any]] = {}
    for row in current:
        key = _context_row_key(row)
        key_to_row[key] = row
    inserted = 0
    updated = 0
    for row in items:
        key = _context_row_key(row)
        if key in key_to_row:
            key_to_row[key] = row
            updated += 1
        else:
            key_to_row[key] = row
            inserted += 1
    merged = list(key_to_row.values())
    merged.sort(key=lambda x: int(x.get("created_at", 0) or 0))
    save_injected_context_items(thread_id, merged)
    return {"inserted": inserted, "updated": updated, "total": len(merged)}


def injected_file_overview(thread_id: str) -> List[Dict[str, Any]]:
    rows = load_injected_context_items(thread_id)
    by_file: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        f = str(row.get("source_file", "")).strip()
        if not f:
            continue
        item = by_file.setdefault(
            f,
            {"source_file": f, "file_type": row.get("file_type", "unknown"), "chunks": 0, "updated_at": 0},
        )
        item["chunks"] += 1
        item["updated_at"] = max(int(item.get("updated_at", 0) or 0), int(row.get("created_at", 0) or 0))
    out = list(by_file.values())
    out.sort(key=lambda x: int(x.get("updated_at", 0) or 0), reverse=True)
    return out


def _fallback_similarity_scores(query: str, docs: List[str]) -> List[float]:
    q_tokens = set(re.findall(r"[a-z0-9_]+", query.lower()))
    if not q_tokens:
        return [0.0 for _ in docs]
    scores: List[float] = []
    for text in docs:
        d_tokens = set(re.findall(r"[a-z0-9_]+", text.lower()))
        if not d_tokens:
            scores.append(0.0)
            continue
        inter = len(q_tokens.intersection(d_tokens))
        scores.append(inter / max(1, len(q_tokens)))
    return scores


def _extract_file_mentions(query: str) -> List[str]:
    text = str(query or "").strip().lower()
    if not text:
        return []
    matches = re.findall(
        r"([a-z0-9_\-\u4e00-\u9fff]+\.(?:png|jpg|jpeg|webp|bmp|pdf|tif|tiff))",
        text,
        flags=re.IGNORECASE,
    )
    uniq = []
    seen = set()
    for m in matches:
        key = str(m).lower()
        if key not in seen:
            seen.add(key)
            uniq.append(key)
    return uniq


def _normalize_for_filename_match(text: str) -> str:
    t = str(text or "").strip().lower()
    return re.sub(r"\s+", "", t)


def _match_sources_by_normalized_query(query: str, source_names: List[str]) -> List[str]:
    q_norm = _normalize_for_filename_match(query)
    if not q_norm:
        return []
    out: List[str] = []
    seen = set()
    for name in source_names:
        n = str(name or "").strip()
        if not n:
            continue
        n_norm = _normalize_for_filename_match(n)
        if n_norm and n_norm in q_norm:
            key = n.lower()
            if key not in seen:
                seen.add(key)
                out.append(key)
    return out


def _looks_like_image_question(query: str) -> bool:
    q = str(query or "").strip().lower()
    if not q:
        return False
    image_markers = [
        "鍥剧墖",
        "鍥惧儚",
        "鐓х墖",
        "鎴浘",
        "杩欏紶鍥?",
        "鐪嬪浘",
        "image",
        "picture",
        "photo",
        "screenshot",
    ]
    return any(tok in q for tok in image_markers)


def retrieve_relevant_context(
    thread_id: str,
    query: str,
    top_n: int = 4,
    max_chars: int = 6000,
    min_score: float = 0.01,
) -> List[Dict[str, Any]]:
    items = load_injected_context_items(thread_id)
    if not items:
        return []
    query_text = str(query or "").strip()
    query_lower = query_text.lower()

    mentioned_files = set(_extract_file_mentions(query_text))
    if not mentioned_files:
        mentioned_files = set(
            _match_sources_by_normalized_query(
                query_text,
                [str(x.get("source_file", "")) for x in items if isinstance(x, dict)],
            )
        )
    if mentioned_files:
        file_hits = []
        for item in items:
            source = str(item.get("source_file", "")).strip().lower()
            if source and source in mentioned_files:
                file_hits.append(item)
        if file_hits:
            file_hits.sort(key=lambda x: int(x.get("created_at", 0) or 0), reverse=True)
            out: List[Dict[str, Any]] = []
            chars = 0
            for item in file_hits:
                if len(out) >= max(1, int(top_n)):
                    break
                text = str(item.get("text", "")).strip()
                if not text:
                    continue
                if chars + len(text) > max_chars and out:
                    break
                chars += len(text)
                row = dict(item)
                row["score"] = 1.0
                out.append(row)
            if out:
                return out

    docs = [str(x.get("text", "")) for x in items]
    scores: List[float]
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity

        vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
        matrix = vec.fit_transform(docs + [query])
        sims = cosine_similarity(matrix[-1], matrix[:-1]).flatten()
        scores = [float(s) for s in sims]
    except Exception:
        scores = _fallback_similarity_scores(query, docs)

    adjusted_scores: List[float] = []
    for idx, base in enumerate(scores):
        item = items[idx]
        source = str(item.get("source_file", "")).strip().lower()
        boost = 0.0
        if source and source in query_lower:
            boost += 0.85
        if source:
            stem = Path(source).stem.lower()
            if stem and stem in query_lower:
                boost += 0.45
        adjusted_scores.append(float(base) + boost)

    ranked = sorted(enumerate(adjusted_scores), key=lambda x: x[1], reverse=True)
    out: List[Dict[str, Any]] = []
    char_count = 0
    for idx, score in ranked:
        if len(out) >= max(1, int(top_n)):
            break
        if score < float(min_score):
            continue
        item = items[idx]
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        if char_count + len(text) > max_chars and out:
            break
        char_count += len(text)
        row = dict(item)
        row["score"] = round(float(score), 4)
        out.append(row)

    if not out and _looks_like_image_question(query_text):
        image_items = [x for x in items if str(x.get("file_type", "")).lower() == "image"]
        if image_items:
            image_items.sort(key=lambda x: int(x.get("created_at", 0) or 0), reverse=True)
            chars = 0
            for item in image_items:
                if len(out) >= max(1, int(top_n)):
                    break
                text = str(item.get("text", "")).strip()
                if not text:
                    continue
                if chars + len(text) > max_chars and out:
                    break
                chars += len(text)
                row = dict(item)
                row["score"] = 0.2
                out.append(row)
    return out


def thread_exists(thread_id: str) -> bool:
    if _db_enabled():
        _db_setup()
        row = _db_fetchone(
            "SELECT 1 FROM user_threads WHERE thread_id = ? LIMIT 1",
            "SELECT 1 FROM user_threads WHERE thread_id = %s LIMIT 1",
            (thread_id,),
        )
        if row is not None:
            return True
    return (BASE_DIR / str(thread_id)).exists()


def thread_workspace(thread_id: str) -> Path:
    return BASE_DIR / str(thread_id)


def file_signature(path: Path) -> str:
    st = path.stat()
    return f"{path.name}:{int(st.st_mtime)}:{int(st.st_size)}"


def user_display_name(user_id: str) -> str:
    uid = normalize_user_id(user_id)
    if _db_enabled():
        _db_setup()
        _migrate_profile_from_file_if_needed(uid)
        row = _db_fetchone(
            "SELECT user_name FROM user_profiles WHERE user_id = ? LIMIT 1",
            "SELECT user_name FROM user_profiles WHERE user_id = %s LIMIT 1",
            (uid,),
        )
        if row is not None:
            name = str(row[0] or "").strip()
            return name or uid
    profile = _safe_read_json(_user_dir(uid) / "profile.json", {})
    name = str(profile.get("user_name", "")).strip()
    return name or uid
