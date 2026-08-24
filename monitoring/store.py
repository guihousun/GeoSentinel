from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from storage_manager import storage_manager


class MonitorStore:
    """Local, global event-monitor workspace shared by every web session."""

    def __init__(self, cache_dir: Path | None = None, **_ignored: Any) -> None:
        configured = str(os.getenv("NTL_MONITOR_DATA_DIR", "") or "").strip()
        self._cache_dir = cache_dir or (Path(configured) if configured else storage_manager.base_dir / "_monitor")
        self._state_path = self._cache_dir / "monitor_state.json"
        self._lease_path = self._cache_dir / "monitor_lease.json"
        self._lock = threading.RLock()

    def ensure_schema(self) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        if not self._state_path.exists():
            self._write_state({"events": [], "runs": []})

    def storage_status(self) -> dict[str, str]:
        return {"mode": "local_workspace", "reason": "所有会话共享项目本地监测工作区。"}

    def _read_state(self) -> dict[str, Any]:
        self.ensure_schema()
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        return {"events": list(payload.get("events") or []), "runs": list(payload.get("runs") or [])}

    def _write_state(self, payload: dict[str, Any]) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, self._state_path)

    def _read_lease(self) -> dict[str, Any]:
        try:
            return dict(json.loads(self._lease_path.read_text(encoding="utf-8")))
        except Exception:
            return {}

    def try_acquire_lease(self, owner_id: str, lease_seconds: int) -> bool:
        """Acquire a local, cross-process lease without involving the history database."""
        self.ensure_schema()
        now = int(time.time())
        until = now + max(60, int(lease_seconds))
        payload = {"owner_id": owner_id, "locked_until": until, "updated_at": now}
        with self._lock:
            for _attempt in range(2):
                try:
                    descriptor = os.open(self._lease_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
                except FileExistsError:
                    current = self._read_lease()
                    if int(current.get("locked_until") or 0) > now and current.get("owner_id") != owner_id:
                        return False
                    try:
                        self._lease_path.unlink()
                    except FileNotFoundError:
                        continue
                    except OSError:
                        return False
                    continue
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, ensure_ascii=False)
                return True
        return False

    def release_lease(self, owner_id: str) -> None:
        with self._lock:
            current = self._read_lease()
            if current.get("owner_id") != owner_id:
                return
            try:
                self._lease_path.unlink()
            except FileNotFoundError:
                return

    def record_run(self, record: dict[str, Any]) -> None:
        now = int(time.time())
        with self._lock:
            state = self._read_state()
            run_id = str(record.get("run_id") or "")
            runs = [item for item in state["runs"] if str(item.get("run_id") or "") != run_id]
            runs.append({**record, "updated_at": now})
            state["runs"] = sorted(runs, key=lambda item: int(item.get("updated_at") or 0), reverse=True)[:80]
            self._write_state(state)

    def upsert_events(self, events: list[dict[str, Any]], *, expire_before: int) -> None:
        now = int(time.time())
        with self._lock:
            state = self._read_state()
            by_key = {str(item.get("dedup_key") or ""): item for item in state["events"]}
            for event in events:
                key = str(event.get("dedup_key") or "")
                if not key:
                    continue
                previous = by_key.get(key, {})
                by_key[key] = {
                    **previous,
                    **event,
                    "first_seen_at": int(previous.get("first_seen_at") or event.get("first_seen_at") or now),
                    "last_seen_at": int(event.get("last_seen_at") or now),
                    "updated_at": now,
                    "is_active": 1,
                }
            state["events"] = [
                {**item, "is_active": 0 if int(item.get("last_seen_at") or 0) < expire_before else int(item.get("is_active", 1) or 0)}
                for item in by_key.values()
            ]
            self._write_state(state)

    def list_events(self, limit: int = 18) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 18), 100))
        with self._lock:
            events = [item for item in self._read_state()["events"] if int(item.get("is_active", 1) or 0)]
        return sorted(events, key=self._sort_key, reverse=True)[:safe_limit]

    def latest_status(self) -> dict[str, Any]:
        with self._lock:
            runs = sorted(self._read_state()["runs"], key=lambda item: int(item.get("updated_at") or 0), reverse=True)
        return dict(runs[0]) if runs else {}

    @staticmethod
    def _sort_key(item: dict[str, Any]) -> tuple[int, int]:
        severity_rank = {"high": 3, "medium": 2, "low": 1}.get(str(item.get("severity") or "low"), 1)
        return severity_rank, int(item.get("updated_at") or 0)
