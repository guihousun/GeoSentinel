from __future__ import annotations

import os
from pathlib import Path


def load_dotenv_file(dotenv_path: Path | None = None) -> dict[str, str]:
    path = dotenv_path or Path(".env")
    if not path.exists():
        return {}
    pairs: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key:
            pairs[key] = value
    return pairs


def get_env_or_dotenv(name: str, fallback_names: tuple[str, ...] = ()) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    dot = load_dotenv_file()
    if name in dot and dot[name]:
        os.environ.setdefault(name, dot[name])
        return dot[name]
    for key in fallback_names:
        v = os.getenv(key) or dot.get(key)
        if v:
            os.environ.setdefault(key, v)
            return v
    return None

