from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable


def build_layer_signature(selected_layers: Iterable[Path | str]) -> str:
    normalized = sorted(str(Path(p).resolve()) for p in selected_layers)
    payload = "\n".join(normalized)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def advance_map_view_state(
    *,
    thread_id: str,
    layer_signature: str,
    opened_once_by_thread: dict[str, bool],
    last_layer_sig_by_thread: dict[str, str],
    reset_nonce_by_thread: dict[str, int],
) -> dict[str, int | bool]:
    is_first_open = not bool(opened_once_by_thread.get(thread_id, False))
    previous_signature = last_layer_sig_by_thread.get(thread_id)
    is_layer_switched = previous_signature is not None and previous_signature != layer_signature

    if is_layer_switched:
        reset_nonce_by_thread[thread_id] = int(reset_nonce_by_thread.get(thread_id, 0)) + 1

    opened_once_by_thread[thread_id] = True
    last_layer_sig_by_thread[thread_id] = layer_signature

    return {
        "is_first_open": is_first_open,
        "is_layer_switched": is_layer_switched,
        "map_nonce": int(reset_nonce_by_thread.get(thread_id, 0)),
    }
