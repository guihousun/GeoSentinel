"""Reuse the established feed adapters without importing the legacy agent graph."""
import importlib.util
import json
import signal

signal.alarm(140)
spec = importlib.util.spec_from_file_location("monitor_sources", "/app/sources.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
items, sources = module.collect_monitor_candidates()
# Source exceptions may contain request details. Only publish bounded status codes.
for source in sources:
    if source.get("status") == "error":
        source["message"] = "数据源本轮暂不可用"
print(json.dumps({"items": items, "sources": sources}, ensure_ascii=False))
