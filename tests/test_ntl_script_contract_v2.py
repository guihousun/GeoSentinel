from __future__ import annotations

import importlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def _contract(*, output_name: str = "result.txt", mode: str = "execute") -> dict:
    return {
        "schema": "ntl.script.contract.v2",
        "objective": "Write one deterministic test artifact.",
        "input_manifest": [],
        "method_steps": ["write the declared output"],
        "parameters": {},
        "output_manifest": [{"path": f"outputs/{output_name}", "required": True}],
        "validation_checks": ["declared output exists and is non-empty"],
        "failure_gates": ["output missing or empty"],
        "execution": {
            "mode": mode,
            "timeout_seconds": 60,
            "overwrite_policy": "version",
            "network_scope": [],
            "test_strategy": "auto",
        },
    }


def _script(contract: dict, body: str) -> str:
    return f"NTL_SCRIPT_CONTRACT = {contract!r}\n\n{body}\n"


@pytest.fixture()
def code_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NTL_USER_DATA_DIR", str(tmp_path / "user_data"))
    monkeypatch.setenv("NTL_EXEC_SANDBOX", "1")
    monkeypatch.setenv("NTL_EXEC_SANDBOX_TIMEOUT_S", "60")

    import storage_manager
    from tools import NTL_Code_generation

    storage_manager = importlib.reload(storage_manager)
    module = importlib.reload(NTL_Code_generation)
    monkeypatch.setattr(module, "_archive_success_script", lambda *args, **kwargs: {"archived": False})
    token = storage_manager.current_thread_id.set("contract-v2-test")
    try:
        yield module, storage_manager
    finally:
        storage_manager.current_thread_id.reset(token)


def _save_script(storage_module, name: str, content: str) -> Path:
    path = Path(
        storage_module.storage_manager.resolve_output_path(
            name,
            thread_id="contract-v2-test",
        )
    )
    path.write_text(content, encoding="utf-8")
    return path


def test_contract_v1_is_rejected_without_conversion(code_runtime) -> None:
    module, storage_module = code_runtime
    contract = _contract()
    contract["schema"] = "ntl.script.contract.v1"
    script = _script(contract, "print('must not execute')")
    _save_script(storage_module, "legacy.py", script)

    result = json.loads(module.execute_geospatial_script("legacy.py"))

    assert result["status"] == "fail"
    assert result["error_type"] == "ScriptContractError"
    assert "only 'ntl.script.contract.v2' is accepted" in result["error_message"]
    assert Path(result["failure_artifacts"]["manifest_path"]).exists()


def test_valid_v2_script_executes_and_writes_manifest(code_runtime) -> None:
    module, storage_module = code_runtime
    script = _script(
        _contract(),
        "from pathlib import Path\n"
        "Path('outputs').mkdir(exist_ok=True)\n"
        "Path('outputs/result.txt').write_text('ok', encoding='utf-8')\n"
        "print('outputs/result.txt')",
    )
    _save_script(storage_module, "simple_task.py", script)

    result = json.loads(module.execute_geospatial_script("simple_task.py"))

    assert result["status"] == "success"
    assert result["contract_validation"]["schema"] == "ntl.script.contract.v2"
    assert result["contract_output_audit"]["pass"] is True
    manifest_path = Path(result["execution_manifest"]["manifest_path"])
    assert manifest_path.name == "simple_task.manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["contract_schema"] == "ntl.script.contract.v2"
    assert manifest["status"] == "success"


def test_plan_only_validates_but_does_not_execute(code_runtime) -> None:
    module, storage_module = code_runtime
    script = _script(
        _contract(output_name="never.txt", mode="plan_only"),
        "raise RuntimeError('plan_only must not execute')",
    )
    _save_script(storage_module, "plan_task.py", script)

    result = json.loads(module.execute_geospatial_script("plan_task.py"))

    assert result["status"] == "planned"
    assert result["execution_skipped"] is True
    output_path = Path(
        storage_module.storage_manager.resolve_output_path(
            "never.txt",
            thread_id="contract-v2-test",
        )
    )
    assert not output_path.exists()
    assert Path(result["execution_manifest"]["manifest_path"]).exists()


def test_missing_declared_output_fails_and_archives_diagnostics(code_runtime) -> None:
    module, storage_module = code_runtime
    script = _script(_contract(output_name="missing.txt"), "print('completed without artifact')")
    _save_script(storage_module, "missing_output.py", script)

    result = json.loads(module.execute_geospatial_script("missing_output.py"))

    assert result["status"] == "fail"
    assert result["error_type"] == "OutputValidationError"
    assert result["contract_output_audit"]["pass"] is False
    failure_artifacts = result["failure_artifacts"]
    assert "memory\\failed_runs" in failure_artifacts["manifest_path"]
    assert Path(failure_artifacts["script_path"]).exists()
    assert Path(failure_artifacts["log_path"]).exists()
    assert Path(failure_artifacts["manifest_path"]).exists()


def test_contract_rejects_more_than_one_light_repair(code_runtime) -> None:
    module, _ = code_runtime
    contract = _contract()
    contract["execution"]["repair_history"] = [
        {"reason": "fix import", "before": "import pands", "after": "import pandas"},
        {"reason": "fix typo", "before": "reslt.csv", "after": "result.csv"},
    ]

    validation = module._validate_ntl_script_contract(_script(contract, "print('ok')"))

    assert validation["pass"] is False
    assert any("at most one light repair" in error for error in validation["errors"])


def test_sandbox_exposes_script_file_context(code_runtime) -> None:
    module, storage_module = code_runtime
    script = _script(
        _contract(output_name="file_context.txt"),
        "from pathlib import Path\n"
        "Path('outputs').mkdir(exist_ok=True)\n"
        "Path('outputs/file_context.txt').write_text(Path(__file__).name, encoding='utf-8')\n"
        "print(__file__)",
    )
    _save_script(storage_module, "file_context.py", script)

    result = json.loads(module.execute_geospatial_script("file_context.py"))

    assert result["status"] == "success"
    output_path = Path(storage_module.storage_manager.resolve_output_path("file_context.txt"))
    assert output_path.read_text(encoding="utf-8").startswith("sandbox_exec_")


def test_concurrent_identical_execution_is_serialized_and_deduplicated(code_runtime) -> None:
    module, storage_module = code_runtime
    script = _script(
        _contract(output_name="concurrent.txt"),
        "import time\n"
        "from pathlib import Path\n"
        "time.sleep(0.2)\n"
        "Path('outputs').mkdir(exist_ok=True)\n"
        "with Path('outputs/concurrent.txt').open('a', encoding='utf-8') as handle:\n"
        "    handle.write('executed\\n')\n"
        "print('outputs/concurrent.txt')",
    )
    _save_script(storage_module, "concurrent.py", script)
    config = {"configurable": {"thread_id": "contract-v2-test"}}

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(module.execute_geospatial_script, "concurrent.py", config=config)
            for _ in range(2)
        ]
        results = [json.loads(future.result()) for future in futures]

    assert sorted(result.get("already_executed", False) for result in results) == [False, True]
    output_path = Path(storage_module.storage_manager.resolve_output_path("concurrent.txt"))
    assert output_path.read_text(encoding="utf-8").splitlines() == ["executed"]


def test_cross_workspace_output_is_rejected_without_copying(code_runtime) -> None:
    module, storage_module = code_runtime
    foreign_workspace = storage_module.storage_manager.get_workspace("another-thread")
    foreign_output = foreign_workspace / "outputs" / "foreign.txt"
    foreign_output.write_text("foreign", encoding="utf-8")
    script = _script(
        _contract(output_name="local.txt"),
        "from pathlib import Path\n"
        "Path('outputs').mkdir(exist_ok=True)\n"
        "Path('outputs/local.txt').write_text('local', encoding='utf-8')\n"
        f"print({str(foreign_output)!r})",
    )
    _save_script(storage_module, "cross_workspace.py", script)

    result = json.loads(module.execute_geospatial_script("cross_workspace.py"))

    assert result["status"] == "fail"
    assert result["error_type"] == "CrossWorkspaceOutputError"
    assert result["artifact_audit"]["pass"] is False
    current_outputs = storage_module.storage_manager.get_workspace("contract-v2-test") / "outputs"
    assert not (current_outputs / "foreign.txt").exists()
