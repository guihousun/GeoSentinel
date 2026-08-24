import importlib
import os
import re
from pathlib import Path

import pytest


def _runtime_function(name: str):
    runtime = importlib.import_module("ntl_toolkit.runtime")
    assert hasattr(runtime, name), f"ntl_toolkit.runtime missing {name}"
    return getattr(runtime, name)


def _runtime_module():
    return importlib.import_module("ntl_toolkit.runtime.paths")


def test_resolve_local_path_joins_relative_unicode_path_to_unicode_workdir(
    tmp_path: Path,
) -> None:
    resolve_local_path = _runtime_function("resolve_local_path")
    workdir = tmp_path / "工作目录"
    relative_path = Path("输入") / "夜间灯光.tif"

    result = resolve_local_path(relative_path, workdir)

    assert result == (workdir / relative_path).resolve(strict=False)


def test_resolve_local_path_keeps_absolute_paths_absolute_and_resolved(
    tmp_path: Path,
) -> None:
    resolve_local_path = _runtime_function("resolve_local_path")
    result = resolve_local_path(
        (tmp_path / "absolute" / ".." / "absolute" / "result.tif").resolve(strict=False),
        tmp_path / "ignored",
    )

    assert result == (tmp_path / "absolute" / "result.tif").resolve(strict=False)


def test_resolve_local_path_rejects_rooted_relative_windows_path(
    tmp_path: Path,
) -> None:
    resolve_local_path = _runtime_function("resolve_local_path")
    raw_path = r"\temp\a.txt"

    with pytest.raises(
        ValueError,
        match=(
            "^"
            + re.escape(raw_path)
            + r".*fully absolute path or ordinary relative path is required"
        ),
    ):
        resolve_local_path(raw_path, tmp_path)


def test_resolve_local_path_rejects_drive_relative_windows_path(
    tmp_path: Path,
) -> None:
    resolve_local_path = _runtime_function("resolve_local_path")
    raw_path = r"C:foo\bar.txt"

    with pytest.raises(
        ValueError,
        match=(
            "^"
            + re.escape(raw_path)
            + r".*fully absolute path or ordinary relative path is required"
        ),
    ):
        resolve_local_path(raw_path, tmp_path)


def test_require_input_path_returns_existing_file_and_raises_with_resolved_missing_path(
    tmp_path: Path,
) -> None:
    require_input_path = _runtime_function("require_input_path")
    workdir = tmp_path / "workspace"
    existing = workdir / "inputs" / "existing.txt"
    existing.parent.mkdir(parents=True)
    existing.write_text("ok", encoding="utf-8")

    assert require_input_path(Path("inputs") / "existing.txt", workdir) == existing.resolve(
        strict=False
    )

    missing = Path("inputs") / "missing.txt"
    expected_missing = (workdir / missing).resolve(strict=False)
    with pytest.raises(FileNotFoundError, match="^" + re.escape(str(expected_missing)) + "$"):
        require_input_path(missing, workdir)


def test_reserve_output_path_uses_001_when_requested_output_exists(tmp_path: Path) -> None:
    reserve_output_path = _runtime_function("reserve_output_path")
    requested = tmp_path / "render.tif"
    requested.write_text("taken", encoding="utf-8")

    result = reserve_output_path(requested)

    assert result == tmp_path / "render_001.tif"
    assert not result.exists()


def test_reserve_output_path_increments_existing_numbered_outputs_to_002(
    tmp_path: Path,
) -> None:
    reserve_output_path = _runtime_function("reserve_output_path")
    requested = tmp_path / "render.tif"
    requested.write_text("taken", encoding="utf-8")
    (tmp_path / "render_001.tif").write_text("taken", encoding="utf-8")

    result = reserve_output_path(requested)

    assert result == tmp_path / "render_002.tif"
    assert not result.exists()


def test_reserve_output_path_preserves_pathlib_multi_suffix_stem_behavior(
    tmp_path: Path,
) -> None:
    reserve_output_path = _runtime_function("reserve_output_path")
    requested = tmp_path / "archive.tar.gz"
    requested.write_text("taken", encoding="utf-8")

    result = reserve_output_path(requested)

    assert result == tmp_path / "archive.tar_001.gz"
    assert not result.exists()


def test_reserve_output_path_creates_parent_directory_without_creating_output_file(
    tmp_path: Path,
) -> None:
    reserve_output_path = _runtime_function("reserve_output_path")
    requested = tmp_path / "输出" / "maps" / "night-lights.tif"

    result = reserve_output_path(requested)

    assert result == requested.resolve(strict=False)
    assert result.parent.exists()
    assert not result.exists()


def test_reserve_output_path_exhaustion_raises_actionable_runtime_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reserve_output_path = _runtime_function("reserve_output_path")
    runtime_paths = _runtime_module()
    requested = (tmp_path / "render.tif").resolve(strict=False)

    def fake_exists(path: Path) -> bool:
        if path == requested:
            return True
        return path.parent == requested.parent and path.name.startswith("render_")

    monkeypatch.setattr(runtime_paths.Path, "exists", fake_exists)

    with pytest.raises(
        RuntimeError,
        match=(
            r"unable to reserve after 9999 attempts.*"
            + re.escape(str(requested))
        ),
    ):
        reserve_output_path(requested)


def test_load_runtime_environment_adds_missing_keys_without_overwriting_existing_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    load_runtime_environment = _runtime_function("load_runtime_environment")
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            [
                "NEW_KEY=loaded",
                "EXISTING_KEY=from_file",
                "IGNORED_NONE",
                "QUOTED_VALUE=hello world",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("NTL_MCP_ENV_FILE", str(env_file))
    monkeypatch.setenv("EXISTING_KEY", "from_env")
    monkeypatch.delenv("NEW_KEY", raising=False)
    monkeypatch.delenv("IGNORED_NONE", raising=False)
    monkeypatch.delenv("QUOTED_VALUE", raising=False)

    loaded = load_runtime_environment()

    assert loaded == {"NEW_KEY": "loaded", "QUOTED_VALUE": "hello world"}
    assert os.environ["NEW_KEY"] == "loaded"
    assert os.environ["EXISTING_KEY"] == "from_env"
    assert "IGNORED_NONE" not in os.environ


def test_load_runtime_environment_returns_empty_dict_when_env_file_setting_is_empty_or_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_runtime_environment = _runtime_function("load_runtime_environment")
    monkeypatch.delenv("NTL_MCP_ENV_FILE", raising=False)

    assert load_runtime_environment() == {}

    monkeypatch.setenv("NTL_MCP_ENV_FILE", "   ")
    assert load_runtime_environment() == {}


def test_runtime_workdir_prefers_unicode_env_value_and_defaults_to_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime_workdir = _runtime_function("runtime_workdir")
    configured = tmp_path / "线程工作目录"
    monkeypatch.setenv("NTL_MCP_WORKDIR", str(configured))

    assert runtime_workdir() == configured.expanduser().resolve()

    cwd = tmp_path / "cwd-default"
    cwd.mkdir()
    monkeypatch.delenv("NTL_MCP_WORKDIR", raising=False)
    monkeypatch.chdir(cwd)

    assert runtime_workdir() == cwd.resolve()
