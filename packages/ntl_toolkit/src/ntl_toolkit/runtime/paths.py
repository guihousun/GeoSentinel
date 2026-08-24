from pathlib import Path, PureWindowsPath


def resolve_local_path(raw_path: str | Path, workdir: str | Path) -> Path:
    raw_text = str(raw_path)
    windows_path = PureWindowsPath(raw_text)
    if windows_path.root and not windows_path.drive:
        raise ValueError(
            f"{raw_text} is a Windows partially-qualified path; "
            "a fully absolute path or ordinary relative path is required."
        )
    if windows_path.drive and not windows_path.root:
        raise ValueError(
            f"{raw_text} is a Windows partially-qualified path; "
            "a fully absolute path or ordinary relative path is required."
        )

    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = Path(workdir).expanduser() / path
    return path.resolve(strict=False)


def require_input_path(raw_path: str | Path, workdir: str | Path) -> Path:
    path = resolve_local_path(raw_path, workdir)
    if not path.exists():
        raise FileNotFoundError(str(path))
    return path


def reserve_output_path(raw_path: str | Path) -> Path:
    requested = Path(raw_path).expanduser().resolve(strict=False)
    requested.parent.mkdir(parents=True, exist_ok=True)
    if not requested.exists():
        return requested

    for index in range(1, 10000):
        candidate = requested.with_name(
            f"{requested.stem}_{index:03d}{requested.suffix}"
        )
        if not candidate.exists():
            return candidate

    raise RuntimeError(
        f"unable to reserve after 9999 attempts for {requested}; "
        "provide a different filename or remove an existing file."
    )
