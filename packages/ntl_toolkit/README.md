# ntl-toolkit

Shared installable domain core for NTL-GPT GIS and nighttime-light workflows.

## Install

From the repository root in the `NTL-GPT-Stable` environment:

```powershell
python -m pip install -e packages/ntl_toolkit
```

The package exposes two local stdio entry points:

- `ntl-gis-core` for deterministic local GIS and nighttime-light analysis;
  [`mcp_servers/gis_core_server.py`](../../mcp_servers/gis_core_server.py) is
  its checked-in launcher.
- `ntl-download` for synchronous explicit GEE export and audited official
  VNP46A2 Earthdata HDF5 country mosaics;
  [`mcp_servers/download_server.py`](../../mcp_servers/download_server.py) is
  its checked-in launcher. See [`docs/mcp/ntl-download.md`](../../docs/mcp/ntl-download.md).

## Runtime contract

- Relative paths resolve below `NTL_MCP_WORKDIR`; absolute Windows paths are
  also accepted.
- Inputs are read from the selected workspace. Generated files are written to
  the requested output location with an automatic suffix when a file exists.
- Tools never delete or overwrite files. `validate_environment`, inspection,
  NTL metric calculation, and geodata validation are read-only.
- Credentials are loaded from the process environment or
  `NTL_MCP_ENV_FILE`; they are not tool arguments or result fields.
- `ntl-download` emits MCP progress and writes sanitized run manifests. It
  does not create a background job queue or a database.

See [`tool_migration_manifest.json`](tool_migration_manifest.json) for legacy
capability mappings and parity status.
