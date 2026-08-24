# `ntl-gis-core` MCP

`ntl-gis-core` is a local, single-user stdio MCP service for deterministic
vector, raster, and nighttime-light operations. It exposes 16 atomic tools and
returns the shared `ntl.tool.result.v1` structured result. It is not a remote
service, does not provide authentication, and does not replace the GEE or
Earthdata services.

## Install and start

Use the supported `NTL-GPT-Stable` environment. From the repository root:

```powershell
python -m pip install -e packages/ntl_toolkit
```

The direct Windows startup command is:

```powershell
C:\ProgramData\Miniconda3\Scripts\conda.exe run -n NTL-GPT-Stable python D:\NTL-GPT-main\mcp_servers\gis_core_server.py
```

Codex-style TOML configuration:

```toml
[mcp_servers.ntl-gis-core]
command = "C:/ProgramData/Miniconda3/Scripts/conda.exe"
args = ["run", "-n", "NTL-GPT-Stable", "python", "D:/NTL-GPT-main/mcp_servers/gis_core_server.py"]
```

Claude Desktop JSON configuration:

```json
{
  "mcpServers": {
    "ntl-gis-core": {
      "command": "C:/ProgramData/Miniconda3/Scripts/conda.exe",
      "args": ["run", "-n", "NTL-GPT-Stable", "python", "D:/NTL-GPT-main/mcp_servers/gis_core_server.py"]
    }
  }
}
```

OpenClaw stdio configuration uses the same process contract:

```json
{
  "name": "ntl-gis-core",
  "transport": "stdio",
  "command": "C:/ProgramData/Miniconda3/Scripts/conda.exe",
  "args": ["run", "-n", "NTL-GPT-Stable", "python", "D:/NTL-GPT-main/mcp_servers/gis_core_server.py"]
}
```

For another checkout, replace both the Python script path and the package
installation path. Do not put credentials in the command-line arguments.

## Runtime and path rules

The server captures `NTL_MCP_WORKDIR` when it starts. Relative input and output
paths resolve below that directory; absolute Windows paths are accepted. An
unset `NTL_MCP_WORKDIR` uses the process working directory. Use ordinary paths
or fully qualified paths such as `D:/data/ntl_2023.tif`; partially qualified
paths such as `D:relative.tif` are rejected.

`NTL_MCP_ENV_FILE` may point to a local dotenv file. Existing process
environment variables take precedence. `NTL_MCP_STATE_DIR` is reserved for
future persistent MCP state and is not needed by the current synchronous GIS
tools. Never place `.env`, RAG stores, credentials, or `user_data` inside the
committed evaluation fixtures.

Read-only tools are `validate_environment`, `inspect_vector`,
`inspect_raster`, `calculate_ntl_metrics`, and `validate_geodata`. The other
tools create new outputs. No tool deletes or overwrites a file; if the
requested output exists, the runtime reserves a suffixed path such as
`result_001.tif`.

## Tool catalog

| Group | Tools |
| --- | --- |
| Read-only checks | `validate_environment`, `inspect_vector`, `inspect_raster`, `calculate_ntl_metrics`, `validate_geodata` |
| Vector outputs | `filter_points_by_polygon`, `spatial_join_points_to_admin`, `buffer_points_aeqd`, `dissolve_intersections` |
| Raster outputs | `clip_raster`, `reproject_raster`, `mosaic_rasters` |
| NTL outputs | `calculate_zonal_statistics`, `composite_ntl_rasters`, `analyze_ntl_trend`, `detect_ntl_anomaly` |

The service also publishes `ntl://gis/capabilities` and
`ntl://schemas/result-v1` resources. Accepted formats and common error codes
are defined in the capabilities resource and in
`packages/ntl_toolkit/src/ntl_toolkit/adapters/mcp/gis_capabilities.json`.

## Evaluation assets

`evaluations/ntl_gis_core.xml` contains one stable case for every tool. The
cases reference only the committed files under `evaluations/fixtures/` and
use `eval-output/` for generated files. An evaluation runner must copy the
fixtures into a temporary `NTL_MCP_WORKDIR` and discard generated outputs after
the run. Output-producing cases assert result structure and reopenability,
not a machine-specific absolute path.

## Troubleshooting

- `ModuleNotFoundError`, `ImportError`, or GDAL ABI errors: start the server
  through `conda.exe run -n NTL-GPT-Stable`, then run `python -m pip install -e
  packages/ntl_toolkit` inside that same environment. Do not mix a system
  Python with Conda's Rasterio/GeoPandas binaries.
- PROJ database or CRS lookup errors: verify the Conda environment's PROJ data
  is available and that `pyproj.datadir.get_data_dir()` points inside the
  environment. Reopen the client after correcting the environment.
- `INPUT_NOT_FOUND`: check `NTL_MCP_WORKDIR`, then use a relative path under
  the copied evaluation workspace or a fully qualified absolute path.
- `CRS_MISSING`, `CRS_MISMATCH`, or `GRID_MISMATCH`: inspect the inputs first;
  the service does not silently reproject or resample them.
- No server response or malformed JSON: ensure the launcher is a stdio
  process and that startup logging is not written to stdout. The MCP server
  itself keeps stdout reserved for the protocol.

## Current rollout status

The MCP catalog and transport contract are covered by
`packages/ntl_toolkit/tests/test_mcp_gis_core.py`. The local legacy LangChain
adapters are covered by `packages/ntl_toolkit/tests/test_langchain_parity.py`:
the existing public tool names, input schemas, thread-workspace resolution and
chat-oriented outputs are retained while local numerical work delegates to the
shared core. Two intentional safety contracts differ from the historical local
implementation: local composites always require aligned CRS/dimensions/affine
grids, and anomaly detection requires at least two images (one baseline plus a
target). GEE and network-download tools are deliberately outside this local
GIS MCP cutover.
