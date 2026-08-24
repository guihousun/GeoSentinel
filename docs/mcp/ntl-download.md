# `ntl-download` MCP

`ntl-download` is a local, single-user stdio MCP service for direct Google
Earth Engine raster downloads, tracked Earth Engine batch exports, and official
NASA Earthdata VNP46A1/VNP46A2 daily mosaics.
It is intentionally separate from `ntl-gis-core`: the latter remains a
network-independent local GIS and nighttime-light analysis service.

Long direct downloads keep the MCP call open and emit progress notifications.
GEE batch export submission returns immediately and writes a sanitized local
manifest for later status inspection or cancellation. There is no web service,
database, local task queue, or remote account system.

## Install and configure

Install the package once in `NTL-GPT-Stable`:

```powershell
C:\Users\27334\miniconda3\Scripts\conda.exe run -n NTL-GPT-Stable python -m pip install -e D:\NTL-GPT-main\packages\ntl_toolkit
```

Codex configuration:

```toml
[mcp_servers.ntl-download]
command = "C:/Users/27334/miniconda3/Scripts/conda.exe"
args = ["run", "-n", "NTL-GPT-Stable", "python", "D:/NTL-GPT-main/mcp_servers/download_server.py"]
env = { NTL_MCP_WORKDIR = "D:/NTL-GPT-data", NTL_MCP_ENV_FILE = "D:/NTL-GPT-main/.env" }
```

For another checkout or Conda installation, replace the two paths. Keep
credentials in the local dotenv file or process environment, never in the MCP
configuration or tool arguments.

## Tools

| Tool | What it does |
| --- | --- |
| `validate_download_environment` | Checks local download dependencies, Earthdata token presence, and optional non-interactive GEE initialization. |
| `download_gee_raster` | Downloads a validated GEE Image or ImageCollection reduction with one or more bands, controlled QA/scaling/index presets, dates where applicable, WGS84 bounding box, scale, CRS, and output path. |
| `submit_gee_batch_export` | Submits a validated large raster export to Drive, Cloud Storage, or an Earth Engine asset and writes an `ntl.gee.export.v1` manifest. |
| `inspect_gee_batch_export` | Refreshes live task state, progress, destination, and exact Earth Engine failure details in the export manifest. |
| `cancel_gee_batch_export` | Requests cancellation of a non-terminal export represented by an export manifest. |
| `download_vnp46a2_official_h5_country` | Plans or runs the official VNP46A2 non-gap-filled HDF5 country workflow: boundaries, CMR, download, validation, mosaic, audit, and optional package. |
| `download_vnp46a1_official_h5` | Plans or runs official VNP46A1 HDF5 retrieval for exactly one country or WGS84 BBox, producing daily `DNB_At_Sensor_Radiance_500m` GeoTIFFs and optional `UTC_Time` companion GeoTIFFs. |
| `inspect_download_run` | Reads a VNP46A1 or VNP46A2 audit when available; during an active run, returns its persisted status, current phase, completed phases, and last update time. |

The service also publishes `ntl://download/capabilities` and the shared
`ntl://schemas/result-v1` resource.

## Credential and path rules

- GEE uses credentials already authorized in the local Earth Engine runtime.
  `ntl-download` never runs OAuth. Use EasyGEE's environment/auth tools if
  `validate_download_environment(initialize_gee=true)` reports a setup error.
- The official HDF5 route reads `EARTHDATA_TOKEN` by default. It reports only
  whether that variable is configured; token values are redacted from manifests
  and tool results.
- Relative paths resolve below `NTL_MCP_WORKDIR`; absolute Windows paths are
  accepted. Partially-qualified paths such as `D:output.tif` are rejected.
- Direct GEE output names are reserved with a numeric suffix instead of
  overwriting an existing GeoTIFF. GEE batch and VNP runs retain their manifests
  to support status recovery.

## Progress, recovery, and audit

The direct GEE tool reports initialization, imagery selection, export,
validation, and completion. Batch exports use `submit_gee_batch_export`, then
`inspect_gee_batch_export` with the same manifest until a terminal state. A
successful submission is not a completed export. The official HDF5 routes report each pipeline phase plus sanitized
script output and persist `*_runtime.json` state files. If the client does not
render progress notifications, call `inspect_download_run` on the same run
directory: before audit it reports `running`, the current phase, completed
phases, and last update time; after audit it returns the exact recovery targets.
An active or failed runtime state takes precedence over a stale audit from an
earlier attempt in the same directory.
For a long active call, a second local stdio client may inspect the same run
directory without creating a service, queue, or database.

For VNP46A2, `retry_download` and `not_processed` entries are returned as exact
`ISO3:YYYY-MM-DD` retry targets. `downloaded_without_mosaic` entries are
returned separately for a mosaic-only retry. `no_granules` is an availability
state, not a transport error. Do not declare a run complete while the audit
contains `downloaded_without_mosaic`, `retry_download`, `not_processed`, or
`other_manifest_status`.

Use this official HDF5 route only for explicit raw country-scale daily
non-gap-filled `DNB_BRDF_Corrected_NTL` rasters. For country statistics,
long-series summaries, and many-feature comparisons, use GEE server-side
reductions instead of downloading country rasters.

For VNP46A1, provide exactly one of `countries=["ISR"]` or
`bbox=[34.0,29.0,35.0,30.0]`. Its primary output is at-sensor radiance, not
VNP46A2 BRDF-corrected light. Set `include_utc_time=true` only when validating
UTC acquisition timing near a date boundary; it creates a separate decimal UTC
hours raster and must never be interpreted as radiance.

## Troubleshooting

- `GEE_NOT_INITIALIZED`: run EasyGEE environment/auth planning, complete the
  browser authorization locally if needed, then retry the MCP tool.
- `GEE_BATCH_SUBMIT_FAILED`: verify dataset metadata, destination permissions,
  AOI, bands, scale, and quota; preserve the failure manifest when one exists.
- `GEE_BATCH_STATUS_FAILED`: provide the original `ntl.gee.export.v1` manifest
  and the same Earth Engine project used for submission.
- `EARTHDATA_TOKEN_MISSING`: set or refresh the token in the dotenv file named
  by `NTL_MCP_ENV_FILE`, then restart the MCP client.
- `VNP46A2_AUDIT_INCOMPLETE`: call `inspect_download_run`, use its exact retry
  targets, run the required phase, then audit again.
- No server response or malformed protocol: start only through the stdio launcher;
  ordinary diagnostic output must not be written to stdout before MCP starts.
