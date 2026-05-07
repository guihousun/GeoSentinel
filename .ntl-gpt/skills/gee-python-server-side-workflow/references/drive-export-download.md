# Drive Export Download Into Workspace

Use this reference when Earth Engine exports to Google Drive and the final artifact must be copied into the 地缘环境智能计算平台 thread workspace.

## Path Rule

Always download into the current thread workspace `outputs/`. In project code, resolve local output paths with:

```python
from storage_manager import resolve_output_path

out_path = resolve_output_path("result.csv")
```

Do not write to absolute desktop paths, global temp directories, or another thread workspace.

## Export Setup

Use deterministic export names so the Drive lookup is unambiguous:

```python
file_prefix = f"ntl_{task_id}_{output_stem}"
task = ee.batch.Export.table.toDrive(
    collection=feature_collection,
    description=file_prefix,
    folder=drive_folder,
    fileNamePrefix=file_prefix,
    fileFormat="CSV",
)
task.start()
```

For images, include `scale`, `region`, `crs` when required by the script contract, and `maxPixels` for large but intentional exports.

## Download Flow

1. Wait for the Earth Engine task to reach `COMPLETED`.
2. Query Drive by `name`, folder, and modified time where possible.
3. If multiple files match, choose the newest file with the exact prefix and expected extension.
4. Download to `resolve_output_path(...)`.
5. Validate output size and parseability.

For CSV/table outputs, require:

- file exists
- file size is greater than zero
- expected columns are present
- row count matches the expected AOI/period/features when known

For raster outputs, require:

- file exists
- file size is greater than zero
- raster can be opened by `rasterio` when available
- CRS, transform, and dimensions are non-empty

## Failure Handling

If Drive lookup fails after a completed task:

- report the export description, file prefix, target Drive folder, and task id/status;
- do not claim the output is available locally;
- ask for Drive access/folder confirmation only after checking the exact prefix.

If the task failed before Drive output exists, use `export-task-lifecycle.md` to classify the Earth Engine failure instead of debugging Drive.

## Workspace Boundary

The Drive artifact is an intermediate delivery mechanism. The user-facing output is the file copied into `outputs/`, not the Drive URL or Drive folder name.


