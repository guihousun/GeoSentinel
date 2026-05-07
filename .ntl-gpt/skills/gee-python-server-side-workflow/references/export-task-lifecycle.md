# Earth Engine Export Task Lifecycle

Use this reference when a GEE Python script creates `Export.table` or `Export.image` tasks, or when a prior task must be inspected, cancelled, or diagnosed.

## Required Pattern

1. Create an export task with a deterministic `description`.
2. Call `task.start()`.
3. Poll or list task status until it reaches a terminal state.
4. If the task failed, return the task `error_message` and classify the failure.
5. If the task completed, verify the expected artifact exists before reporting success.

Keep task descriptions short and searchable:

```python
description = f"ntl_{task_id}_{output_stem}"[:100]
```

## Status Inspection

Prefer status checks that preserve the raw Earth Engine fields:

```python
status = task.status()
state = status.get("state")
error_message = status.get("error_message")
```

Terminal states:

- `COMPLETED`: proceed to artifact lookup/download.
- `FAILED`: report `error_message`; do not silently retry with a different dataset, region, or algorithm.
- `CANCELLED`: report cancellation.

Non-terminal states:

- `READY`
- `RUNNING`

## Listing Existing Tasks

Use task listing when the script needs to find duplicate, stale, or failed exports:

```python
for t in ee.batch.Task.list():
    s = t.status()
    print(s.get("description"), s.get("state"), s.get("error_message"))
```

For cleanup, filter by a project-specific description prefix. Do not cancel unrelated user tasks.

## Failure Classification

Escalate as environment/configuration:

- authentication failure
- `USER_PROJECT_DENIED`
- missing `serviceusage.serviceUsageConsumer`
- API disabled
- quota denial
- permission denied for an asset, folder, or project

Treat as script/data repair candidates:

- missing band
- empty image collection
- empty feature collection
- invalid geometry
- reducer output missing expected fields

Treat as bounded retry candidates:

- memory limit exceeded
- computation timed out
- aggregation too large

For bounded retries, first use `tileScale` with reductions. Only increase `scale` if the script contract allows coarser statistics and the output documents the changed scale.

## Success Gate

An export task is not a completed project output until both are true:

- Earth Engine task state is `COMPLETED`.
- The exported file or table has been found and copied into the current thread workspace `outputs/`.

For table exports, verify row count and expected columns. For image exports, verify file presence and, when practical, basic raster metadata.

