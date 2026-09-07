---
name: code-execution-validation
description: Apply the Analyst's internal Geo-CodeCoT lifecycle to v2 scripts, execution, artifacts, validation, and one bounded technical repair.
---

# Code Execution and Validation

1. Inspect accepted contracts and inputs.
2. Decompose the method into explicit deterministic steps.
3. Save only `ntl.script.contract.v2` code under `/outputs/` and perform static preflight. The script must contain a Python-literal assignment named exactly `NTL_SCRIPT_CONTRACT`; comments or `schema_version` do not satisfy the validator. Filesystem tools use virtual `/inputs/` and `/outputs/` paths, but the executed Python process starts at the task workspace: inside `NTL_SCRIPT_CONTRACT` and Python code use relative `inputs/...` and `outputs/...` paths.
4. Execute the exact saved script once and validate the declared outputs, manifest, ranges, and failure gates in one batched pass.
5. Allow at most one recorded non-semantic repair for a concrete path, format, serialization, or syntax defect. If the repaired execution fails, stop and report failure; never create or execute a third implementation.
6. After the final mutation, re-open and checksum the final script and each referenced output once before saving `AnalysisPackage`. Do not repeat checks on unchanged files or add unrequested diagnostic/alternate-method artifacts. Package persistence is the immutability boundary: never overwrite, edit, re-run, or validate a referenced file afterward; return the saved handle and stop.

Execute only against inputs already prepared by bounded Data Searcher tools. The child process receives no GEE, LLM, tracing, proxy, or user credential environment; never inspect environment variables, hidden runner files, telemetry, or host paths. This is a credential boundary, not an OS-level filesystem or network sandbox.

Changing product, AOI, date, QA, baseline, threshold, method, or objective requires NTL_Engineer approval. A zero exit code without valid artifacts is failure.

## Required literal contract

Use this exact top-level shape before imports and executable code; replace the placeholder content, but do not rename keys:

```python
NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "One non-empty sentence describing the frozen objective.",
    "input_manifest": [{"path": "inputs/input.ext", "role": "declared input"}],
    "method_steps": ["deterministic step 1"],
    "parameters": {},
    "output_manifest": [{"path": "outputs/result.ext", "required": True}],
    "validation_checks": ["declared output exists and is non-empty"],
    "failure_gates": ["fail if the declared output is missing or invalid"],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 1800,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": [],
    },
}
```

The schema key is `schema`, not `schema_version`. All nine displayed top-level keys are mandatory. `NTL_SCRIPT_CONTRACT` must be an `ast.literal_eval`-compatible dictionary: no function calls, variables, comprehensions, JSON-only booleans, or dynamically computed values.

When no repair occurred, keep `repair_history: []`. When one allowed repair is
needed, replace it with exactly one object such as
`{"reason": "workspace paths must be relative", "before": "/inputs and /outputs", "after": "inputs and outputs"}`.
Do not use a string or an object containing only `note`.

