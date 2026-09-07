---
name: workspace-and-artifact-contract
description: Enforce the isolated GeoSentinel workspace, artifact paths, manifests, and checksum rules shared by all four roles.
---

# Workspace and Artifact Contract

- Read assigned inputs from `/inputs/` or accepted package paths; write new artifacts only under `/outputs/`.
- Distinguish filesystem-tool paths from typed-contract paths: filesystem calls use virtual `/inputs/...` and `/outputs/...`, while `ArtifactRecord.path`, `artifact_manifest_path`, and other typed contract artifact fields must use workspace-relative `inputs/...` or `outputs/...` with no leading slash.
- Treat `/shared/` as read-only. Never write outside the current run workspace or invent an artifact path.
- For a local input/output artifact, declare only the workspace-relative path, its semantic `role`, and `media_type` when known. On typed save, the system resolves that exact workspace file and injects its actual SHA-256 and byte count.
- Never calculate, guess, copy, null-fill, or placeholder-fill local `sha256` or `bytes`. Do not block or fail merely because no checksum utility is available to the model; verify content and semantics with available inspection tools and let the save layer bind identity.
- Preserve failed scripts, logs, checks, and partial outputs for audit. A submitted job or zero exit code is not artifact validation.
- Return compact references in messages; keep large tables, logs, and binaries in files.

