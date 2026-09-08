# GeoSentinel Documentation

This directory contains public operator and integration documentation. Start with the root [README](../README.md) for installation and the project overview.

## Current Platform

- [DSH installation and operation](../dsh/README.md): current recommended product entry, native UI, managed plugins, Docker and account setup.
- [Migration to DSH](migration-to-dsh.md): old Python to DSH, existing DSH upgrades, backups, path constraints, public rollout and rollback.
- [DSH implementation and acceptance](../dsh/IMPLEMENTATION.md): verified capabilities and explicit limitations.

The following Python platform documents describe the legacy runtime and remain useful for capability migration:

- [Geoenvironmental intelligence platform overview](geoenvironmental-platform.md): current frontend/backend ownership, runtime boundaries, monitor behavior, startup and verification commands.
- [Geoenvironmental platform UI standards](geoenvironmental-ui-standards.md): task-workbench structure, evidence-first information hierarchy, visual constraints and acceptance checklist.

## Deployment

- [Windows Server operations](deployment/windows-server.md): update, launch, Nginx, HTTPS, PostgreSQL, backup, and troubleshooting.
- [Geoenvironmental public web service](deployment/geoenvironmental-web-service.md): FastAPI public workbench, 花生壳（Oray）HTTPS mapping, account session settings, and operations.

## Product Planning

- [`planning/geoenvironmental-intelligence-platform-phase1.md`](planning/geoenvironmental-intelligence-platform-phase1.md): phase-1 scope, architecture, schedule, pricing, acceptance criteria, dependencies, and exclusions for the geoenvironmental intelligence platform.
- [`planning/geoenvironmental-intelligence-platform-phase1-summary.md`](planning/geoenvironmental-intelligence-platform-phase1-summary.md): concise, result-oriented phase-1 proposal for external review.

## MCP Services

- [`ntl-gis-core`](mcp/ntl-gis-core.md): local deterministic GIS and nighttime-light tools.
- [`ntl-download`](mcp/ntl-download.md): GEE export and official VNP46A1/VNP46A2 Earthdata download tools.

## Development Records

Historical implementation plans are local engineering records and are not part of the public runtime distribution. Current behavior is documented by the README, MCP guides, tests, source code, and `.ntl-gpt/skills/` contracts.

## Documentation Rules

- Keep commands aligned with `environment.yml`, `check_env.py`, `Streamlit.py`, and `run_web.py` when the public web service is in scope.
- Never include API keys, tokens, passwords, private data paths, or user workspace contents.
- Prefer concise operator guidance over internal implementation transcripts.
- Update the relevant guide whenever a public entrypoint or environment variable changes.
