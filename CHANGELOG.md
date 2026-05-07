# Changelog

## 2026-04-28 - Multi-user Runtime Update

### Added
- Added a local admin console for user management, GEE profile inspection, workspace cleanup, thread deletion, and account disable/enable actions.
- Added per-thread and per-user workspace quota controls.
- Added China official statistics and country GDP tools for more reliable socioeconomic retrieval.
- Added a CJK geospatial visualization skill for readable Chinese map and chart output.
- Added official VIIRS availability scanning and GEE baseline helpers.

### Changed
- Updated Streamlit UI rendering for code blocks, images, GIF previews, live reasoning refresh, and chat input fallback.
- Persisted analysis logs in turn summaries so recent reasoning traces can be restored with thread history.
- Strengthened agent routing and GEE guardrails for China 34 province-level NTL statistics tasks.
- Improved generated-code execution checks so empty geospatial outputs are treated as failures.
- Updated local RAG assets and official NTL source metadata.
- Synchronized `README.md`, `.env.example`, `check_env.py`, and `environment.yml` with the new runtime controls and dependencies.

### Security
- Removed a hardcoded OpenAI API key from the FAISS knowledge-base manager and now require `OPENAI_API_KEY` from the environment.

### Not Included
- Local tests, Playwright screenshots, workflow learning logs, `.learnings` notes, and interview/process documents were intentionally left out of the published commit.
