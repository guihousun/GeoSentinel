# GeoSentinel DSH Workspace

This directory is the opt-in DSH product implementation. Its entrypoint is
[`scripts/start.mjs`](scripts/start.mjs); read [`README.md`](README.md) and
[`IMPLEMENTATION.md`](IMPLEMENTATION.md) before changing runtime boundaries.

- Keep workbench, platform and research ownership separate. AgentTeams is the sole scheduler here.
- Do not edit the user's personal DSH profile, legacy Conda environments or old Python graph for this runtime.
- Fixed supervisor and three specialists; no peer delegation, arbitrary model selection or user-controlled host tools.
- Authenticated user approval, not model text, starts a staged plan. Revised scope needs renewed approval.
- Accounts own projects; projects own chats and inputs. Every HTTP, stream and artifact lookup rechecks ownership.
- Admission is a persistent automatic queue (two active research chats and two Docker jobs per user, ten globally for each by default). Read `RESOURCE-MANAGEMENT.md` before changing limits, recovery, accounting or cleanup. Never replay interrupted execution automatically or silently purge live data.
- Model-authored Python runs only in the bounded, network-disabled Docker worker. Networked GEE uses a fixed entrypoint and separate credential mount.
- Credentials, local databases, jobs, browser state and QA accounts stay under ignored configuration/runtime paths.
- Do not describe Docker resource limits as proof against all hostile code. Do not claim legacy NTL capability parity without separate acceptance.
- Keep dependency pins and the independent fork patch synchronized. Never modify installed third-party source directly.
- Reuse native DSH UI slots, components and interaction contracts before building replacements. Keep GeoSentinel-specific adapters narrow. Reuse never bypasses account/project ownership, revision-bound approval or sandbox policy; do not enable upstream personal-host endpoints to make a component work. Prefer public exports; document unsupported native features rather than silently emulating them.
- After changes: `pnpm test`, relevant real smoke tests, syntax checks and desktop browser verification. Do not run acceptance scripts on production user data.
- Preserve sources and uncertainty. Fixtures must remain visibly labelled; repeated computation is not independent scientific validation.
