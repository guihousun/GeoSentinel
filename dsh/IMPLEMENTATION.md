# GeoSentinel DSH implementation

## Resource hardening (2026-09-08)

- Persisted research admission with eligible FIFO dispatch: two active chats per user, ten globally by default. Docker uses a separate two/ten queue. Operator configuration can reduce limits without exposing runtime controls to ordinary users.
- SQLite-backed waiting requests, prompt-rate windows, job states and usage counters; single-host process ownership prevents competing dispatchers. Undispatched research survives restart; interrupted execution is not replayed. Approval revision and ownership are rechecked on dispatch.
- Project uploads and monitor imports share serialized quota checks. Low-disk guards protect uploads, reports and Docker work; offline cleanup previews retained soft-deleted workspaces and requires explicit `--apply`.
- 46 regression tests passed, including the 30-minute Docker execution limit and configurable 3 GiB default / 4 GiB memory override. Real Docker isolation verified two same-user jobs plus a waiting third, cgroup memory enforcement, cancellation and output limits. Recovery smoke verified scope-filtered orphan cleanup without deleting another platform's container.
- Isolated native-web acceptance verified two active question flows and a queued third; stopping one admitted the waiting request. A forced service restart interrupted two active flows and completed the previously undispatched question. Desktop queue and usage panels checked at 1366x768 / 1440x900; no post-reload console errors. This is not a ten-container load test.
- Operational details and limitations: [Resource management](RESOURCE-MANAGEMENT.md).

## Agreed scope

- One managed product with workbench, platform-management and research plugins.
- Reviewed dependencies, pinned versions, isolated forks only when needed.
- Invitation-only accounts; multiple private projects per user; multiple chats per project.
- No migration of old chat history. No deletion of legacy data.
- AgentTeams fork is the target sole scheduler. Python graph remains a migration reference.
- Fixed supervisor, data assistant, analyst and event assistant; specialists cannot delegate.
- Simple tasks execute directly; multi-step research requests one meaningful plan approval.
- Server-owned project paths, task execution paths, tool permissions and resource limits.
- Fixed Docker GIS environment; no user-controlled dependency installation or host shell.
- Platform-managed GEE access initially, with a future user-credential binding boundary.
- Streaming progress, cancellation, persisted outputs and GEE acquisition-to-analysis acceptance.

## Source baselines

- GeoSentinel: a97780b (2026-09-08), branch codex/geosentinel-dsh.
- NTL reference: codex/hierarchical-multiagent-experiments (working tree includes local edits).
- AgentTeams: v0.1.15, commit da2e2e49242c6ecd7e801a74dba0c8268a0a2f81.
- DSH: 0.1.2-alpha.2 (required baseline for AgentTeams v0.1.15).

## Completion gates

- [x] Locked host and fork build; dedicated DSH home; no personal-profile mutation.
- [x] Fixed-role server enforcement and restricted member tool surfaces.
- [x] Invitation login and account administration.
- [x] Project/chat/file ownership enforced on HTTP and streaming paths.
- [x] Plan approval and progress in the product UI.
- [x] Cancellation terminates active descendants and Docker work.
- [x] Restricted Docker execution and durable artifact delivery.
- [x] Real GEE retrieval and downstream analysis acceptance.
- [x] Two-user concurrent isolation and neighboring workflow regression.

No gate is complete based solely on implementation intent or mocked tests.

## Executed acceptance (2026-09-08, local trial)

| Area | Evidence |
| --- | --- |
| Dependency graph | `check-env.mjs`: 215 resolved DSH packages, all `0.1.2-alpha.2`; fork builds successfully |
| Deterministic checks | 11 product tests; managed-role integration test; complete upstream lifecycle verification passed |
| GEE Image | SRTMGL1_003, Yangon's small AOI, requested 500 m: real 12x12 GeoTIFF; 144 valid pixels; mean 5.6805556, range 3-8 |
| GEE ImageCollection | VIIRS VCMCFG, avg_rad, January 2022, same AOI: real 12x12 GeoTIFF; 144 valid pixels; mean about 1.5093, range about 0.425-9.075 |
| Role dispatch | Data assistant acquired the raster; analyst inspected it and delivered a report; supervisor checked and closed the team |
| Artifact delivery | Authenticated HTTP download returned actual TIFF bytes and matching size; unauthenticated download rejected |
| Event role | Event assistant read a clearly labelled synthetic JSON fixture, retained uncertainty and fixture status, and wrote a source-linked Markdown report |
| Approval UI | Clicked the real plan approval button in the browser; stale revisions and model self-approval rejected |
| Multiuser | Two separate invited users ran real model chats concurrently with token streams; no other user's identifiers, history, streams or project files were accessible |
| Docker boundary | Concurrent private inputs; non-root; no analysis network, GEE credentials or Docker socket; bounded outputs; stdout returned |
| Cancellation | Real AgentTeams analyst + Docker sleep task cancelled through the authenticated API in approximately 515 ms; persisted job cancelled and team halted |
| Persistence | Complete parent and child history retrieved after service restart, including an archived team's reports |
| Desktop UI | Login and invitation form fit 1366x768; workbench verified at 1366x768 and 1440x900; composer remains visible, 16 px message text, rendered Markdown tables, no page overflow or browser errors/warnings |

Machine-local proof files remain ignored under `.runtime/`: `research-srtm-acceptance.json`,
`research-viirs-acceptance.json`, `event-acceptance.json`, `multiuser-acceptance.json`,
`isolation-acceptance.json`, and `cancellation-acceptance.json`. They are test evidence,
not production seed data. Test accounts and histories are not committed or migrated.

The first SRTM trial exposed a missing member claim permission and premature supervisor
takeover; it was not accepted as success. The corrected second trial passed. Later
schema restrictions removed administrator-only member parameters rather than relying
on prompt text alone. Native DSH report-tool setup was disabled because its scope is
incompatible with this fixed member allowlist; AgentTeams owns completion reporting.

## Remaining product scope (not claimed complete)

### DSH latest-channel upgrade, 2026-09-08

- Upgraded the product and independent AgentTeams fork to `0.1.2-rc.1`, the npm `latest` tag at verification time. This remains an RC; `0.1.3-alpha.2` was not selected.
- All 214 resolved product DSH packages are pinned to the same release. The fork now carries the same override policy and a reproducible lockfile in its exported patch.
- Adapted member initialization to synchronous `agent/created` composition and `Session.ownEvents()`. Host-authored FIFO deliveries use the exported `dsh-subagent/internal` queue adapter, preserving plugin provenance rather than silently changing delivery into model-authored steering. This internal integration is version-pinned and must be retested on future upgrades.
- Passed 21 product tests, two managed-fork tests (including retired-member queue rejection), fork typecheck/build, environment audit and UTF-8/syntax/diff checks.
- An isolated 8511 service with a separate account database passed actual model greeting, staged team approval, event-assistant activation and `geo_list_files` execution. Receipt: ignored `.runtime/upgrade-acceptance.json`. The temporary service was stopped afterwards.
- After switching 8510, an existing QA account and its historical projects/chats were readable. Account/invitation database backup and pre-upgrade session copies are in ignored `.runtime/upgrade-backup/`.
- This upgrade does not claim a fresh GEE-download benchmark or completion of the native frontend. The native UI draft stays disabled by default; the existing workbench remains the product entry.

### Managed native dark theme, 2026-09-08

- Added the exact `dsh-dream-skin@8.30.1` dependency. `plugins/workbench/skin-theme.mjs` reads only its static exported `SKINS` data and rejects non-static expressions; installed upstream source is unchanged.
- The managed Midnight variant registers through the native DSH theme service. Secondary/tertiary text and essential control borders have stronger contrast. Authentication, project navigation, chat, Better Sidebar, monitoring and artifact views use semantic theme tokens.
- Upstream host persistence, wallpaper/URL/theme-pack imports, settings UI and global DOM observers are not activated. This is a restricted theme integration, not the complete Dream Skin customization plugin.
- Passed 26 tests, including primary/secondary/tertiary/link contrast against both main surfaces. Native login and chat/monitor screens were visually checked at 1366x768 and 1440x900; no page overflow, and the composer remains in view. Native service remains an isolated 8511 preview; 8510 was not switched or restarted.

### Native UI acceptance and repairs, 2026-09-08

Tested the isolated 8511 native UI using separate administrator and ordinary-user
accounts. The existing `simon` account, invitation policy and 8510 service were not
changed by this acceptance run.

| Area | Actual browser acceptance |
| --- | --- |
| Accounts | Invitation registration, login, logout, password change and re-login; administrator invite generation and disable/re-enable of the QA ordinary user |
| Ownership | Ordinary account has no management entry; admin API returned 403, another user's history 404, host-file API 403 |
| Projects/chats | Create, select, rename, delete with cancel/confirm, refresh recovery, left-sidebar collapse/expand |
| Research | Real model reply; staged two-member plan; user approval; read a synthetic input; write and download a 1,571-byte report; stop generation |
| Files/monitor | Upload and duplicate-file error, inputs/output list, report download; monitor tab, world extent and zoom, event import into the draft, Better Sidebar tabs and bottom-panel toggle |
| Desktop | Login/registration, admin panel and chat/monitor inspected at common desktop sizes; 1366x768 and 1440x900 workbench has no page overflow and a visible composer; modal Tab focus remains trapped, Escape closes the panel |

Repairs: missing project-picker slot; file `name`/`path` mismatch; project/chat
selection mismatch; remembered-session load race; stale native session scope after
re-login; modal stacking/focus; zero-width native details column exposing invisible
controls; unsupported fork action silently swallowing its error; tool business
failure incorrectly shown as success. Theme, ownership and client-state regression
tests now total **28 passing tests**. Negative HTTP tests intentionally produced
401/403/404/409 responses; these are not runtime exceptions. Restarting 8511 applied
the history-projection fix and the monitor-import/download flow was rechecked.

The native historical-fork operation remains unavailable and now gives an explicit
message; it does not silently create a new conversation or pretend to copy history.
Terminal/command capabilities remain intentionally disabled. This run did not
repeat GEE/Docker scientific benchmarks or exhaustively test every Better Sidebar
floating-window gesture. Synthetic inputs, downloaded report, screenshots and test
accounts stay in ignored runtime storage; the new QA accounts were disabled after
acceptance. No commit or push was performed.

### Remaining scope

Native reuse policy: prefer public DSH components and slots before custom UI.
The earlier component-only plan adapter has been replaced by the following native
interaction integration; personal-host endpoints remain disabled.

### Native question lifecycle integration, 2026-09-08

- Reuses the unmodified upstream `dsh-tool-ask-user`, `UserQuestionService` waterfall,
  `dsh-client-ui-user-questions` plugin, `PendingQuestion`, question draft store,
  native question composer and native plan-review decision card.
- `plugins/platform/questions.mjs` supplies an authenticated, same-origin transport
  between the native host waterfall and the native client listener. It validates
  account/chat ownership, opaque request IDs, answer options, cancellation and
  revision-bound plan approval. Native Remote host-control endpoints are not enabled.
- Only a live root/supervisor may ask the user. Staged plans enter the native
  `plan-review` intent after the supervisor has finished composing its reply.
  Discuss/decline leaves the team unapproved; confirmation reaches the existing
  guarded AgentTeams approval function. The model cannot create approval metadata.
- Native interaction behavior is retained: an answer/decision card temporarily
  replaces the composer; discussion restores it. The custom plan summary remains
  only for progress and reopening review, with no duplicate approval button.
- Isolated 8512 real-model browser acceptance passed selected-option delivery
  (province scope), free-text delivery (Sichuan), plan discussion, decline, reopening,
  approval and a data-assistant `geo_list_files` task completing only after approval.
  No real user research inputs were changed by these tests.
- Pending generic requests are process-local; refresh reconnects, restart invalidates
  old IDs. Review requests can be rebuilt from durable staged plans. This does not
  claim durable cross-process continuation of an in-flight model question.
- 33 tests passed, including duplicate submission, owner isolation, stale revisions,
  cancelled request IDs, native skip semantics and free text. After checking the
  existing user's captain/members were idle, 8511 was restarted with the integration;
  existing administrator login, question API and health checks passed. The isolated
  8512 acceptance service was stopped. No commit or push was performed.

### Native Todo projection, 2026-09-08

- Reuses the unmodified DSH Conversation TodoDock/TodoPanel and its native
  `todos` session projection, including collapse/expand, counts and status glyphs.
  AgentTeams remains the sole task-state authority; no second model-maintained
  `write_todos` list or scheduler was enabled.
- Removed the duplicate custom task list. Only the native review reopening action
  remains custom; approval still uses the native question/review lifecycle.
- The native three-state schema cannot directly represent failures/cancellation:
  these keep explicit Chinese labels and count as unfinished, never completed.
  A halted in-progress item is shown as stopped, not spinning indefinitely.
- Extended client regression coverage for completion, progress, failure, cancellation,
  stopped teams, per-chat isolation and clearing a removed team. All 33 tests passed.
  Actual existing-task browser inspection at 1366x768 and 1440x900 confirmed the
  Todo panel stays above the composer; its collapse/expand works. Frontend-only
  change, no service restart, account mutation or real task execution.

### Legacy workbench removal, 2026-09-08

- Removed the native sidebar's legacy-return link and all five files under
  `plugins/workbench/public/`. Retired their page/script/style and exclusive vendor
  routes; removed unused direct `marked`, `dompurify` and `lucide` dependencies.
- `/` and `/geo/` redirect to `/geo/native/`; native UI and its protected sidebar
  adapter are always active. The former `GEO_NATIVE_UI_PREVIEW` switch is retired.
  Shared auth/project/chat/question/file/monitor APIs and Leaflet resources remain.
- 34 tests passed, including redirects, removed-file/route assertions and shared
  resources. After an idle check, 8511 restarted; root redirect, login, absence of
  the old sidebar link, old-assets 404 and health/monitor 200 were verified live.

### Remaining platform scope

- Full NTL capability migration and parity benchmark beyond these verified deterministic tools.
- Map topics, country profiles, enterprise knowledge integration and polished final branding. Shared public monitoring was added after this initial acceptance; see MONITOR.md.
- Per-user GEE credential binding UI; this version uses administrator GEE authorization only.
- Large-scale storage, filesystem hard quotas, queued batch jobs, distributed deployment and production hardening.
- The container image pins its base digest and direct Python requirements; a complete transitive Python lock remains a deployment-hardening task.

This is a functional controlled pilot for the agreed first three parts, not an unrestricted
public coding service or a claim that Docker eliminates every hostile-code risk.
