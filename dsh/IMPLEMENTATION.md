# GeoSentinel DSH implementation

## Native read-only child sessions (2026-09-08)

- Activated the pinned native subagent catalog and conversation renderer. The managed session adapter supplies authenticated catalogs, addresses, read-only event snapshots and root-linked refresh signals.
- Both platform membership and durable native parentage are checked. Child endpoints are GET-only; account access is checked again after asynchronous reads. Native composer slot replacement and client action guards prevent direct child prompting, cancellation, renaming and approval.
- Better Sidebar receives only owned virtual workspace metadata and capability-empty channels for child views. Its general subagent, sidechat and host terminal features remain disabled.
- Existing stopped research histories were inspected without replay. Desktop checks cover 1366x768 and 1440x900, native catalog switching and return-to-parent navigation. See [scope and usage](SUBAGENT-ARCHITECTURE.md).

## GEE readiness and native-subagent study (2026-09-08)

- Diagnosed a live launch using the legacy environment without `GEO_GEE_CREDENTIALS`. Generated a separate ignored product configuration from allowed keys, retained the existing DSH home and monitor directory, and pointed the fixed acquisition worker at the administrator's existing Earth Engine credential file. No credentials were copied to user inputs or Git.
- Corrected managed CLI bootstrap: launch DSH from its trusted profile directory, with administrator bootstrap variables inherited from the wrapper, instead of having DSH re-read them as an untrusted project `.env` layer.
- Corrected the image-side 600-second alarm left behind by the previous host-side 30-minute change. Rebuilt the image and verified its default 1800-second alarm, explicit host override and invalid-value rejection.
- Real acquisition test: Shanghai small AOI `[121.45,31.15,121.5,31.2]`, 500 m, SRTM and VIIRS ANNUAL_V21 `average_masked` for 2020; both yielded readable 12x12 rasters with 144 valid pixels, and the next isolated container inspected the outputs. This is not acceptance of the full 16-district workflow.
- After explicit user approval, stopped the old research, backed up the DSH home, restarted port 8511 and verified login, retained history and stopped state. No automatic research replay. Environment checks and 48 regression tests passed.
- Native child-agent study is documented in [SUBAGENT-ARCHITECTURE.md](SUBAGENT-ARCHITECTURE.md). AgentTeams remains installed; the subsequent read-only integration is described above.

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

## 0.1.5 core migration: parity index and verification evidence (2026-09-11)

### How the comparison is made

- **Module level**: the ids registered with the client module loader on each side (taken from the
  loader's own registrations, not from package-name-looking substrings, which would also count CSS
  class names). The running 0.1.2 product registers 17 client modules; the 0.1.5 candidate registers
  26 and boots 25. The only module DROPPED is `dsh-better-sidebar`; the 10 added modules are all
  0.1.5 native surfaces (sidebar family, upload, attachment, approval, deliverables, resources, the
  file resource provider), so the capability set is a superset rather than a reduction.
- **Artifact level**: a release id is `sha(JSON.stringify(fileHashes)).slice(0, 16)` over the frozen
  files, so the same id means the same bytes. `.runtime/candidate-validity.mjs <sourceRoot>
  <candidate>/manifest.json` answers "does the current source still freeze into the candidate that
  was validated and acceptance-tested" — `publish` performs the same comparison itself before it
  switches (release/manager.mjs).

### User-facing surfaces

| Surface | 0.1.2 product | 0.1.5 candidate | Evidence |
| --- | --- | --- | --- |
| Login, invitation, account panel | yes | yes | 133 product tests; the preview instance logs in as an ordinary user and the account panel shows usage |
| Project / chat list and entries | the product's own list | the product's own list registered into the native `sidebar.workspaces` / `sidebar.footer.action` | browser acceptance: the sidebar lists projects and their chats, clicking one opens it; management entry points sit behind the row's gear |
| Conversation, streaming answer, transcript | yes | yes | every step of the one-command acceptance runs a real chat; answers cross-checked against `native-history` |
| Attachment upload | the product's own dock | native upload dock over the product's `/api/upload/native` bridge | acceptance: dock shows `upload-sample.md / MD 63B`, the only upload request is the bridge call, the workspace lists the file |
| File panel and document preview | the product's own panel (refresh on navigation) | native right sidebar + the product's explorer namespace + a LIVE change feed | acceptance: groups, expand, click-to-render Markdown; PDF preview; **an open preview updated by itself about 4 s after the agent rewrote the file**, with no click |
| File resources (`dsh-resource://`) | no | yes | the document preview resolves through the resource provider, which is bundled and booted on 0.1.5 |
| Staged plan and confirmation | AgentTeams plan in the product UI | native plan mode (`exit_plan_mode`) plus the native question panel | acceptance: plan submitted, three-part question form asked, answered through the product's own contract, turn continued about 5 s later |
| Delegation and role boundaries | AgentTeams, four fixed roles | native subagents plus the product's three role tools on the agent-preset plane | measured: supervisor 57 tools with no `subagent`/shell/workflow; the data and analysis specialists' FIRST requests carry 21 and 29 tools, item-by-item equal to `product.json.roleTools` |
| Real retrieval and analysis (the scientific core) | real GEE retrieval + Docker analysis on the 0.1.2 line | same, re-run end-to-end on the current candidate | ordinary-user run through the product's own routes: `geo_download_gee` fired after ~5 s and produced `分析结果/空间数据/imagery.tif` (8,952 bytes, job record `20260911-025409-gee-4c008a`); `geo_execute_python` fired after ~30 s and produced `ntl_summary.py` + `ntl-summary.json` plus its own job record; the summary was read back through the product's file route: `{"dataset":"NOAA/VIIRS/001/VNP46A2","path":"outputs/20260911-025409-gee-4c008a/imagery.tif","pixels":2070,"mean":7.420264}`. Nothing mocked: the deployed GEE entry point and the deployed analysis image ran |
| Monitoring brief and spatial data panels | yes | yes | panel rendering plus the route behind each one re-verified on the current candidate: all three panels rendered, `/monitor/events` answered 200 twice, 0 console errors, 0 failed requests. Their snapshots are EMPTY there because a preview instance keeps monitoring collection off by design, and the panels say so ("0/0 条线索 · 数据待更新", "没有可预览的空间数据文件") instead of inventing content; a data-bearing snapshot was verified on an earlier candidate in this line |
| Model and theme settings | native settings | native settings (with `ui-agent-preset` closed) | verified in the administrator development instance; not separately re-verified for ordinary users |

### Deliberately not reused, with reasons

- **`dsh-client-ui-workspace`** (the native session list): its activation waits on `workspaces` and
  `remote.directoryPicker`, and the picker lives on the host plane the product keeps closed; booting
  it leaves the loader entry pending, which makes the native loader report the WHOLE client bundle as
  failed (blank page). It was never bundled on either line — it does not appear in the frozen 0.1.2
  release either, and the module-level comparison above finds only `dsh-better-sidebar` dropped — so
  the native session-list grouping interactions are a boundary, not a parity gap.
- **`dsh-client-ui-open-in-app`**: polls the closed host API plane and answers 401 for an ordinary
  user, so the button would be inert while every page load logged a failed request.
- **`dsh-client-ui-plan`**: needs `remote.commands` from the closed plane; the plan itself is carried
  by native plan mode plus the question panel.
- **Host shell, `workflow`, `ralph`**: product boundaries. Model-authored code runs only in the
  bounded, network-disabled analysis container, and orchestration belongs to the fixed roles.
- **Directory picker**: a host-plane capability that an ordinary user never had; session archiving is
  project-level by design (`archiveSession` refuses to archive a single session).

### Re-verifying in one command

1. `node dsh/tools/release.mjs preview --id <candidate> --hold 1800` in the validation home.
2. `python .runtime/acceptance-suite.py 8513 <login-token>` — runs the six browser/API checks and
   prints a PASS/FAIL verdict table (all six passed on `c5f518a3c6f651e6`).
3. `node .runtime/candidate-validity.mjs <sourceRoot> <candidate>/manifest.json` — the frozen scope
   still matches the source (it did, 187/187 files, before and after this document was written).

### Still open

- Formal publication. The technical path is pre-flighted: freezing the main tree produced the SAME
  candidate id (hence the same bytes) as the one acceptance-tested here, offline install with
  `--frozen-lockfile` succeeded, and the Docker image built.
- The main tree's dependency upgrade (0.1.2 → 0.1.5). It replaces the runtime the current
  administrator session runs on, so it is executed together with publication, on explicit approval.
- One observed ordering deviation, left to the administrator: in the staged-plan acceptance the
  supervisor called search/download tools BEFORE presenting the plan and asking for confirmation,
  while its persona asks for the plan first. No boundary was crossed (downloads use the normal
  entry points and quotas); it is recorded rather than silently patched.
