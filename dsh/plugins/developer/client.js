window.__ModuleLoader__.load({
  id: "@geosentinel/dsh-developer",
  factory(require) {
    const React = require("react");
    const { Button } = require("@deepseek-ai/dsh-client-ui-primitives");
    return { inject: ["slots"], apply(ctx) {
      const embedded = window.parent !== window;
      const notify = (type, data = {}) => window.parent.postMessage({ type, ...data }, location.origin);
      const productUrl = globalThis.__GEOSENTINEL_DEVELOPMENT__ ? "/geo/native/" : "http://127.0.0.1:8511/geo/native/";
      const research = () => embedded ? notify("geo:research") : location.assign(productUrl);
      const open = () => embedded ? notify("geo:releases") : location.assign(productUrl + "#releases");
      if (embedded) ctx.inject(["sessions", "remote", "remote.session", "remote.workspace", "uiWorkspace", "workspaces"], (inner) => {
        const source = window.__GEOSENTINEL_SOURCE__;
        function DirectoryExplorer(props) {
          const h = React.createElement, ref = React.useRef(null), request = React.useRef(0);
          const [listing, setListing] = React.useState(null), [address, setAddress] = React.useState(source);
          const [roots, setRoots] = React.useState([]), [error, setError] = React.useState("");
          const [loading, setLoading] = React.useState(false), [hidden, setHidden] = React.useState(false);
          const [folder, setFolder] = React.useState(null);
          async function navigate(path) {
            const version = ++request.current; setLoading(true); setError("");
            try { const result = await inner.uiWorkspace.listDirectory(path); if (version === request.current) { setListing(result); setAddress(result.path); } }
            catch (e) { if (version === request.current) setError(e.message); }
            finally { if (version === request.current) setLoading(false); }
          }
          React.useEffect(() => {
            if (!props.open) { ref.current?.close(); return; }
            ref.current?.showModal(); void navigate(source);
            let cancelled = false;
            fetch("/geo/api/admin/development/roots", { method: "POST" }).then(async (r) => { const result = await r.json(); if (!r.ok) throw new Error(result.error); if (!cancelled) setRoots(result.roots); }).catch((e) => { if (!cancelled) setError(e.message); });
            return () => { cancelled = true; request.current++; };
          }, [props.open]);
          const control = (label, action, disabled = false) => h(Button, { onClick: action, disabled: disabled || props.busy }, label);
          const parent = listing?.crumbs?.at(-2)?.path;
          return h("dialog", { ref, "aria-label": "选择服务器工作区", onCancel: props.onCancel, style: { width: "min(840px,90vw)", height: "min(560px,85vh)", padding: 24, background: "var(--dsw-alias-bg-layer-2)", color: "var(--dsw-alias-label-primary)", border: "1px solid var(--dsw-alias-border-l2)", borderRadius: 8 } },
            h("div", { style: { display: "flex", flexDirection: "column", height: "100%", gap: 16 } },
              h("strong", null, "选择服务器工作区"),
              h("form", { style: { display: "flex", gap: 8 }, onSubmit: (e) => { e.preventDefault(); void navigate(address); } },
                control("↑ 上一级", () => navigate(parent), !parent || loading),
                h("input", { "aria-label": "目录地址", value: address, onChange: (e) => setAddress(e.target.value), style: { flex: 1, minWidth: 0, padding: 8, color: "inherit", background: "transparent", border: "1px solid var(--dsw-alias-border-l2)", borderRadius: 4 } }), h(Button, { type: "submit", disabled: loading }, "转到")),
              h("div", { style: { display: "grid", gridTemplateColumns: "150px minmax(0,1fr)", gap: 16, flex: 1, minHeight: 0 } },
                h("nav", { "aria-label": "服务器磁盘", style: { display: "flex", flexDirection: "column", gap: 8, overflow: "auto" } }, h("strong", null, "此电脑"), control("项目目录", () => navigate(source)), ...roots.map((root) => h(Button, { key: root, onClick: () => navigate(root) }, root))),
                h("div", { style: { overflow: "auto" }, "aria-busy": loading }, loading && h("p", { role: "status" }, "正在读取目录…"),
                  ...((listing?.entries ?? []).filter((entry) => hidden || !entry.hidden).map((entry) => h("button", { key: entry.path, type: "button", onClick: () => navigate(entry.path), style: { display: "block", width: "100%", textAlign: "left", padding: "10px 12px", background: "transparent", color: "inherit", border: 0, cursor: "pointer" } }, "▸ ", entry.name))),
                  listing && !listing.entries.length && !loading && h("p", null, "此目录没有子文件夹"))),
              (error || props.error) && h("p", { role: "alert" }, error || props.error),
              folder !== null && h("form", { onSubmit: async (e) => { e.preventDefault(); try { const created = await inner.uiWorkspace.createDirectory(listing.path, folder); setFolder(null); await navigate(created); } catch (e) { setError(e.message); } } }, h("input", { "aria-label": "新文件夹名称", required: true, value: folder, onChange: (e) => setFolder(e.target.value) }), h(Button, { type: "submit" }, "创建")),
              h("footer", { style: { display: "flex", gap: 12, alignItems: "center" } }, control("新建文件夹", () => setFolder(""), !listing || loading), h("label", null, h("input", { type: "checkbox", checked: hidden, onChange: (e) => setHidden(e.target.checked) }), "显示隐藏文件"), h("span", { style: { flex: 1 } }), control("取消", props.onCancel), control("选择此文件夹", () => props.onPicked(listing.path), !listing || loading))));
        }
        for (const name of ["conversation.hero.workspace.directoryFlow", "sidebar.workspaces.directoryFlow"])
          ctx.slots.inject(name, () => ctx.slots.register({ name, id: "geo-directory-explorer", priority: 100 }, DirectoryExplorer));
        const createDevelopment = async (reuse = false) => {
          const workspace = await inner.remote.workspace.create({ path: source });
          if (!workspace.ok) throw new Error(workspace.error?.message || "开发工作区不可用");
          await inner.sessions.refresh();
          const snapshot = inner.sessions.list.getSnapshot();
          const blank = reuse && snapshot.ids.find((id) => {
            const item = snapshot.byId[id];
            return item?.blank && item.cwd === source && item.agentPreset === "cordis";
          });
          if (blank) { inner.sessions.open(blank); return; }
          const result = await inner.remote.session.create({ workspaceId: workspace.value.workspace.workspaceId, agentPreset: "cordis" });
          if (!result.ok) throw new Error(result.error?.message || "创建失败");
          await inner.sessions.refresh(); inner.sessions.open(result.value.sessionId);
        };
        // Archiving marks a session in the registry-wide archive set; the
        // session list still contains it, so the shell list filters it out.
        const archivedSessions = () => {
          try {
            const snapshot = inner.workspaces?.list?.getSnapshot?.() ?? inner.workspaces?.getSnapshot?.() ?? null;
            return new Set(snapshot?.archivedSessionIds ?? []);
          } catch { return new Set(); }
        };
        const publish = () => {
          const snapshot = inner.sessions.list.getSnapshot();
          const archived = archivedSessions();
          // An explicit rename must survive on a still-blank session, so the
          // stored title wins over the "新创造任务" placeholder.
          notify("geo:development-catalog", { current: snapshot.current, items: snapshot.ids.filter((id) => !archived.has(id)).slice(0, 200).map((id) => ({ id, title: snapshot.byId[id]?.displayTitle || snapshot.byId[id]?.title || "新创造任务", running: snapshot.byId[id]?.running === true })) });
        };
        const receive = async (event) => {
          if (event.source !== window.parent || event.origin !== location.origin) return;
          try {
            if (event.data?.type === "geo:development-create") {
              // Reuse an existing blank task instead of adding another empty one.
              await createDevelopment(true); publish();
            } else if (event.data?.type === "geo:development-open") {
              const id = event.data.id;
              if (!inner.sessions.list.getSnapshot().ids.includes(id)) throw new Error("开发任务不存在");
              inner.sessions.open(id);
            } else if (event.data?.type === "geo:development-rename") {
              const id = event.data.id, title = String(event.data.title ?? "").trim();
              if (!title) throw new Error("名称不能为空");
              const binding = inner.sessions.binding(id);
              if (!binding) throw new Error("创造任务不存在");
              const result = await binding.session.rename(title);
              if (!result?.ok) throw new Error(result?.error?.message || "重命名失败");
              await inner.sessions.refresh(); publish();
            } else if (event.data?.type === "geo:development-delete") {
              const id = event.data.id, wasCurrent = inner.sessions.list.getSnapshot().current === id;
              if (!inner.sessions.list.getSnapshot().ids.includes(id)) throw new Error("创造任务不存在");
              await inner.uiWorkspace.archiveSession(id);
              await inner.sessions.refresh(); publish();
              // Archiving the session that is currently open does not remove it
              // from the list; report that honestly instead of failing silently.
              if (!archivedSessions().has(id)) throw new Error("当前创造任务不能删除，请先切换到其他任务");
              if (wasCurrent && !disposed) await createDevelopment(true);
            } else if (event.data?.type === "geo:development-settings") {
              const trigger = document.querySelector('[data-slot="sidebar.settings"] button');
              if (!trigger) throw new Error("原生设置尚未就绪");
              trigger.click();
            }
          } catch (error) { notify("geo:development-error", { message: error.message }); }
        };
        inner.effect(() => {
          // Keep the native picker and its navigation; only seed an unspecified path.
          const listDirectory = inner.uiWorkspace.listDirectory;
          const scopedListDirectory = (path, signal) => listDirectory.call(inner.uiWorkspace, path ?? source, signal);
          inner.uiWorkspace.listDirectory = scopedListDirectory;
          const off = inner.sessions.list.subscribe(publish); window.addEventListener("message", receive);
          // Track the native three-column grid by observation instead of polling.
          // The details column changes size when the sidebar collapses or the
          // user drags it, and the body mutation catches it mounting after the
          // first paint — no timer, and no regex over a possibly non-px track.
          let root = null, tracked = null, sizes = null, changes = null, scheduled = false;
          const measure = () => {
            if (!root?.isConnected) return;
            const tracks = getComputedStyle(root).gridTemplateColumns.split(/\s+/).filter(Boolean);
            const width = tracks[tracks.length - 1];
            if (width?.endsWith("px")) root.style.setProperty("--geo-embedded-details", width);
            if (!root.hasAttribute("data-sidebar-collapsed")) { try { inner.get("layout")?.toggleSidebar(); } catch {} }
          };
          const sync = () => {
            const nextRoot = document.querySelector('div:has(> div > [data-slot="sidebar"])');
            const nextTracked = nextRoot?.querySelector('[data-slot="details"]') ?? null;
            if (nextRoot === root && nextTracked === tracked) { measure(); return; }
            sizes?.disconnect();
            root = nextRoot; tracked = nextTracked;
            sizes = typeof ResizeObserver === "function" ? new ResizeObserver(measure) : null;
            const target = tracked ?? root;
            if (target) sizes?.observe(target);
            measure();
          };
          const schedule = () => {
            if (scheduled) return;
            scheduled = true;
            const run = () => { scheduled = false; sync(); };
            if (typeof requestAnimationFrame === "function") requestAnimationFrame(run);
            else if (typeof setTimeout === "function") setTimeout(run, 0);
          };
          if (typeof MutationObserver === "function") {
            changes = new MutationObserver(schedule);
            changes.observe(document.body, { childList: true, subtree: true });
          }
          sync();
          const ready = inner.get("loader")?.await() ?? Promise.resolve();
          let disposed = false;
          ready.then(async () => {
            await inner.sessions.refresh();
            // Blank creation tasks hold nothing: on every load keep only the one
            // in use, so repeated 新建创造任务 cannot pile up empty entries.
            const snapshot = inner.sessions.list.getSnapshot(), archived = archivedSessions();
            const stale = snapshot.ids.filter((id) => id !== snapshot.current && snapshot.byId[id]?.blank && !archived.has(id));
            for (const id of stale) { try { await inner.uiWorkspace.archiveSession(id); } catch { /* one failure must not block the rest */ } }
            if (stale.length) await inner.sessions.refresh();
            const settled = inner.sessions.list.getSnapshot(), current = settled.byId[settled.current];
            if (!disposed && (!current || current.blank)) await createDevelopment(true);
            if (!disposed) { notify("geo:development-ready"); publish(); }
          }).catch((error) => notify("geo:development-error", { message: error.message }));
          return () => {
            disposed = true; sizes?.disconnect(); changes?.disconnect(); off(); window.removeEventListener("message", receive);
            if (inner.uiWorkspace.listDirectory === scopedListDirectory) inner.uiWorkspace.listDirectory = listDirectory;
          };
        });
      });
      // Ordinary-user capability management. The panel reads the published
      // catalog (product.json + shipped skills) from the admin worker and
      // toggles a narrowing policy file that the running product re-reads
      // within a few seconds. It can only switch OFF published capabilities.
      function Capabilities({ open: visible, onClose }) {
        const h = React.createElement, ref = React.useRef(null);
        const [data, setData] = React.useState(null), [error, setError] = React.useState(""), [busy, setBusy] = React.useState(false);
        const load = async () => {
          setBusy(true); setError("");
          try { const response = await fetch("/geo/api/development/capabilities"); const value = await response.json(); if (!response.ok) throw new Error(value.error || "读取失败"); setData(value); }
          catch (e) { setError(e.message); } finally { setBusy(false); }
        };
        React.useEffect(() => {
          if (!visible) { ref.current?.close(); return; }
          ref.current?.showModal(); void load();
        }, [visible]);
        const toggle = async (group, name, enabled) => {
          setBusy(true); setError("");
          try {
            const response = await fetch("/geo/api/development/capabilities", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ group, name, enabled }) });
            const value = await response.json();
            if (!response.ok) throw new Error(value.error || "保存失败");
            setData(value);
          } catch (e) { setError(e.message); } finally { setBusy(false); }
        };
        const row = (group, item) => h("div", { key: group.id + ":" + item.name, style: { display: "flex", alignItems: "center", gap: 10, padding: "6px 0" } },
          h("span", { "aria-hidden": true, title: item.enabled ? "常驻" : "禁用", style: { color: item.enabled ? "var(--dsw-alias-label-primary)" : "var(--dsw-alias-label-tertiary)" } }, item.enabled ? "●" : "○"),
          h("code", { style: { minWidth: 220 } }, item.name),
          item.description && h("span", { style: { flex: 1, minWidth: 0, color: "var(--dsw-alias-label-tertiary)", fontSize: 12, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" } }, item.description),
          h(Button, { disabled: busy, onClick: () => toggle(group.id, item.name, !item.enabled) }, item.enabled ? "关闭" : "启用"));
        return h("dialog", { ref, "aria-label": "普通模式能力管理", onCancel: onClose, style: { width: "min(880px,92vw)", height: "min(640px,88vh)", padding: 24, background: "var(--dsw-alias-bg-layer-2)", color: "var(--dsw-alias-label-primary)", border: "1px solid var(--dsw-alias-border-l2)", borderRadius: 8 } },
          h("div", { style: { display: "flex", flexDirection: "column", height: "100%", gap: 12 } },
            h("strong", null, "普通模式能力管理"),
            h("p", { style: { margin: 0, color: "var(--dsw-alias-label-tertiary)", fontSize: 12 } },
              "只能关闭发布快照里已有的能力，不能新增；保存后普通用户实例在数秒内生效。● 常驻 · ○ 禁用",
              data ? ` · rev ${data.revision}` : ""),
            h("div", { style: { flex: 1, minHeight: 0, overflow: "auto" } },
              !data && h("p", { role: "status" }, "正在读取…"),
              ...((data?.groups ?? []).map((group) => h("details", { key: group.id, open: group.id === "main", style: { borderBottom: "1px solid var(--dsw-alias-border-l2)", paddingBottom: 8 } },
                h("summary", null, `${group.label} · ${group.items.filter((item) => item.enabled).length}/${group.items.length}`),
                ...group.items.map((item) => row(group, item)))))),
            error && h("p", { role: "alert" }, error),
            h("footer", { style: { display: "flex", gap: 12, alignItems: "center" } },
              h("code", { style: { flex: 1, minWidth: 0, color: "var(--dsw-alias-label-tertiary)", fontSize: 12, overflow: "hidden", textOverflow: "ellipsis" } }, data?.file ?? ""),
              h(Button, { disabled: busy, onClick: load }, "刷新"), h(Button, { onClick: onClose }, "关闭"))));
      }
      if (globalThis.__GEOSENTINEL_DEVELOPMENT__) {
        ctx.inject(["locale"], (inner) => {
          inner.effect(() => inner.locale.addLanguage({ id: "zh-Hans", label: "中文", fallback: "zh" }));
          inner.effect(() => inner.locale.register("conversation", "zh-Hans", { "hero.headline": "地缘环境智能计算平台", "hero.preview": "开发模式" }));
          inner.locale.setLocale("zh-Hans");
        });
      }
      function CapabilityRow() {
        const h = React.createElement;
        const [open, setOpen] = React.useState(false);
        return h(React.Fragment, null,
          h("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, padding: "16px 0" } },
            h("span", null, "普通模式能力管理"),
            h(Button, { onClick: () => setOpen(true) }, "管理工具与技能")),
          h(Capabilities, { open, onClose: () => setOpen(false) }));
      }
      ctx.slots.inject("settings.general.item", () => ctx.slots.register({ name: "settings.general.item", id: "geosentinel-capabilities", order: 31 }, CapabilityRow));
      ctx.slots.inject("settings.general.item", () => ctx.slots.register({ name: "settings.general.item", id: "geosentinel-release", order: 30 }, () =>
        React.createElement("div", { style: { display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, padding: "16px 0" } },
          React.createElement("span", null, "GeoSentinel 产品配置"),
          React.createElement("div", { style: { display: "flex", gap: 8 } }, React.createElement(Button, { onClick: research }, "研究模式"), React.createElement(Button, { onClick: open }, "配置与同步发布")))));
      // Embedded sessions carry no extra header actions: the sidebar already
      // owns 新建创造任务 and the mode tabs own 返回研究任务.
      ctx.slots.inject("conversation.session.header.utilities", () => ctx.slots.register({ name: "conversation.session.header.utilities", id: "geosentinel-publish" },
        () => embedded ? null : React.createElement("div", { style: { display: "flex", alignItems: "center", gap: 8 } }, React.createElement(Button, { onClick: research }, "研究模式"), React.createElement(Button, { onClick: open }, "产品发布"))));
    } };
  },
});
