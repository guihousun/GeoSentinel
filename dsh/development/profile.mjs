import { stringifyDocument } from "../release/profile-schema.mjs";

/**
 * The development instance's Cordis composition, extracted from the worker so the
 * rules stay testable without spawning a child process.
 *
 * Both documents are written with the release pipeline's dialect-aware emitter
 * (`js-yaml` reached from the boot loader's own dependency tree, with the
 * content-addressed-store fallback). The plain `yaml` package the worker used before
 * is NOT linked at the root of a 0.1.5 install — `require("yaml")` there throws
 * MODULE_NOT_FOUND, so administrator development would never start after the upgrade.
 * Using one emitter for the profile, the boot overlay and the settings file also keeps
 * every document in the dialect DSH actually boots with.
 *
 * Note on the sidebars: the development instance keeps the browser API plane open
 * (`api-remotes` publishes the `remote.workspace` / `remote.directoryPicker` namespaces
 * the directory explorer uses), so the native sidebar family and `ui-workspace` activate
 * there normally. `dsh-better-sidebar` does not claim the `sidebar` slot — it provides
 * the `betterSidebar` service and adds two child surfaces — so both coexist, exactly as
 * they did on the 0.1.2 line; nothing here needs to disable one of them.
 */
export function developmentFiles({ product, platform = process.platform, pwshPath }) {
  const seeds = {
    "package.json": JSON.stringify({ name: "geosentinel-development", private: true,
      dsh: { profile: { bundles: ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app"] } } }),
    "cordis.yml": "[]\n",
    "cordis.patch.yml": stringifyDocument([
      { id: "agent-default-model", config: product.defaultModel },
      // Same native planes the product relies on: the session preset carries the
      // delegation tools, and plan mode is the staged-plan approval gate.
      { id: "agent-presets", disabled: false },
      { id: "plan-mode", disabled: false },
      { id: "ui-agent-preset", disabled: true },
      { insert: [
        { id: "geosentinel-platform", name: "@geosentinel/dsh-platform" },
        { id: "geosentinel-research", name: "@geosentinel/dsh-research" },
        { id: "geosentinel-workbench", name: "@geosentinel/dsh-workbench" },
        { id: "better-sidebar", name: "dsh-better-sidebar" },
        { id: "dream-skin", name: "dsh-dream-skin" },
      ] },
    ]),
    "settings.yaml": stringifyDocument({ "ui-theme": { preference: "dark", fontSize: 16 } }),
  };
  const overlay = stringifyDocument([
    ...(platform === "win32" ? [{ id: "pwsh-sandbox", config: { pwshPath } }] : []),
    { id: "directory-picker", disabled: true },
    { id: "web-runtime", config: { openBrowser: false, printUrl: false, surfaceContext: true, trustedHosts: [] } },
    { insert: [{ id: "geosentinel-developer", name: "@geosentinel/dsh-developer" },
      { id: "development-directory-host", name: "@deepseek-ai/dsh-host-directory-picker-browse" }] },
  ]);
  return { seeds, overlay };
}
