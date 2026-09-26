import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
const examples = source.slice(source.indexOf("    const researchExamples ="), source.indexOf("    function apply(ctx)"));
const component = source.slice(source.indexOf("      function ResearchExamples("), source.indexOf("      function HeroBrand("));
function harness() {
  let draft = "", attachments = [], phase = "plain", timer, reduced = false, hooks = [], cursor;
  const shell = { state: { subscribe() {}, getSnapshot: () => ({ draft, attachmentIds: attachments, phase }) }, setDraft(text) { draft = text; } };
  const h = (type, props, ...children) => ({ type, props: props ?? {}, children });
  const context = vm.createContext({
    ctx: { get: () => ({ input: { for: () => shell } }) }, binding: () => ({ ctx: {} }),
    h, icons: {}, button: (title, icon, onClick) => h("button", { onClick }, title),
    document: { hidden: false }, window: { matchMedia: () => ({ get matches() { return reduced; } }) },
    setInterval: fn => { timer = fn; return 1; }, clearInterval() {},
    React: { useSyncExternalStore: (_, get) => get(), useState(initial) { const i = cursor++; if (!(i in hooks)) hooks[i] = typeof initial === "function" ? initial() : initial; return [hooks[i], value => { hooks[i] = typeof value === "function" ? value(hooks[i]) : value; }]; }, useEffect(fn) { fn(); } },
  });
  const render = vm.runInContext(examples + component + "\nResearchExamples", context);
  return { render() { cursor = 0; timer = undefined; return render({ id: "c1" }); }, tick() { timer?.(); }, setDraft(value) { draft = value; }, getDraft: () => draft,
    setAttachments(value) { attachments = value; }, setReduced(value) { reduced = value; } };
}
test("examples fill native draft only, preserve typed drafts and attachments", () => {
  const ui = harness();
  let tree = ui.render();
  assert.equal(tree.children[0].props.disabled, false);
  tree.children[0].props.onClick();
  assert.match(ui.getDraft(), /share\/缅甸地理\/CATALOG.md/);
  ui.setDraft("用户自己的草稿");
  // Rechecks the latest draft even if the old button was clicked before rerender.
  tree.children[0].props.onClick();
  assert.equal(ui.getDraft(), "用户自己的草稿");
  assert.equal(ui.render().children[0].props.disabled, true);
  ui.setDraft(""); ui.setAttachments(["upload-1"]);
  assert.equal(ui.render().children[0].props.disabled, true);
});
test("rotation changes examples and pauses for reduced motion, focus and explicit pause", () => {
  const ui = harness();
  const title = tree => tree.children[0].children[0].children[0];
  let tree = ui.render(), first = title(tree);
  ui.tick(); tree = ui.render(); assert.notEqual(title(tree), first);
  ui.setReduced(true); first = title(tree); ui.tick(); assert.equal(title(ui.render()), first);
  ui.setReduced(false); tree.props.onFocus(); tree = ui.render(); first = title(tree); ui.tick(); assert.equal(title(ui.render()), first);
  tree.props.onMouseLeave(); tree = ui.render(); ui.tick(); assert.equal(title(ui.render()), first);
  tree.props.onBlur({ currentTarget: { contains: () => false }, relatedTarget: null }); tree = ui.render(); tree.children[1].children[2].props.onClick(); tree = ui.render(); first = title(tree); ui.tick(); assert.equal(title(ui.render()), first);
});
