import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
const helper = source.slice(source.indexOf("    function nativeArtifactEvent("), source.indexOf("    function teamTodos("));
const normalize = vm.runInNewContext(helper + "\nnativeArtifactEvent", { location: { origin: "http://example.test:8502" } });
const event = (text) => ({ type: "assistant/message", data: { message: { content: [{ type: "text", text }] } } });

test("native artifact images use the current origin without enabling host file access", () => {
  for (const route of ["/geo/api/chats/chat-1/files?path=outputs%2F图.png&inline=1", "/geo/api/chats/chat_2/files?path=job%2Fplot.jpg&inline=1"]) {
    const original = event(`![分析图](${route})`);
    const result = normalize(original);
    assert.equal(result.data.message.content[0].text, `![分析图](http://example.test:8502${route})`);
    assert.equal(original.data.message.content[0].text, `![分析图](${route})`);
  }
});

test("unrelated destinations, tool content and user text are not rewritten", () => {
  for (const text of ["![x](/etc/passwd)", "![x](//evil.test/p.png)", "![x](https://other.test/p.png)", "[download](/geo/api/chats/c/files?path=x)"]) {
    assert.equal(normalize(event(text)).data.message.content[0].text, text);
  }
  const user = { ...event("![x](/geo/api/chats/c/files?path=x)"), type: "user/message" };
  assert.equal(normalize(user), user);
});

test("completed streaming image tokens and angle-bracket URLs are supported", () => {
  const chunk = { type: "assistant/chunk", data: { chunk: { type: "text-delta", text: "![x](</geo/api/chats/c/files?path=p.png&inline=1>)" } } };
  assert.equal(normalize(chunk).data.chunk.text, "![x](<http://example.test:8502/geo/api/chats/c/files?path=p.png&inline=1>)");
});

test("Markdown whitespace and optional image titles from real history are supported", () => {
  const route = "/geo/api/chats/chat-1/files?path=outputs%2Fplot.png&inline=1";
  for (const text of [`![图]( ${route})`, `![图](\n<${route}>  )`, `![图]( ${route} "资料图")`]) {
    assert.equal(normalize(event(text)).data.message.content[0].text, text.replace(route, "http://example.test:8502" + route));
  }
});
