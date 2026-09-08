import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
const base = "http://127.0.0.1:8510/geo/api";
const account = JSON.parse(await readFile(".runtime/qa-account.json", "utf8"));
const login = await fetch(base + "/auth/login", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(account),
});
const cookie = login.headers.get("set-cookie").split(";")[0];
const session = JSON.parse(
  await readFile(".runtime/research-session.json", "utf8"),
);
async function api(route) {
  const res = await fetch(base + route, { headers: { cookie } });
  assert.equal(res.status, 200);
  return res.json();
}
const history = await api(`/chats/${session.chatId}/history`);
assert.equal(history.status, "completed");
assert.ok(
  history.events.some(
    (e) =>
      e.agentRole === "NTL_Data_Searcher" &&
      e.type === "tool/call" &&
      e.data.name === "geo_download_gee",
  ),
);
assert.ok(
  history.events.some(
    (e) =>
      e.agentRole === "NTL_Analyst" &&
      e.type === "tool/call" &&
      e.data.name === "geo_inspect_raster",
  ),
);
const { files } = await api(`/chats/${session.chatId}/files`);
const raster = files.find((f) => f.name.endsWith("/imagery.tif"));
assert.ok(raster);
assert.ok(
  files.some((f) => /\.md$|summary.*\.json$/i.test(f.name)),
  "analysis report missing",
);
const res = await fetch(
  base +
    `/chats/${session.chatId}/files?path=${encodeURIComponent(raster.name)}`,
  { headers: { cookie } },
);
assert.equal(res.status, 200);
assert.match(res.headers.get("content-disposition"), /attachment/);
const bytes = Buffer.from(await res.arrayBuffer());
assert.equal(bytes.length, raster.size);
assert.ok(bytes.length > 100);
assert.ok(
  bytes.subarray(0, 4).equals(Buffer.from([73, 73, 42, 0])) ||
    bytes.subarray(0, 4).equals(Buffer.from([77, 77, 0, 42])),
  "not a TIFF",
);
assert.equal(
  (
    await fetch(
      base +
        `/chats/${session.chatId}/files?path=${encodeURIComponent(raster.name)}`,
    )
  ).status,
  401,
);
const report = {
  passed: true,
  dataset: session.dataset ?? "SRTM",
  chatId: session.chatId,
  raster: {
    ...raster,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  },
  files: files.length,
  roles: ["NTL_Engineer", "NTL_Data_Searcher", "NTL_Analyst"],
  at: new Date().toISOString(),
};
await writeFile(
  `.runtime/research-${report.dataset.toLowerCase()}-acceptance.json`,
  JSON.stringify(report, null, 2),
);
console.log(JSON.stringify(report));
