import test from "node:test";
import assert from "node:assert/strict";
import { withArtifacts } from "../plugins/research/index.mjs";

// The conversation can only display what the tool result announces: a figure is
// reachable in the chat exactly when `withArtifacts` listed it with `inline: true`
// and a same-origin `url` carrying `inline=1`. These assertions lock that producer
// side, so "the answer mentioned a chart" can never pass for "the chat showed it".
const identity = { chatId: "chat-0000-1111-2222-333344445555" };
const job = "20260910-120352-execute-4043d8";

test("artifact listing marks images with an inline url and keeps other files as downloads", () => {
  const value = {
    status: "completed",
    result: {
      figure: `outputs/${job}/fig6-2.png`,
      table: `outputs/${job}/table6-2_复算结果.csv`,
      report: `outputs/${job}/report.md`,
      bundle: `outputs/${job}/layers.zip`,
      outside: "inputs/城市体系位序规模.xlsx",
    },
  };
  const { artifacts } = withArtifacts(value, identity);
  const byPath = new Map(artifacts.map((artifact) => [artifact.path, artifact]));

  // Every job-relative file is announced; anything outside `outputs/<job>/` is not.
  assert.deepEqual([...byPath.keys()].sort(), [
    `outputs/${job}/fig6-2.png`,
    `outputs/${job}/layers.zip`,
    `outputs/${job}/report.md`,
    `outputs/${job}/table6-2_复算结果.csv`,
  ]);

  const figure = byPath.get(`outputs/${job}/fig6-2.png`);
  assert.equal(figure.kind, "image");
  assert.equal(figure.inline, true);
  assert.ok(figure.url.startsWith(`/geo/api/chats/${identity.chatId}/files?path=`), figure.url);
  assert.ok(figure.url.endsWith("&inline=1"), figure.url);
  // The url must address the chat-workspace form the files route accepts.
  assert.equal(decodeURIComponent(figure.url.split("path=")[1].split("&")[0]), figure.path);

  // Allowlisted types keep an inline url (a CSV renders as a table, a Markdown
  // report as text); a zip is announced for download only, with no url at all.
  const inlineExpectations = [["table6-2_复算结果.csv", "table"], ["report.md", "text"]];
  for (const [name, kind] of inlineExpectations) {
    const artifact = byPath.get(`outputs/${job}/${name}`);
    assert.equal(artifact.kind, kind);
    assert.equal(artifact.inline, true);
    assert.ok(artifact.url.endsWith("&inline=1"), artifact.url);
    assert.equal(decodeURIComponent(artifact.url.split("path=")[1].split("&")[0]), artifact.path);
  }
  const bundle = byPath.get(`outputs/${job}/layers.zip`);
  assert.equal(bundle.kind, "file");
  assert.equal(bundle.inline, undefined);
  assert.equal(bundle.url, undefined);
});

test("artifact listing ignores values without job outputs and leaves them untouched", () => {
  assert.equal(withArtifacts({ status: "completed", result: { note: "无产物" } }, identity).artifacts, undefined);
  assert.equal(withArtifacts("plain text", identity), "plain text");
  assert.equal(withArtifacts(null, identity), null);
});

test("file-path forms from the platform envelope are announced as artifacts too", () => {
  const { artifacts } = withArtifacts(
    { files: [{ path: `${job}/fig.png`, size: 1234 }], result: {} },
    identity,
  );
  assert.deepEqual(artifacts, [
    {
      path: `outputs/${job}/fig.png`,
      kind: "image",
      inline: true,
      url: `/geo/api/chats/${identity.chatId}/files?path=${encodeURIComponent(`outputs/${job}/fig.png`)}&inline=1`,
    },
  ]);
});
