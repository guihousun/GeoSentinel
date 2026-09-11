import test from "node:test";
import assert from "node:assert/strict";
import { assertReleaseGate } from "../release/preflight.mjs";
import { acceptPreview } from "../release/acceptance.mjs";
const valid = () => ({ id: "candidate", authorization: "本次修改验收通过后发布", version: "0.1.5-rc.1", now: 100000, evidence: { id: "candidate", passed: true, check: "preview-upload-v1", at: 90000 }, controller: { protocol: 1, version: "0.1.5-rc.1", at: 99000 } });
test("release gate rejects missing authorization, evidence, stale checks and incompatible controllers", () => {
  assert.doesNotThrow(() => assertReleaseGate(valid()));
  for (const patch of [{ authorization: "" }, { evidence: null }, { evidence: {...valid().evidence,id:"other"} }, { evidence: {...valid().evidence,passed:false} }, { now: 4000000 }, { controller:null }, { controller:{...valid().controller,version:"0.1.2-rc.1"} }]) assert.throws(() => assertReleaseGate({...valid(),...patch}));
});
test("preview probe refuses production and wrong candidates before authentication or writes", async () => {
  for (const health of [{preview:false,release:"candidate"},{preview:true,release:"other"}]) {
    const calls=[];
    await assert.rejects(acceptPreview({},"candidate","secret",{fetcher:async (url, options)=>{calls.push([url,options.method]);return new Response(JSON.stringify(health));}}), /不是指定候选/);
    assert.deepEqual(calls, [["http://127.0.0.1:8513/geo/api/health","GET"]]);
  }
});
