import test from "node:test";
import assert from "node:assert/strict";
import {normalizeBundleInserts} from "../development/bundle-compat.mjs";
test("duplicate bundle inserts become configuration overrides without disabling plugins",()=>{
 for(const name of ["better-sidebar","dream-skin"]){
  const bundle=[{insert:[{id:name,name:"dsh-"+name}]}];
  assert.deepEqual(normalizeBundleInserts(bundle,bundle),[]);
  const rows=[{insert:[{id:"other",name:"other"},{id:name,name:"dsh-"+name,config:{enabled:true}}]}];
  const normalized=normalizeBundleInserts(rows,bundle);
  assert.deepEqual(normalized,[{insert:[{id:"other",name:"other"}]},{id:name,config:{enabled:true}}]);
  assert.equal(normalizeBundleInserts(normalized,bundle),null);
 }
});
test("different plugins sharing an id are not silently merged",()=>{
 assert.throws(()=>normalizeBundleInserts([{insert:[{id:"x",name:"second"}]}],[{insert:[{id:"x",name:"first"}]}]),/ID冲突/);
});
