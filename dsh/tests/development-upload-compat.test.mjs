import test from "node:test";
import assert from "node:assert/strict";
import {normalizeUploadBundle} from "../development/upload-compat.mjs";
const pkg={dsh:{profile:{bundles:["native","dsh-file-upload","other"]}}};
const bundle=[{insert:[{id:"file-upload",name:"dsh-file-upload",config:{maxFileBytes:12}}]}];
test("upload migration preserves native upload, third-party config and neighboring plugins",()=>{
 const result=normalizeUploadBundle(pkg,[{id:"file-upload",config:{native:true}}],bundle);
 assert.deepEqual(result.pkg.dsh.profile.bundles,["native","other"]);
 assert.deepEqual(result.rows[0],{id:"file-upload",config:{native:true}});
 assert.deepEqual(result.rows[1].insert[0],{id:"geo-file-upload",name:"dsh-file-upload",config:{maxFileBytes:12}});
 assert.equal(normalizeUploadBundle(result.pkg,result.rows,bundle),null);
 assert.equal(pkg.dsh.profile.bundles.length,3);
});
test("existing namespaced plugin is reused and unrelated id collisions fail",()=>{
 const row={id:"custom-upload",name:"dsh-file-upload",config:{maxFileBytes:25}};
 assert.deepEqual(normalizeUploadBundle(pkg,[{insert:[row]}],bundle).rows,[{insert:[row]}]);
 assert.throws(()=>normalizeUploadBundle(pkg,[{insert:[{id:"geo-file-upload",name:"other"}]}],bundle),/already used/);
});
