import { readFile, writeFile } from 'node:fs/promises';
const account=JSON.parse(await readFile('.runtime/qa-account.json','utf8'));
const res=await fetch('http://127.0.0.1:8510/geo/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(account)});
if(!res.ok)throw new Error('QA login failed');
const cookie=res.headers.get('set-cookie').split(';')[0],separator=cookie.indexOf('=');
await writeFile('.runtime/browser-auth.json',JSON.stringify({cookies:[{name:cookie.slice(0,separator),value:cookie.slice(separator+1),domain:'127.0.0.1',path:'/',expires:Math.floor(Date.now()/1000)+604800,httpOnly:true,secure:false,sameSite:'Strict'}],origins:[]}));
console.log('Refreshed ignored QA browser state; no credentials printed.');
