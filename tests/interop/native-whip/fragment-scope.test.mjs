// Exact native fixture helper formatting, not a simulated RTC/media result.
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
const source=await readFile(new URL('./native.js',import.meta.url),'utf8');
const sandbox=vm.createContext({window:{}});vm.runInContext(source,sandbox);
const offer=['v=0','o=- 1 1 IN IP4 127.0.0.1','s=-','t=0 0','a=group:BUNDLE 0','m=audio 9 UDP/TLS/RTP/SAVPF 111','c=IN IP4 0.0.0.0','a=mid:0','a=ice-ufrag:newClient','a=ice-pwd:newClientPassword0123456789'].join('\r\n')+'\r\n';
test('actual helper preserves Chrome media ICE scope for fixed Go restart patcher',()=>{
 const candidate='candidate:2 1 udp 2122260223 192.0.2.1 50001 typ host';
 const actual=vm.runInContext(`fragment(${JSON.stringify(offer)},${JSON.stringify([candidate])})`,sandbox);
 assert.equal(actual,['a=group:BUNDLE 0','m=audio 9 UDP/TLS/RTP/SAVPF 111','a=mid:0','a=ice-ufrag:newClient','a=ice-pwd:newClientPassword0123456789','a='+candidate].join('\r\n')+'\r\n');
 assert(actual.indexOf('a=ice-ufrag:')>actual.indexOf('m=audio'));
 assert(source.includes("expectedUfrag===oldUfrag)throw Error('real restart must change local ICE credentials')"));
 assert(source.includes("newTag===oldTag)throw Error('actual ICE restart did not change native local ufrag ETag')"));
 assert(source.includes('const end=Date.now()+10000'));
});
