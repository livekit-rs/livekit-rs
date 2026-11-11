import assert from 'node:assert/strict';
import{topologyValid,recovered}from'./criteria.mjs';
import{readFile}from'node:fs/promises';
const files=process.argv.slice(2);assert.equal(files.length,4,'Go false, Rust false, Go true, Rust true result paths required');
const values=await Promise.all(files.map(async p=>JSON.parse(await readFile(p,'utf8'))));
for(let pair=0;pair<2;pair++){
 const[go,rust]=values.slice(pair*2,pair*2+2);
 for(const result of[go,rust]){
  assert.equal(result.status,'PASS_SCOPED');assert.equal(result.flag,pair===1);assert.equal(result.sdk,'2.23.0');assert.deepEqual(result.cleanupErrors,[]);
  assert.equal(result.rows.length,4,'both PC modes and both delivery kinds are required, no partial aggregation');
  const expected=['false:lossy','false:reliable','true:lossy','true:reliable'];
  assert.deepEqual(result.rows.map(r=>`${r.single}:${r.kind}`).sort(),expected);
  for(const r of result.rows){assert.equal(r.topologyBefore.length,3);assert.ok(r.topologyBefore.every(t=>topologyValid(t,r.single)));assert.equal(r.topologyAfter.length,2);assert.ok(r.topologyAfter.every(t=>topologyValid(t,r.single)));assert.equal(r.status,'PASS');assert.equal(r.closeOnly.status,'PASS');assert.equal(r.closeOnly.noReconnect,true);assert.deepEqual(r.cleanupErrors,[]);assert.equal(r.final.state,'connected');assert.ok(r.final.videoProgress>0&&r.final.audioProgress>0&&r.final.witnessVideoProgress>0);assert.deepEqual(r.final.playbackErrors,[]);if(pair===1){assert.equal(r.rebuild.status,'PASS');assert.equal(r.rebuild.newSid,true);assert.equal(r.rebuild.newNativeHandle,true);assert.ok(r.rebuild.elapsedMs<=30000);assert.ok(r.newGenerationMedia.elapsedMs<=30000);assert.ok(recovered(r.newGenerationMedia.victim)&&recovered(r.newGenerationMedia.witness));assert.equal(r.rebuild.localTrackCount,2);assert.ok(topologyValid(r.rebuild.topology,r.single));assert.ok(Object.values(r.newGenerationSustain).every(n=>n>0));}else{assert.equal(r.noReconnect.status,'PASS');assert.equal(r.noReconnect.stableSid,true);assert.equal(r.noReconnect.otherKindDelivered,true);}}
 }
 assert.equal(go.sdkSha256,rust.sdkSha256,'same unchanged browser SDK bytes required');
 for(const g of go.rows){const r=rust.rows.find(r=>r.single===g.single&&r.kind===g.kind);assert.deepEqual(r.final.events.filter(e=>['Reconnecting','Reconnected','SignalReconnecting'].includes(e.type)),g.final.events.filter(e=>['Reconnecting','Reconnected','SignalReconnecting'].includes(e.type)),'actual reconnect event order/count differs');}
}
console.log(JSON.stringify({status:'PASS_SCOPED',scope:'four genuine prestarted profiles; 16 actual official-SDK lifecycle rows, no publication/subscription flag claim'}));
