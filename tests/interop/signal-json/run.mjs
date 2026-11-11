/** Actual authenticated local WS LiveKit JSON profile; no SDK/source rewrites. */
import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {randomUUID,createHash} from 'node:crypto';
import {AccessToken,RoomServiceClient} from 'livekit-server-sdk';
// Copied, byte-identified descriptor-specific F40 consumer, not an SDK mutation.
import {readResponse} from './raw-protocol.mjs';
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',apiKey=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const cases=JSON.parse(await readFile(process.env.SIGNAL_JSON_GO_CASES,'utf8'));
const rows=(await readFile(process.env.SIGNAL_JSON_GO_RESULTS,'utf8')).split('\n').filter(Boolean).map(s=>JSON.parse(s));
assert.equal(cases.length,35);assert.equal(rows.length,35);
const api=new RoomServiceClient(url.replace(/^ws/,'http'),apiKey,secret),room=`wrapper-json-${randomUUID()}`;
const report={status:'RUNNING',scope:'local authenticated SignalRequest text parser/pong or close, no media/SDK claim',cases:[],cleanupErrors:[]};
const clean=error=>String(error).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
let socket;
async function closeOwned(peer){
 if(!peer||peer.readyState===WebSocket.CLOSED)return;
 let timer;
 const closed=new Promise((resolve,reject)=>{
  peer.addEventListener('close',resolve,{once:true});
  timer=setTimeout(()=>reject(new Error('owned LiveKit JSON cleanup close timeout')),10000);
 });
 try{peer.close(1000,'fixture cleanup');await closed;}finally{clearTimeout(timer);}
}
try{
 await api.createRoom({name:room});
 // Valid trickle is not a parser-only operation without negotiated ICE, so these
 // four enum semantics are proved by the independent component oracle instead.
 // Invalid-frame tests wait for actual close without queuing unrelated input.
 // Accepted-frame tests require a subsequent correlated Pong for liveness.
 // A final malformed sentinel then exercises server-initiated parse-error close;
 // client cleanup cannot race participant responses against a close handshake.
 const componentOnly=new Set(['unknown-enum-name','unknown-enum-number','enum-name','enum-numeric-string']);
 for(const [index,c] of cases.entries()){
  const expected=rows[index];assert.equal(c.id,expected.id);
  if(componentOnly.has(c.id)){report.cases.push({id:c.id,scope:'COMPONENT_ORACLE_ONLY',reason:'valid trickle requires independent negotiated ICE lifecycle'});continue;}
  const row={id:c.id,status:'RUNNING'};report.cases.push(row);
  const token=new AccessToken(apiKey,secret,{identity:`wrapper-json-${randomUUID()}`});token.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});
  const endpoint=new URL(url);endpoint.pathname='/rtc';endpoint.search='';endpoint.searchParams.set('access_token',await token.toJwt());endpoint.searchParams.set('protocol','17');endpoint.searchParams.set('sdk','unknown');
  socket=new WebSocket(endpoint);socket.binaryType='arraybuffer';const marker=500000+index;
  let timer,joined=false,settled=false,closeResolve;
  const closed=new Promise(resolve=>{closeResolve=resolve;});
  const outcome=await new Promise((resolve,reject)=>{
   timer=setTimeout(()=>reject(new Error('actual LiveKit JSON response/close timeout')),10000);
   socket.addEventListener('message',event=>{try{
    if(!joined){assert.ok(event.data instanceof ArrayBuffer);assert.equal(readResponse(Buffer.from(event.data)).type,'join');joined=true;socket.send(c.json);if(expected.accepted)socket.send(JSON.stringify({pingReq:{timestamp:String(marker)}}));return;}
    const value=typeof event.data==='string'?JSON.parse(event.data):undefined;
    if(value?.pongResp&&Number(value.pongResp.lastPingTimestamp)===marker){settled=true;resolve('ACCEPTED');}
   }catch(error){reject(error);}});
   socket.addEventListener('error',()=>reject(new Error('actual LiveKit JSON WebSocket transport error')));
   socket.addEventListener('close',event=>{row.actualClose={code:event.code,clean:event.wasClean,reason:event.reason};closeResolve(event);if(!settled){if(!joined)reject(new Error('connection closed before actual Join'));else{settled=true;resolve('REJECTED');}}});
  });clearTimeout(timer);
  row.outcome=outcome;assert.equal(outcome,expected.accepted?'ACCEPTED':'REJECTED',c.id);
  if(socket.readyState!==WebSocket.CLOSED){assert.equal(outcome,'ACCEPTED');socket.send('{\"pingReq\":');let closeTimer;try{const event=await Promise.race([closed,new Promise((_,reject)=>{closeTimer=setTimeout(()=>reject(new Error('owned LiveKit JSON close timeout')),10000);})]);assert.equal(event.code,1000);assert.equal(event.wasClean,true);}finally{clearTimeout(closeTimer);}}
  socket=undefined;row.status='PASS';
 }
 report.status='PASS_SCOPED';report.componentOracleCases=35;report.actualWsCases=31;
}catch(error){report.status='FAIL';report.error=clean(error);process.exitCode=1;}
finally{
 try{await closeOwned(socket);}catch(error){report.cleanupErrors.push(clean(error));report.status='FAIL';process.exitCode=1;}
 try{await api.deleteRoom(room);}catch(error){report.cleanupErrors.push(clean(error));report.status='FAIL';process.exitCode=1;}
 report.componentOutputSha256=createHash('sha256').update(await readFile(process.env.SIGNAL_JSON_GO_RESULTS)).digest('hex');
 await writeFile(process.env.SIGNAL_JSON_OUTPUT||'signal-json-wrapper.json',JSON.stringify(report,null,2)+'\n');
}
