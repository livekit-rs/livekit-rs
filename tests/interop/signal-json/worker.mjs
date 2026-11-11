/** Actual authenticated registered worker WS JSON profile, no jobs fabricated. */
import assert from 'node:assert/strict';
import {readFile,writeFile} from 'node:fs/promises';
import {randomUUID,createHash} from 'node:crypto';
import {AccessToken} from 'livekit-server-sdk';
const cases=JSON.parse(await readFile(process.env.WORKER_JSON_GO_CASES,'utf8'));
const rows=(await readFile(process.env.WORKER_JSON_GO_RESULTS,'utf8')).split('\n').filter(Boolean).map(s=>JSON.parse(s));
assert.equal(cases.length,20);assert.equal(rows.length,20);
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',apiKey=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const report={status:'RUNNING',scope:'real authenticated registered worker text frame/Pong or close, no job/WT/media claim',cases:[],cleanupErrors:[]};
let socket,timer;
const safe=e=>String(e).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
async function closeOwned(peer){if(!peer||peer.readyState===WebSocket.CLOSED)return;let timer;try{const closed=new Promise((resolve,reject)=>{peer.addEventListener('close',resolve,{once:true});timer=setTimeout(()=>reject(new Error('actual owned worker close timeout')),10000);});peer.close(1000,'fixture complete');const event=await closed;assert.equal(event.code,1000);assert.equal(event.wasClean,true);}finally{clearTimeout(timer);}}
try{
 for(const [index,c] of cases.entries()){
  const expected=rows[index];assert.equal(c.id,expected.id);const row={id:c.id,status:'RUNNING'};report.cases.push(row);
  const token=new AccessToken(apiKey,secret,{identity:`json-worker-${randomUUID()}`});token.addGrant({agent:true});const endpoint=new URL('/agent',url);endpoint.searchParams.set('access_token',await token.toJwt());
  socket=new WebSocket(endpoint);const marker=700000+index;let registered=false,settled=false;
  const outcome=await new Promise((resolve,reject)=>{
   timer=setTimeout(()=>reject(new Error('actual worker JSON registration/outcome timeout')),10000);
   socket.addEventListener('open',()=>socket.send(JSON.stringify({register:{type:'JT_ROOM',agentName:'f49-json-wire',namespace:'fixture',version:'f49-json-profile'}})));
   socket.addEventListener('message',event=>{try{
    assert.equal(typeof event.data,'string','actual worker JSON response required');const message=JSON.parse(event.data);
    if(!registered){assert.match(message.register?.workerId||'',/^AW_/);registered=true;row.registered=true;socket.send(c.json);socket.send(JSON.stringify({ping:{timestamp:String(marker)}}));return;}
    if(message.pong&&Number(message.pong.lastTimestamp)===marker){settled=true;resolve('ACCEPTED');}
   }catch(error){reject(error);}});
   socket.addEventListener('close',event=>{row.actualClose={code:event.code,clean:event.wasClean,reason:event.reason};if(!settled){if(!registered)reject(new Error('actual worker closed before registration'));else{settled=true;resolve('REJECTED');}}});
   socket.addEventListener('error',()=>{row.transportErrors=(row.transportErrors||0)+1;if(!registered)reject(new Error('actual worker JSON transport error before registration'));});
  });clearTimeout(timer);row.outcome=outcome;assert.equal(outcome,expected.accepted?'ACCEPTED':'REJECTED',c.id);
  await closeOwned(socket);socket=undefined;row.closed=true;row.status='PASS';
 }
 report.status='PASS_SCOPED';report.actualWorkerWsCases=20;
}catch(error){report.status='FAIL';report.error=safe(error);process.exitCode=1;}
finally{
 clearTimeout(timer);try{await closeOwned(socket);}catch(error){report.cleanupErrors.push(safe(error));report.status='FAIL';process.exitCode=1;}
 report.componentOutputSha256=createHash('sha256').update(await readFile(process.env.WORKER_JSON_GO_RESULTS)).digest('hex');
 await writeFile(process.env.WORKER_JSON_OUTPUT||'worker-json-wrapper.json',JSON.stringify(report,null,2)+'\n');
}
