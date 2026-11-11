// Controlled prewarm and requestFunc hooks of the unchanged official SDK.
// Observation is deliberately separate from comparison to a real Go receipt.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import {resolve,join} from 'node:path';
import {mkdtemp,readFile,writeFile,appendFile,rm,readdir,rename} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import http from 'node:http';
import net from 'node:net';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
const sdkRoot=process.env.AGENT_SDK_ROOT,interop=process.env.INTEROP_PROJECT;
assert(sdkRoot&&interop);
const sdkURL=pathToFileURL(resolve(sdkRoot,'node_modules/@livekit/agents/dist/index.js')).href;
const {AgentServer,ServerOptions,initializeLogger}=await import(sdkURL);
const require=createRequire(resolve(interop,'package.json'));
const {RoomServiceClient,AgentDispatchClient,AccessToken}=require('livekit-server-sdk');
const sdkRequire=createRequire(resolve(sdkRoot,'package.json'));
const {Receiver}=sdkRequire('ws');
const {ServerMessage}=sdkRequire('@livekit/protocol');
const url=process.env.LIVEKIT_URL,workerURL=process.env.AGENT_WORKER_LIVEKIT_URL;
const key=process.env.LIVEKIT_API_KEY,secret=process.env.LIVEKIT_API_SECRET;
const profile=process.env.AGENT_OBSERVER_PROFILE,mode=process.env.AGENT_AVAILABILITY_MODE;
assert.equal(profile,'delayed-availability-observe');
assert.equal(mode,'accept');assert.equal(process.env.AGENT_JOB_TYPE,'0');
const temp=await mkdtemp(join(tmpdir(),'livekit-agent-delayed-public-'));
const phasePath=process.env.AGENT_ROUTING_OBSERVER;
const roomName=`delay-room-${Date.now()}`,agentName=`delay-agent-${Date.now()}`;
const started=Date.now(),timeline=[];
const hash=x=>createHash('sha256').update(x).digest('hex');
const cleanError=e=>({name:e?.name??'Error',code:e?.code??null,message_sha256:hash(String(e?.message??e)),message:String(e?.message??e).replace(/eyJ[A-Za-z0-9_.-]+/g,'[JWT]').replaceAll(secret??'secret','[FIXTURE_SECRET]').slice(0,600)});
async function event(type,fields={}){const row={type,elapsed_ms:Date.now()-started,...fields};timeline.push(row);if(process.env.AGENT_OBSERVER_EVENTS)await appendFile(process.env.AGENT_OBSERVER_EVENTS,JSON.stringify(row)+'\n');}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function bounded(p,label,ms=15000){let timer;try{return await Promise.race([p,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label+' deadline')),ms);})]);}finally{clearTimeout(timer);}}
async function waitFor(f,label,ms=15000){const until=Date.now()+ms;while(Date.now()<until){const v=await f();if(v)return v;await sleep(20);}throw Error(label+' deadline');}
async function optionalJSON(p){try{return JSON.parse(await readFile(p,'utf8'));}catch(e){if(e.code==='ENOENT'||e instanceof SyntaxError)return null;throw e;}}
async function childFiles(){const result=[];for(const name of await readdir(temp)){if(/^child-\d+\.json$/.test(name))result.push(await optionalJSON(join(temp,name)));}return result.filter(Boolean);}
async function snapshotRelease(phase){await writeFile(phasePath+'.next',JSON.stringify({...routing,phase})+'\n');await rename(phasePath+'.next',phasePath);await waitFor(async()=> (await optionalJSON(process.env.AGENT_OBSERVER_SNAPSHOT_RELEASE))?.phase===phase,'actual independent owner debug '+phase,5000);}
// Own both endpoints. Pass bytes unchanged and retain only public protocol
// message kinds, JobIDs, and times from the worker WebSocket.
const upstreamURL=new URL(workerURL);
assert.equal(upstreamURL.hostname,'127.0.0.1');
assert.equal(upstreamURL.protocol,'http:');
const wire=[],relayErrors=[],relaySockets=new Set();
const relay=net.createServer(downstream=>{
 const upstream=net.createConnection(Number(upstreamURL.port),'127.0.0.1');
 for(const socket of [downstream,upstream]){
  relaySockets.add(socket);
  socket.on('close',()=>relaySockets.delete(socket));
  socket.on('error',error=>{relayErrors.push(error.code??error.name);upstream.destroy();downstream.destroy();});
 }
 downstream.on('close',()=>upstream.destroy());
 upstream.on('close',()=>downstream.end());
 downstream.pipe(upstream);
 upstream.pipe(downstream);
 let request=Buffer.alloc(0);
 downstream.on('data',function detectWorker(chunk){
  request=Buffer.concat([request,chunk]);
  const end=request.indexOf('\r\n');
  if(end<0 && request.length<=8192)return;
  downstream.off('data',detectWorker);
  if(!/^GET \/agent(?:\?| )/.test(request.subarray(0,end).toString('ascii')))return;
  const receiver=new Receiver({isServer:false,maxPayload:4*1024*1024});
  receiver.on('error',error=>relayErrors.push(error.code??error.name));
  receiver.on('message',(data,binary)=>{
   try{
    const frame=binary?ServerMessage.fromBinary(data):ServerMessage.fromJsonString(data.toString());
    if(frame.message.case==='assignment'||frame.message.case==='termination'){
     wire.push({type:frame.message.case,job_id:frame.message.value.jobId??frame.message.value.job?.id,elapsed_ms:Date.now()-started});
    }
   }catch(error){relayErrors.push(error.name);}
  });
  let headers=Buffer.alloc(0),upgraded=false;
  upstream.on('data',chunk=>{
   if(upgraded){if(!receiver.destroyed)receiver.write(chunk);return;}
   headers=Buffer.concat([headers,chunk]);
   const boundary=headers.indexOf('\r\n\r\n');
   if(boundary<0 && headers.length<=32768)return;
   if(boundary<0||!/^HTTP\/1\.1 101 /.test(headers.subarray(0,boundary).toString('ascii'))){
    relayErrors.push('worker upgrade response missing');return;
   }
   if(/\r\nsec-websocket-extensions:/i.test(headers.subarray(0,boundary).toString('ascii'))){
    relayErrors.push('unsupported worker extension negotiated');return;
   }
   upgraded=true;
   const rest=headers.subarray(boundary+4);
   headers=Buffer.alloc(0);
   if(rest.length)receiver.write(rest);
  });
 });
});
relay.listen(0,'127.0.0.1');
await once(relay,'listening');
const relayURL='ws://127.0.0.1:'+relay.address().port+'/';
const warmStarted=join(temp,'prewarm-started.json');
const warmRelease=join(temp,'prewarm-release');
const entry=join(temp,'entry.mjs');
await writeFile(entry,`import {defineAgent} from ${JSON.stringify(sdkURL)};
import {writeFile,rename,access} from 'node:fs/promises';
export default defineAgent({prewarm:async()=>{
 const marker=${JSON.stringify(warmStarted)};
 await writeFile(marker+'.next',JSON.stringify({pid:process.pid,at_ms:Date.now()}));
 await rename(marker+'.next',marker);
 const deadline=Date.now()+18000;
 while(Date.now()<deadline){
  try{await access(${JSON.stringify(warmRelease)});return;}
  catch(error){if(error.code!=='ENOENT')throw error;}
  await new Promise(r=>setTimeout(r,20));
 }
 throw Error('owned prewarm gate deadline');
},entry:async ctx=>{
 if(ctx.isFakeJob)throw Error('fake job forbidden');
 const path=${JSON.stringify(temp)}+'/child-'+process.pid+'.json';
 let writes=Promise.resolve();
 const save=()=>{const bytes=JSON.stringify(row);writes=writes.then(async()=>{await writeFile(path+'.next',bytes);await rename(path+'.next',path);});return writes;};
 const row={pid:process.pid,job_id:ctx.job.id,fake_job:ctx.isFakeJob,entry:true,entry_at_ms:Date.now(),connected:false,shutdown:false};
 ctx.addShutdownCallback(async()=>{row.shutdown=true;row.shutdown_at_ms=Date.now();await save();});
 await save();
 try{await ctx.connect();row.connected=true;row.connected_at_ms=Date.now();row.room=ctx.room.name;row.participant_sid=ctx.room.localParticipant.sid;row.identity=ctx.room.localParticipant.identity;row.kind=ctx.room.localParticipant.kind;row.remote_participant_count=ctx.room.remoteParticipants.size;await save();}
 catch(error){row.connect_error=String(error?.message??error).slice(0,500);await save();throw error;}
}});\n`);
const service=new RoomServiceClient(url,key,secret),dispatches=new AgentDispatchClient(url,key,secret);
initializeLogger({pretty:false,level:'warn'});
let releaseAvailability,requestedResolve,requestedJob,requestOutcome,requestCount=0,requestHeldAt;const requestedJobs=[];
const availability=new Promise(r=>{releaseAvailability=r;});
const requestSeen=new Promise(r=>{requestedResolve=r;});
const worker=new AgentServer(new ServerOptions({agent:entry,wsURL:relayURL,apiKey:key,apiSecret:secret,agentName,serverType:0,production:false,simulation:false,host:'127.0.0.1',port:0,numIdleProcesses:0,maxRetry:0,loadFunc:async()=>0,loadThreshold:1,logLevel:'warn',shutdownProcessTimeout:10000,initializeProcessTimeout:22000,
 requestFunc:async req=>{requestCount++;assert.equal(req.job.type,0);assert.equal(req.room.name,roomName);assert.equal(req.agentName,agentName);requestedJob??={id:req.id,room:req.room.name,type:req.job.type};requestedJobs.push({id:req.id,room:req.room.name,type:req.job.type});requestHeldAt??=Date.now();await event('actual_request_hook',{job_id:req.id,job_type:req.job.type});requestedResolve();
  try{await bounded(availability,'application availability gate',8500);await event('availability_released_to_official_sdk',{held_ms:Date.now()-requestHeldAt,mode});if(mode==='accept')await req.accept('Delayed official Agent','delayed-official-agent','delayed SDK metadata',{'fixture':'delayed-official-sdk'});else await req.reject();requestOutcome={status:'resolved',mode};await event('official_request_hook_resolved',{mode});}
  catch(error){requestOutcome={status:'rejected',mode,error:cleanError(error)};await event('official_request_hook_rejected',{error:cleanError(error)});}
 }}));
const result={status:'RUNNING',started_at_ms:started,profile,availability:mode,scope:'unchanged official Agent SDK held cold prewarm across exact worker termination, observed on an owned byte-transparent loopback relay',observations:{actual_request_hook:false,actual_child_pids:[],delete_flush_scope:'public request bytes flushed only; no Rust handler/actor admission asserted'},timeline};
let running,routing,dispatch,deleteRequest,deletePromise,roomCreated=false,deleteResult;
function publicDelete(id){
 let finishResolve;const finished=new Promise(r=>{finishResolve=r;});
 const response=(async()=>{const token=new AccessToken(key,secret);token.addGrant({roomAdmin:true,room:roomName});const auth=await token.toJwt();const endpoint=new URL('/twirp/livekit.AgentDispatchService/DeleteDispatch',url);assert.equal(endpoint.protocol,'http:');const body=Buffer.from(JSON.stringify({dispatchId:id,room:roomName}));
  return await new Promise(resolveResponse=>{const req=http.request(endpoint,{method:'POST',headers:{Authorization:'Bearer '+auth,'Content-Type':'application/json','Content-Length':body.length,Connection:'close'}},res=>{const chunks=[];let size=0;res.on('data',chunk=>{size+=chunk.length;if(size>65536){req.destroy(Error('bounded DeleteDispatch response'));return;}chunks.push(chunk);});res.once('end',()=>{const raw=Buffer.concat(chunks);let decoded;try{decoded=JSON.parse(raw);}catch{}resolveResponse({status:res.statusCode,body_sha256:hash(raw),code:decoded?.code??null,elapsed_ms:Date.now()-started});});});deleteRequest=req;req.once('finish',()=>{finishResolve({elapsed_ms:Date.now()-started,bytes:body.length});});req.once('error',error=>resolveResponse({error:cleanError(error),elapsed_ms:Date.now()-started}));req.end(body);});})();
 return {finished,response};
}
try{
 const registered=new Promise(r=>worker.event.once('worker_registered',r));running=worker.run();let runningError;running.catch(e=>{runningError=e;});const workerID=await bounded(Promise.race([registered,running]),'actual SDK registration');assert.match(workerID,/^AW_/);
 const room=await service.createRoom({name:roomName,nodeId:process.env.AGENT_TARGET_NODE_ID});roomCreated=true;
 routing={owner_kind:process.env.AGENT_OWNER_KIND,worker_kind:process.env.AGENT_WORKER_KIND,owner_binary_sha256:process.env.AGENT_OWNER_SHA256,worker_binary_sha256:process.env.AGENT_WORKER_SHA256,room:roomName,room_sid:room.sid,agent_name:agentName,worker_id:workerID,target_node_id:process.env.AGENT_TARGET_NODE_ID,rest_url:url,worker_url:workerURL};
 await writeFile(phasePath+'.next',JSON.stringify({...routing,phase:'room_created_worker_registered'})+'\n');await rename(phasePath+'.next',phasePath);
 await waitFor(async()=> (await optionalJSON(process.env.AGENT_ROUTING_RELEASE))?.admit===true,'actual owner and worker subscription proof');
 const creation=dispatches.createDispatch(roomName,agentName,{metadata:'held original availability'});creation.catch(()=>{});
 await bounded(requestSeen,'actual official request hook');result.observations.actual_request_hook=true;
 dispatch=await bounded(creation,'public CreateDispatch response while availability is held',3000);assert.match(dispatch.id,/^AD_/);await event('create_dispatch_response',{dispatch_id:dispatch.id});
 assert.equal((await childFiles()).length,0,'availability hook must actually precede child entry');
 await snapshotRelease('request_held');
 const deleting=publicDelete(dispatch.id);deletePromise=deleting.response.then(x=>{deleteResult=x;return x;});
 const flushed=await bounded(deleting.finished,'actual complete public DeleteDispatch request flush',2000);await event('public_delete_request_finished',flushed);
 // Complete client flush proves only a public request in flight. It does
 // not prove server handler/actor admission or accepted-work cancellation.
 // Allow the actual request to run without presuming the original handler
 // responds before pending jobs complete. Request and response are separate.
 await sleep(100);result.observations.delete_response_before_release=deleteResult??null;await snapshotRelease('delete_request_inflight');
 assert(Date.now()-requestHeldAt<8000,'availability must remain within original ten-second budget');
 result.observations.delete_request_finished_before_availability_release=true;releaseAvailability();
 await bounded(deletePromise,'actual DeleteDispatch response after availability release',16000);
 const prewarm=await waitFor(()=>optionalJSON(warmStarted),'actual SDK child prewarm');
 assert.equal(requestCount,1,'exactly one actual worker accepted availability');
 const assignment=await waitFor(()=>wire.find(row=>row.type==='assignment'&&row.job_id===requestedJob.id),'exact worker assignment observed');
 assert.equal((await childFiles()).length,0,'job entry must await prewarm');
 assert(!worker.activeJobs.some(job=>job.job.id===requestedJob.id),'SDK must not expose the prewarming process as an active job');
 await event('cold_prewarm_held',{job_id:requestedJob.id,child_pid:prewarm.pid,child_marker_at_ms:prewarm.at_ms,assignment});
 const gateDeadline=Date.now()+7000;
 let earlyTermination;
 while(Date.now()<gateDeadline){
  earlyTermination=wire.find(row=>row.type==='termination'&&row.job_id===requestedJob.id);
  if(earlyTermination)break;
  await sleep(20);
 }
 if(earlyTermination){
  await sleep(100);
  assert(!worker.activeJobs.some(job=>job.job.id===requestedJob.id));
  assert.equal((await childFiles()).length,0,'early termination must precede SDK child entry');
 }
 result.observations.prewarm_gate={prewarm,assignment,early_termination:earlyTermination??null,active_jobs_at_release:worker.activeJobs.map(j=>j.job.id),release_elapsed_ms:Date.now()-started};
 await event('cold_prewarm_release',{early_termination:earlyTermination??null});
 await writeFile(warmRelease,'release');
 const snapshots=[];const until=Date.now()+5500;
 while(Date.now()<until){if(runningError)throw runningError;const children=await childFiles();for(const c of children){assert.equal(c.fake_job,false);assert(requestedJobs.some(j=>j.id===c.job_id),'real child must correspond to an actually observed original SDK request');assert(Number.isSafeInteger(c.pid)&&c.pid>0&&c.pid!==process.pid);if(c.connected){assert.equal(c.room,roomName);assert.equal(c.identity,'delayed-official-agent');assert.equal(c.kind,4);}}
  const participants=await service.listParticipants(roomName),list=await dispatches.listDispatch(roomName);snapshots.push({elapsed_ms:Date.now()-started,participants:participants.map(p=>({sid:p.sid,identity:p.identity,kind:p.kind,tracks:p.tracks.map(t=>t.sid)})),dispatches:list.map(d=>({id:d.id,jobs:(d.state?.jobs??[]).map(j=>({id:j.id,state:j.state?.status??null}))})),children,active_sdk_jobs:worker.activeJobs.map(j=>j.job.id)});await sleep(100);
 }
 await waitFor(()=>requestOutcome,'official request completion',8000);
 const last=snapshots.at(-1),children=await childFiles();
 result.observations.final_children_before_cleanup=children;
 const childAlive=children.map(c=>{try{process.kill(c.pid,0);return true;}catch(e){if(e.code==='ESRCH')return false;throw e;}});
 result.observations.snapshots=snapshots;result.observations.actual_child_pids=children.map(c=>c.pid);result.observations.request_outcome=requestOutcome;result.observations.delete_response=deleteResult;result.observations.request_count=requestCount;
 result.observations.requested_jobs=requestedJobs;
 result.observations.wire=wire;
 assert.deepEqual(relayErrors,[],'owned worker relay must forward cleanly');
 assert.equal(children.length,1,'one exact physical cold child must reach entry after release');
 assert.equal(children[0].pid,prewarm.pid,'entry must run in the prewarm-held child');
 result.outcome={schema:1,availability:mode,actual_request_count:requestCount,delete_http_status:deleteResult.status??null,delete_error_code:deleteResult.code??deleteResult.error?.code??null,request_resolution:requestOutcome.status,ever_child_entry:children.length>0,ever_child_connected:snapshots.some(s=>s.children.some(c=>c.connected)),agent_participant_observed:snapshots.some(s=>s.participants.some(p=>p.identity==='delayed-official-agent')),child_shutdown_before_cleanup:children.length?children.every(c=>c.shutdown):null,child_alive_before_cleanup:childAlive.some(Boolean),dispatch_present_before_cleanup:last.dispatches.some(d=>d.id===dispatch.id),agent_present_before_cleanup:last.participants.some(p=>p.identity==='delayed-official-agent')};
 await snapshotRelease('settled_before_cleanup');
 result.status='OBSERVED_COMPLETE';
}catch(error){result.status='FAIL';result.error=cleanError(error);}
finally{
 releaseAvailability();await writeFile(warmRelease,'release');const errors=[];
 // Cleanup is stricter than semantic observation: all owned real resources
 // must actually retire, even when the original Go exposes a late job.
 if(deleteRequest&&!deleteRequest.destroyed)deleteRequest.destroy();
 if(roomCreated)try{await bounded(service.deleteRoom(roomName),'actual cleanup DeleteRoom',15000);}catch(e){errors.push({step:'delete-room',error:cleanError(e)});}
 try{await bounded(worker.close(),'official SDK close',20000);if(running)await bounded(running,'official SDK run release',20000);}catch(e){errors.push({step:'sdk-close',error:cleanError(e)});}
 const children=await childFiles();result.observations.actual_child_pids=[...new Set([...(result.observations.actual_child_pids??[]),...children.map(c=>c.pid)])];result.observations.child_terminal_records=children;
 for(const pid of result.observations.actual_child_pids)try{await waitFor(()=>{try{process.kill(pid,0);return false;}catch(e){if(e.code==='ESRCH')return true;throw e;}},'actual official child ESRCH',10000);}catch(e){errors.push({step:'child-exit',pid,error:cleanError(e)});}
 if(roomCreated)try{assert(!(await service.listRooms([roomName])).some(r=>r.name===roomName));}catch(e){errors.push({step:'room-absence',error:cleanError(e)});}
 for(const socket of relaySockets)socket.destroy();
 await new Promise(resolve=>relay.close(resolve));
 result.observations.wire=wire;
 result.observations.relay_errors=relayErrors;
 result.routing=routing;result.cleanup_errors=errors;if(errors.length)result.status='FAIL';
 await rm(temp,{recursive:true,force:true});
 await writeFile(process.env.AGENT_SDK_EVIDENCE,JSON.stringify(result,null,2)+'\n');
}
if(result.status==='FAIL'){console.error(JSON.stringify({status:result.status,error:result.error,cleanup_errors:result.cleanup_errors}));process.exitCode=1;}else console.log(result.status+' actual original SDK delayed availability observer; runtime release verified');
