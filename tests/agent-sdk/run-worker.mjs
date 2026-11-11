// Uses the unchanged published AgentServer and RTC participant implementation.
// No hand-written worker protocol, token, job acceptance or fake room join.
import { pathToFileURL, fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { createRequire } from 'node:module';
import assert from 'node:assert/strict';
import net from 'node:net';
const sdkRoot = process.env.AGENT_SDK_ROOT;
assert(sdkRoot, 'AGENT_SDK_ROOT must point to the installed locked official package project');
const { AgentServer, ServerOptions, initializeLogger } = await import(pathToFileURL(resolve(sdkRoot, 'node_modules/@livekit/agents/dist/index.js')));
const interopRequire = createRequire(process.env.INTEROP_PROJECT ? resolve(process.env.INTEROP_PROJECT,'package.json') : new URL('../interop/package.json', import.meta.url));
const { RoomServiceClient, AgentDispatchClient, AccessToken } = interopRequire('livekit-server-sdk');
const url = process.env.LIVEKIT_URL;
assert(url, 'LIVEKIT_URL required');
const workerUrl=process.env.AGENT_WORKER_LIVEKIT_URL||url;
const targetNodeId=process.env.AGENT_TARGET_NODE_ID;
if(targetNodeId)assert.match(targetNodeId,/^ND_/);
const apiKey = process.env.LIVEKIT_API_KEY || 'devkey';
const apiSecret = process.env.LIVEKIT_API_SECRET || 'secret';
const jobType=Number(process.env.AGENT_JOB_TYPE??'0');
assert([0,1,2].includes(jobType),'AGENT_JOB_TYPE must be 0 ROOM, 1 PUBLISHER or 2 PARTICIPANT');
const targetIdentity='f05-official-human';
const roomName = `f05-sdk-${Date.now()}`;
const agentName = `f05-worker-${Date.now()}`;
const temp = await mkdtemp(join(tmpdir(), 'livekit-rs-f05-worker-'));
const readyPath = join(temp, 'participant-ready.json');
const markerPath = join(temp, 'participant-shutdown.json');
const cancelledTermination=process.env.AGENT_CANCEL_TERMINATION==='1';
const shutdownEnteredPath=join(temp,'shutdown-entered.json');
const shutdownReleasePath=join(temp,'shutdown-release.json');
const sdkEntryURL = pathToFileURL(resolve(sdkRoot, 'node_modules/@livekit/agents/dist/index.js')).href;
const sdkProtocolURL = pathToFileURL(resolve(sdkRoot, 'node_modules/@livekit/protocol/dist/index.mjs')).href;
const entryPath = join(temp, 'agent-entry.mjs');
await writeFile(entryPath, `import { defineAgent } from ${JSON.stringify(sdkEntryURL)};
import { Job } from ${JSON.stringify(sdkProtocolURL)};
import {writeFile,readFile} from 'node:fs/promises';
export default defineAgent({entry: async(ctx)=>{
 if(ctx.isFakeJob) throw new Error("official fixture must not use a fake/console job");
 ctx.addShutdownCallback(async()=>{
  if(${JSON.stringify(cancelledTermination)}) {
   await writeFile(${JSON.stringify(shutdownEnteredPath)},JSON.stringify({pid:process.pid,jobId:ctx.job.id,startedAt:Date.now()}));
   const deadline=Date.now()+5000;let released=false;
   while(Date.now()<deadline){try{released=JSON.parse(await readFile(${JSON.stringify(shutdownReleasePath)},'utf8')).release===true;}catch{}if(released)break;await new Promise(resolve=>setTimeout(resolve,10));}
   if(!released)throw new Error('actual application shutdown callback release deadline');
  }
  await writeFile(${JSON.stringify(markerPath)}, JSON.stringify({shutdown:true}));
 });
 await ctx.connect();
 // Locked SDK forks with Node's default JSON IPC: protobuf Message.toJSON()
 // serializes enum names. Decode with its unchanged official descriptor; the
 // request-side Job and REST client's Job remain numeric generated messages.
 const actualJob=Job.fromJson(ctx.job);
 await writeFile(${JSON.stringify(readyPath)}, JSON.stringify({identity:ctx.room.localParticipant.identity,room:ctx.room.name,kind:ctx.room.localParticipant.kind,fakeJob:ctx.isFakeJob,jobId:ctx.job.id,jobType:actualJob.type,jobTypeIpc:ctx.job.type??null,targetIdentity:ctx.job.participant?.identity??null,pid:process.pid}));
}});\n`);
const service = new RoomServiceClient(url, apiKey, apiSecret);
const dispatches = new AgentDispatchClient(url, apiKey, apiSecret);
initializeLogger({pretty:false,level:'warn'});
let accepted = 0;
let requestedJob;
const worker = new AgentServer(new ServerOptions({
 agent:entryPath, wsURL:workerUrl.replace(/^http/,'ws').replace(/\/?$/,'/'), apiKey, apiSecret,
 agentName, serverType:jobType, production:false, simulation:false, host:'127.0.0.1', port:0, numIdleProcesses:0,
 maxRetry:0, loadFunc:async()=>0, loadThreshold:1, logLevel:'warn',
 shutdownProcessTimeout:10000, initializeProcessTimeout:10000,
 requestFunc:async(req)=>{accepted++;requestedJob={id:req.id,room:req.room.name,agentName:req.agentName,type:req.job.type,targetIdentity:req.publisher?.identity??null};assert.equal(typeof requestedJob.type,'number','worker request is an actual generated Job');assert.equal(requestedJob.type,jobType);assert.equal(requestedJob.targetIdentity,jobType===0?null:targetIdentity);await req.accept('F05 SDK Agent','f05-official-agent','sdk metadata',{'fixture':'official-sdk'});},
}));
const result={status:'RUNNING',scope:'unmodified official @livekit/agents1.9.4 and rtc-node1.2.1 job and actual native RTC join/leave; no models/voice pipeline claimed',job_type:jobType,observations:{}};
let cancellationSocket;let running;let dispatch;let human;let rtc;let audioSource;let audioTrack;let audioTask;let audioError;let stopAudio=false;let capturedFrames=0;
const deadline=async(promise,label,ms=45000)=>{let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(`${label} deadline`)),ms);timer.unref();})]);}finally{clearTimeout(timer);}};
async function waitFor(check,label){const until=Date.now()+45000;let last;while(Date.now()<until){try{const value=await check();if(value)return value;}catch(e){last=e;}await new Promise(r=>setTimeout(r,100));}throw new Error(`${label} not observed${last?`: ${last.message}`:''}`);}
try{
 const registered=new Promise(resolve=>worker.event.once('worker_registered',resolve));
 running=worker.run();
 const backgroundFailure=new Promise((_,reject)=>running.catch(reject));
 const workerId=await deadline(Promise.race([registered,backgroundFailure]),'actual SDK registration');
 assert.match(workerId,/^AW_/);result.observations.registered=true;
 const created=await service.createRoom({name:roomName,...(targetNodeId?{nodeId:targetNodeId}:{})});
 if(process.env.AGENT_ROUTING_OBSERVER){
  result.routing={room:roomName,room_sid:created.sid,agent_name:agentName,worker_id:workerId,target_node_id:targetNodeId??null,rest_url:url,worker_url:workerUrl};
  await writeFile(process.env.AGENT_ROUTING_OBSERVER,JSON.stringify({...result.routing,phase:'room_created_worker_registered'})+'\n');
 }
 if(process.env.AGENT_ROUTING_RELEASE)await waitFor(async()=>JSON.parse(await readFile(process.env.AGENT_ROUTING_RELEASE,'utf8')).admit===true,'coordinator actual Redis owner/worker proof');
 dispatch=await dispatches.createDispatch(roomName,agentName,{metadata:'real SDK dispatch'});
 if(jobType!==0){
  rtc=await import(pathToFileURL(resolve(sdkRoot,'node_modules/@livekit/rtc-node/dist/index.js')));
  human=new rtc.Room();
  const token=new AccessToken(apiKey,apiSecret,{identity:targetIdentity,name:'F05 real human'});
  token.addGrant({roomJoin:true,room:roomName,canPublish:true,canSubscribe:true,canPublishData:true});
  await deadline(human.connect(url.replace(/^http/,'ws'),await token.toJwt()),'actual human RTC join');
  assert.equal(human.localParticipant.identity,targetIdentity);
  const actualHuman=await service.getParticipant(roomName,targetIdentity);
  assert.equal(actualHuman.identity,targetIdentity);assert.equal(actualHuman.kind,0);
  result.observations.actual_human_participant=true;
  if(jobType===1){
   audioSource=new rtc.AudioSource(48000,1,100);
   audioTrack=rtc.LocalAudioTrack.createAudioTrack('f05-actual-sine',audioSource);
   const publication=await deadline(human.localParticipant.publishTrack(audioTrack,new rtc.TrackPublishOptions({source:rtc.TrackSource.SOURCE_MICROPHONE,dtx:false,red:false})),'actual human audio publish');
   assert.match(publication.sid,/^TR_/);
   let sample=0;
   audioTask=(async()=>{while(!stopAudio){const pcm=new Int16Array(960);for(let i=0;i<pcm.length;i++)pcm[i]=Math.round(10000*Math.sin(2*Math.PI*440*(sample++)/48000));await audioSource.captureFrame(new rtc.AudioFrame(pcm,48000,1,960));capturedFrames++;await new Promise(resolve=>setTimeout(resolve,20));}})();
   audioTask.catch(error=>{audioError=error;});
  }
 }

 const ready=await waitFor(async()=>JSON.parse(await readFile(readyPath,'utf8')),'actual SDK room join');
 assert.equal(ready.room,roomName);assert.equal(ready.identity,'f05-official-agent');assert(Number.isSafeInteger(ready.pid)&&ready.pid>0&&ready.pid!==process.pid,"actual child PID required");assert.equal(ready.fakeJob,false);assert.equal(ready.jobId,requestedJob.id);assert.equal(ready.kind,4);assert.equal(ready.jobType,jobType);assert.equal(ready.targetIdentity,jobType===0?null:targetIdentity);
 const participant=await service.getParticipant(roomName,ready.identity);
 assert.equal(participant.identity,ready.identity);assert.equal(participant.kind,4);
 await waitFor(async()=>worker.activeJobs.some(info=>info.job.id===requestedJob.id),"actual SDK process owns the assigned job");
 assert.equal(participant.kind,4);
 assert.equal(participant.attributes['lk.agent_name'],agentName);
 assert.equal(participant.attributes.fixture,'official-sdk');
 assert.equal(participant.metadata,'sdk metadata');
 assert.equal(accepted,1);assert.equal(requestedJob.room,roomName);assert.equal(requestedJob.agentName,agentName);
 const retained=await waitFor(async()=>{const list=await dispatches.listDispatch(roomName);return list.find(d=>d.id===dispatch.id&&d.state?.jobs?.some(j=>j.id===requestedJob.id));},'actual accepted job retained in dispatch');
 assert.equal(retained.state.jobs.length,1);
 const retainedJob=retained.state.jobs[0];assert.equal(typeof retainedJob.type,'number','REST SDK decodes Job enum through generated fromJson');assert.equal(retainedJob.type,jobType);assert.equal(retainedJob.participant?.identity??null,jobType===0?null:targetIdentity);
 if(jobType===1){
  const sent=await waitFor(async()=>{if(audioError)throw audioError;const stats=await human.getRtcStats();let packets=0n;let bytes=0n;for(const stat of stats.publisherStats){if(stat.stats.case==='outboundRtp'){packets+=stat.stats.value.sent?.packetsSent??0n;bytes+=stat.stats.value.sent?.bytesSent??0n;}}return packets>0n&&bytes>0n?{packets:Number(packets),bytes:Number(bytes)}:undefined;},'actual human outbound audio RTP');
  assert(capturedFrames>0);result.observations.publisher_audio={captured_frames:capturedFrames,packets_sent:sent.packets,bytes_sent:sent.bytes};
 }
 result.observations.job_type_and_target_participant=true;
 result.observations.child_ipc_job_type=ready.jobTypeIpc;
 result.observations={...result.observations,accepted_jobs:accepted,actual_agent_participant:true,metadata_and_attributes:true,retained_job:true};
 if(process.env.AGENT_TERMINATION_RELEASE){
  await writeFile(process.env.AGENT_ROUTING_OBSERVER,JSON.stringify({...result.routing,phase:'job_running',job_id:requestedJob.id,actual_child_pid:ready.pid,agent_identity:ready.identity,agent_sid:participant.sid,human_identity:jobType===0?null:targetIdentity})+'\n');
  await waitFor(async()=>JSON.parse(await readFile(process.env.AGENT_TERMINATION_RELEASE,'utf8')).admit===true,'coordinator actual Redis terminate topic proof');
 }
 if(cancelledTermination){
  // Deliberately do not read the HTTP response: this is a real outstanding
  // caller connection, not a forged PSRPC request or an SDK modification.
  // Go may already have completed its fast catalog handler; record that scope.
  const endpoint=new URL(url);assert.equal(endpoint.protocol,'http:','owned fixture uses plain loopback HTTP');
  const token=new AccessToken(apiKey,apiSecret);token.addGrant({roomAdmin:true,room:roomName});
  const body=Buffer.from(JSON.stringify({dispatchId:dispatch.id,room:roomName}));
  const headers=Buffer.from(`POST /twirp/livekit.AgentDispatchService/DeleteDispatch HTTP/1.1\r\nHost: ${endpoint.host}\r\nAuthorization: Bearer ${await token.toJwt()}\r\nContent-Type: application/json\r\nContent-Length: ${body.length}\r\nConnection: keep-alive\r\n\r\n`);
  const socket=net.createConnection({host:endpoint.hostname,port:Number(endpoint.port||80)});cancellationSocket=socket;
  let socketError;let socketClosed=false;socket.on('error',e=>{socketError=e;});socket.once('close',()=>{socketClosed=true;});
  await deadline(new Promise((resolve,reject)=>{socket.once('connect',resolve);socket.once('error',reject);}), 'actual cancellation caller TCP connect',5000);
  // net.Socket begins paused when no data listener exists; do not drain bytes
  // or classify an unobserved response as an in-flight server handler.
  socket.pause();assert.equal(socket.readableFlowing,false);
  await deadline(new Promise((resolve,reject)=>socket.write(Buffer.concat([headers,body]),e=>e?reject(e):resolve())), 'actual complete signed HTTP request flushed',5000);
  const started=await waitFor(async()=>JSON.parse(await readFile(shutdownEnteredPath,'utf8')),'real official SDK shutdown callback entered');
  assert.equal(started.pid,ready.pid);assert.equal(started.jobId,requestedJob.id);
  process.kill(ready.pid,0);assert.equal(socketClosed,false,'caller socket unexpectedly closed before cancellation');
  let completedBeforeRelease=false;try{completedBeforeRelease=JSON.parse(await readFile(markerPath,'utf8')).shutdown===true;}catch{}
  assert.equal(completedBeforeRelease,false,'actual callback must still be blocked before cancel');
  assert(!(await dispatches.listDispatch(roomName)).some(d=>d.id===dispatch.id),'real dispatch deletion must already be published');
  assert.equal(typeof socket.resetAndDestroy,'function');socket.resetAndDestroy();
  await waitFor(async()=>socketClosed,'actual caller reset socket release');
  result.observations.cancelled_termination={request_bytes_flushed:headers.length+body.length,actual_callback_started:true,actual_child_alive_before_release:true,dispatch_deleted_before_reset:true,caller_tcp_reset:true,caller_socket_closed:socketClosed,response_deliberately_unread:true,server_handler_still_inflight:'not asserted; fixed Go catalog handler may already be complete',socket_error:socketError?.code??null};
  dispatch=undefined;
  await writeFile(shutdownReleasePath,JSON.stringify({release:true}));
 }else{await dispatches.deleteDispatch(dispatch.id,roomName);dispatch=undefined;}
 await waitFor(async()=>JSON.parse(await readFile(markerPath,'utf8')).shutdown,'actual SDK termination callback');
 await waitFor(async()=>!(await service.listParticipants(roomName)).some(p=>p.identity===ready.identity),'actual agent participant departure');
 // Locked SDK numIdleProcesses=0 retains closed executor.runningJob in its
 // activeJobs cache (ipc/proc_pool.ts non-watched launch branch). Prove actual
 // OS child exit rather than accepting that stale cache as a live process.
 await waitFor(async()=>{try{process.kill(ready.pid,0);return false;}catch(e){if(e.code==='ESRCH')return true;throw e;}},"actual SDK child process exited");
 result.observations.sdk_cached_job_after_actual_exit=worker.activeJobs.some(info=>info.job.id===requestedJob.id);
 result.observations.actual_child_exited=true;result.observations.actual_child_pid=ready.pid;
 assert.equal(accepted,1);
 result.observations.terminated=true;
 result.status='PASS';
}catch(e){result.status='FAIL';result.error=e.message;}
finally{
 const errors=[];
 if(cancelledTermination)try{await writeFile(shutdownReleasePath,JSON.stringify({release:true}));}catch(e){errors.push(`shutdown release cleanup: ${e.message}`);}
 if(cancellationSocket&&!cancellationSocket.destroyed)cancellationSocket.destroy();
 stopAudio=true;
 if(audioTask)try{await deadline(audioTask,'human audio producer exit',10000);}catch(e){errors.push(`audio producer: ${e.message}`);}
 if(audioError)errors.push(`audio capture: ${audioError.message}`);
 if(human)try{await deadline(human.disconnect(),'human RTC disconnect',10000);}catch(e){errors.push(`human cleanup: ${e.message}`);}
 if(audioTrack)try{await audioTrack.close(false);}catch(e){errors.push(`audio track cleanup: ${e.message}`);}
 if(audioSource)try{await audioSource.close();}catch(e){errors.push(`audio source cleanup: ${e.message}`);}

 if(dispatch)try{await dispatches.deleteDispatch(dispatch.id,roomName);}catch(e){errors.push(`dispatch cleanup: ${e.message}`);}
 try{await service.deleteRoom(roomName);}catch(e){if(!String(e.message).includes('does not exist'))errors.push(`room cleanup: ${e.message}`);}
 try{await deadline(worker.close(),'official SDK close',20000);if(running)await deadline(running,'official SDK run exit',20000);}catch(e){errors.push(e.message);}
 // The parent created its own native RTC runtime for the human participant.
 // Closing rooms/handles alone does not dispose its native FFI server threads.
 if(rtc)try{await deadline(rtc.dispose(),'parent native RTC dispose',20000);result.observations.parent_native_rtc_disposed=true;}catch(e){errors.push(`native RTC dispose: ${e.message}`);}
 try{await rm(temp,{recursive:true,force:true});}catch(e){errors.push(`temporary fixture cleanup: ${e.message}`);}
 result.cleanup_errors=errors;if(errors.length)result.status='FAIL';
 if(process.env.AGENT_SDK_EVIDENCE)await writeFile(process.env.AGENT_SDK_EVIDENCE,JSON.stringify(result,null,2)+'\n');
}
if(result.status!=='PASS'){console.error(JSON.stringify(result));process.exitCode=1;}else console.log('PASS unchanged AgentServer registration, assignment, native RTC participant, dispatch job, termination and cleanup');
