// Three actual unchanged browser SDK peers and real RoomService calls. NOT_RUN draft.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFile,writeFile,realpath} from 'node:fs/promises';
import {randomUUID,createHash} from 'node:crypto';
import {dirname,join,basename} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';import {promisify} from 'node:util';const exec=promisify(execFile);let sdkPins;
const here=dirname(fileURLToPath(import.meta.url));const require=createRequire(join(process.env.INTEROP_ROOT??join(here,'..'),'package.json'));
const {chromium}=require('playwright');const {RoomServiceClient,AccessToken}=require('livekit-server-sdk');
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
assert.ok(['true','false'].includes(process.env.EXPECT_REMOTE_UNMUTE),'EXPECT_REMOTE_UNMUTE must match actual server room.enable_remote_unmute config');
const remoteUnmute=process.env.EXPECT_REMOTE_UNMUTE==='true';
const single=process.env.SINGLE_PC!=='false';const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10});
const room=`three-api-${randomUUID()}`,identities=['controlled-publisher','controlled-b','controlled-c'];
let browser,server,created=false,resultOwned=false;const pages=[];const report={status:'RUNNING',scope:'original three-peer API/media/topology contract plus admin mute/unmute, actual Remove4 and live Delete5 with retained native resource closure; no BWE/cluster/internal-task/worker claim',remoteUnmute,single,requestedSingle:single,observations:[],cleanupErrors:[]};
let stage='setup';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,message,timeout=10000){const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await fn()){if(Date.now()>deadline)throw Error(message);return;}await sleep(50);}throw Error(message);}
const snapshot=p=>p.evaluate(()=>fixture.snapshot());
async function all(){return Promise.all(pages.map(snapshot));}
function eventCount(s,event,sid){return s.events.filter(e=>e.event===event&&e.sid===sid).length;}
// Read-only evidence from actual unchanged SDK native handles, not the
// requested option or a forced/created peer connection.
function assertTopology(observed,requestedSingle){
 assert.equal(observed.publisherNative,true,'real publisher RTCPeerConnection missing');
 assert.ok(observed.publisherId>0);
 if(requestedSingle){
  assert.equal(observed.mode,'publisher-only');
  assert.equal(observed.subscriberNative,false);
  assert.equal(observed.subscriberId,null);
  assert.equal(observed.uniqueNativeCount,1);
  assert.equal(observed.sameNative,false);
  assert.equal(observed.publisherState,'connected');
 }else{
  assert.ok(['subscriber-primary','publisher-primary'].includes(observed.mode));
  assert.equal(observed.subscriberNative,true);
  assert.ok(observed.subscriberId>0);
  assert.equal(observed.sameNative,false,'dual mode cannot alias one native PC');
  assert.notEqual(observed.publisherId,observed.subscriberId);
  assert.equal(observed.uniqueNativeCount,2);
  // The source primary mode decides which PC must be connected; an
  // unused secondary handle may remain new even though it is a real native PC.
  assert.equal(observed.mode==='subscriber-primary'?observed.subscriberState:observed.publisherState,'connected');
 }
}
function safeFailure(error){
 const frame=String(error?.stack??'').split('\n').find(line=>line.includes(here)&&/:\d+:\d+/.test(line));
 const location=frame?.match(/:(\d+):(\d+)\)?$/);
 const safe=value=>{if(value===null||typeof value==='boolean'||typeof value==='number')return value;if(typeof value==='string')return {type:'string',length:value.length,sha256:createHash('sha256').update(value).digest('hex')};if(Array.isArray(value))return {type:'array',length:value.length};return {type:typeof value,keys:value&&typeof value==='object'?Object.keys(value).sort():[]};};
 return {stage,name:error?.name||'Error',code:error?.code||null,line:location?Number(location[1]):null,column:location?Number(location[2]):null,operator:error?.operator??null,actual:safe(error?.actual),expected:safe(error?.expected)};
}
function topologyContract(s){const t=s.topology;return {mode:t.mode,publisherNative:t.publisherNative,subscriberNative:t.subscriberNative,sameNative:t.sameNative,uniqueNativeCount:t.uniqueNativeCount};}

async function packageIdentity(name,version){
 const entry=require.resolve(name);let parent=dirname(entry),file,bytes,data;
 for(;;){const candidate=join(parent,'package.json');try{const b=await readFile(candidate),d=JSON.parse(b);if(d.name===name){file=candidate;bytes=b;data=d;break;}}catch(error){if(error.code!=='ENOENT')throw error;}const next=dirname(parent);assert.notEqual(next,parent,'actual resolved package missing');parent=next;}
 assert.equal(data.version,version,'actual installed package version');const entrySha256=createHash('sha256').update(await readFile(entry)).digest('hex'),packageSha256=createHash('sha256').update(bytes).digest('hex');assert.equal(entrySha256,sdkPins.packages[name].entry_sha256,'unchanged pinned SDK actual entry');assert.equal(packageSha256,sdkPins.packages[name].package_sha256,'unchanged pinned SDK actual package');return {name,version:data.version,entry,package:file,entrySha256,packageSha256};
}
async function processProof(){
 const pid=Number(process.env.SERVER_PID);assert(Number.isSafeInteger(pid)&&pid>1);process.kill(pid,0);
 const binary=await realpath(process.env.SERVER_BIN),config=await realpath(process.env.SERVER_CONFIG);let argv,observed;
 try{observed=await realpath(`/proc/${pid}/exe`);assert.equal(observed,binary);argv=(await readFile(`/proc/${pid}/cmdline`)).toString().split('\0').filter(Boolean);}
 catch(error){if(error.code!=='ENOENT')throw error;const ps=(await exec('/bin/ps',['-p',String(pid),'-o','comm='],{timeout:10000,maxBuffer:65536})).stdout.trim();assert(ps);if(ps.startsWith('/'))assert.equal(await realpath(ps),binary);else assert.equal(ps,basename(binary));observed=ps;argv=(await exec('/bin/ps',['-p',String(pid),'-o','command='],{timeout:10000,maxBuffer:65536})).stdout.trim().split(/\s+/);}
 // Canonical owned launch only; never persist argv or possible credentials.
 const args=argv.slice(1);assert(args.length===2&&args[0]==='--config'&&args[1]===config||args.length===1&&args[0]==='--config='+config,'only actual canonical --config launch is accepted');
 const bytes=await readFile(config),origin=new URL(url);assert(['http:','https:','ws:','wss:'].includes(origin.protocol));assert(['127.0.0.1','localhost','[::1]'].includes(origin.hostname));
 const port=Number(origin.port||(['https:','wss:'].includes(origin.protocol)?443:80));assert.deepEqual(bytes.toString().split(/\r?\n/).filter(l=>l.startsWith('port:')),['port: '+port]);
 const rb=await readFile(process.env.SERVER_EXECUTION_RECEIPT),receipt=JSON.parse(rb),digest=b=>createHash('sha256').update(b).digest('hex');
 const configLines=bytes.toString().split(/\r?\n/);
 const section=name=>{const starts=configLines.flatMap((l,i)=>l===name+':'?[i]:[]);assert.equal(starts.length,1,'canonical unique config section '+name);const out=[];for(let i=starts[0]+1;i<configLines.length;i++){const line=configLines[i];if(line&&!line.startsWith(' ')&&!line.startsWith('#'))break;if(line.trim()&&!line.trim().startsWith('#'))out.push(line);}return out;};
 assert.deepEqual(section('api').filter(l=>l.trim().startsWith('enable_psrpc_for_get_list_participants:')),['  enable_psrpc_for_get_list_participants: true']);
 assert.deepEqual(section('room').filter(l=>l.trim().startsWith('enable_remote_unmute:')),['  enable_remote_unmute: '+remoteUnmute]);assert.equal(receipt.api_owner_get,true);assert.equal(receipt.room_enable_remote_unmute,remoteUnmute);
 const binarySha=digest(await readFile(binary)),configSha=digest(bytes);assert.equal(receipt.pid,pid);assert.equal(receipt.livekit_url,url);assert.equal(receipt.listener_port,port);assert.equal(receipt.binary_sha256,binarySha);assert.equal(receipt.startup_config_sha256,configSha);assert.equal(receipt.livekit_environment_cleared,true);assert.equal(receipt.no_config_overrides,true);
 const sockets=(await exec(process.env.LSOF||'lsof',['-a','-p',String(pid),'-iTCP:'+port,'-sTCP:LISTEN','-Fpn'],{timeout:10000,maxBuffer:65536})).stdout;assert(sockets.split('\n').includes('p'+pid));assert(sockets.split('\n').some(l=>l.startsWith('n')&&new RegExp(':'+port+'$').test(l)));
 return {pid,executableObserved:observed,binarySha256:binarySha,configSha256:configSha,receiptSha256:digest(rb),listenerSha256:digest(sockets),listenerPort:port};
}

try{
 assert.ok(process.env.RESULT,'explicit result path required');await writeFile(process.env.RESULT,'',{flag:'wx',mode:0o600});resultOwned=true;
 report.processBefore=await processProof();sdkPins=JSON.parse(await readFile(join(here,'sdk-pins.json')));assert.equal(createHash('sha256').update(await readFile(join(process.env.INTEROP_ROOT??join(here,'..'),'package-lock.json'))).digest('hex'),sdkPins.package_lock_sha256);report.packages=await Promise.all([packageIdentity('livekit-client','2.23.0'),packageIdentity('livekit-server-sdk','2.19.2'),packageIdentity('@livekit/protocol','1.52.1'),packageIdentity('playwright','1.64.1')]);
 const sdk=await readFile(require.resolve('livekit-client'));const html=await readFile(join(here,'client.html'));
 report.sdk={bundleSha256:createHash('sha256').update(sdk).digest('hex'),packageLockSha256:createHash('sha256').update(await readFile(join(process.env.INTEROP_ROOT??join(here,'..'),'package-lock.json'))).digest('hex')};
 server=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 stage='create-and-connect';await api.createRoom({name:room});created=true;
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
 const connections=[];
 for(const identity of identities){const page=await browser.newPage();pages.push(page);await page.goto(`http://127.0.0.1:${server.address().port}`);const token=new AccessToken(key,secret,{identity});token.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});connections.push(await page.evaluate(a=>fixture.connect(a),{url,token:await token.toJwt(),single}));}
 const {videoSid,audioSid}=await pages[0].evaluate(()=>fixture.publish());
 await until(async()=>{const s=await all();return s.every(p=>p.connection==='connected')&&s.slice(1).every(p=>p.frames>=30&&p.audible>=10&&p.width===640&&p.height===360);},'three actual peers/initial decoded video and audible audio missing',30000);
 report.observations.push({case:'initial_actual_media',receiverFrames:(await all()).slice(1).map(p=>p.frames),receiverAudibleSamples:(await all()).slice(1).map(p=>p.audible)});
 const codecWitness=await pages[0].evaluate(()=>fixture.outbound());assert(codecWitness.audioPackets>0);assert(codecWitness.sourceAudioCodecMimes.includes('audio/opus'),'actual publisher RTP codec must include Opus, not only an AudioTrack object');report.observations.push({case:'actual_source_opus_rtp',audioPacketsPositive:true,sourceAudioCodecMimes:codecWitness.sourceAudioCodecMimes});
 const initialTopology=(await all()).map(p=>p.topology);
 for(const t of initialTopology)assertTopology(t,single);
 report.observations.push({case:'actual_negotiated_pc_topology',requestedSingle:single,byOrdinal:(await all()).map(topologyContract)});
 // One sequential API producer only; no SDK peer publishes competing data.
 // Each API call returns before the next call; fixed source synchronously
 // forwards into each target's same reliable DataChannel. Do not infer
 // cross-producer or cross-transport order from this end-of-prefix witness.
 // Topic is metadata carried by the SDK callback, not a source topic ACL.
 const sends=[
  {seq:0,topic:'alpha',options:{destinationIdentities:[identities[1]]}},
  {seq:1,topic:'beta',options:{destinationIdentities:[identities[2]]}},
  {seq:2,topic:'alpha',options:{destinationIdentities:[identities[1],identities[2]]}},
  {seq:3,topic:'beta',options:{destinationIdentities:['controlled-absent']}},
  {seq:4,topic:'union',options:{destinationIdentities:[identities[2]],destinationSids:[connections[1].sid]}},
  {seq:5,topic:'broadcast-barrier',options:{}}
 ];
 for(const row of sends)await api.sendData(room,new TextEncoder().encode(JSON.stringify({seq:row.seq,topic:row.topic})),0,{...row.options,topic:row.topic});
 await until(async()=>(await all()).every(p=>p.data.some(d=>d.seq===5)),'reliable broadcast barrier missing');
 const expected=[[5],[0,2,4,5],[1,2,4,5]],rows=(await all()).map((p,i)=>{assert.deepEqual(p.data.map(d=>d.seq),expected[i]);for(const d of p.data){assert.equal(d.topic,sends[d.seq].topic);assert.equal(d.payloadTopic,d.topic);assert.equal(d.kind,0);assert.equal(d.fromServer,true);}return p.data.map(({seq,topic})=>({seq,topic}));});
 report.observations.push({case:'actual_api_data_target_and_topic',receivedByOrdinal:rows,allReliableBarrier:true});
 const before=(await all()),subscribedBefore=eventCount(before[1],'subscribed',videoSid);
 await api.updateSubscriptions(room,identities[1],[videoSid],false);
 await until(async()=>{const b=await snapshot(pages[1]);return eventCount(b,'unsubscribed',videoSid)>=1&&b.publications.some(p=>p.sid===videoSid&&!p.isSubscribed&&!p.hasTrack);},'API did not produce actual target unsubscribe/publication effect');
 // Retain the video element/callback: this fixture never stops it on unsubscribe.
 // Require a real stable decode window while the untargeted video and target audio advance.
 let stopped;
 await until(async()=>{const start=await all();await sleep(1000);const end=await all();if(end[1].frames===start[1].frames&&end[2].frames>=start[2].frames+10&&end[1].audible>=start[1].audible+5){stopped={before:start,after:end};return true;}return false;},'target decode did not stop independently of other video/audio');
 const publisher=await api.getParticipant(room,identities[0]);assert.ok(publisher.tracks.some(t=>t.sid===videoSid),'unsubscribe must not unpublish publisher track');
 report.observations.push({case:'actual_api_unsubscribe',targetVideoStable:true,otherVideoFrameDelta:stopped.after[2].frames-stopped.before[2].frames,targetAudioAudibleDelta:stopped.after[1].audible-stopped.before[1].audible,publisherTrackStillExists:true});
 const frameBase=(await all());await api.updateSubscriptions(room,identities[1],[videoSid],true);
 await until(async()=>{const s=await all();return eventCount(s[1],'subscribed',videoSid)>subscribedBefore&&s[1].publications.some(p=>p.sid===videoSid&&p.isSubscribed&&p.hasTrack)&&s[1].frames>=frameBase[1].frames+15&&s[2].frames>=frameBase[2].frames+15;},'real same-SID resubscription event and decoded media missing');
 report.observations.push({case:'actual_api_resubscribe',samePublisherTrack:true,targetNewDecodedFrames:(await snapshot(pages[1])).frames-frameBase[1].frames,otherNewDecodedFrames:(await snapshot(pages[2])).frames-frameBase[2].frames});
 const admin=new AccessToken(key,secret,{ttl:'10m'});admin.addGrant({roomAdmin:true,room});
 const rpc=await fetch(url.replace(/^ws/,'http')+'/twirp/livekit.RoomService/PerformRpc',{method:'POST',signal:AbortSignal.timeout(10000),headers:{authorization:'Bearer '+await admin.toJwt(),'content-type':'application/json'},body:JSON.stringify({room,destination_identity:identities[2],method:'api.three.echo',payload:'real-server-api',response_timeout_ms:10000})});
 const result=await rpc.json();assert.equal(rpc.status,200);assert.equal(result.payload,'three:real-server-api');assert.equal((await snapshot(pages[2])).rpcCalls,1);
 assert.equal((await snapshot(pages[0])).rpcCalls,0);assert.equal((await snapshot(pages[1])).rpcCalls,0);
 report.observations.push({case:'actual_perform_rpc',httpStatus:200,destinationCallbackCalls:1,otherCallbackCalls:0,payload:'three:real-server-api'});
 assert.ok((await all()).every(p=>p.errors===0&&p.connection==='connected'));
 for(const p of await all()){
  assertTopology(p.topology,single);
  // Record identity continuity without requiring a new no-reconnect policy.
  // Actual Single/dual count and same-object relation remain strict above.
 }

 report.topologyIdentityContinuity=(await all()).map((p,i)=>({publisherSame:p.topology.publisherId===initialTopology[i].publisherId,subscriberSame:p.topology.subscriberId===initialTopology[i].subscriberId}));

 stage='admin-mute-unmute';
 // Additive admin lifecycle begins only AFTER every original fixture assertion.
 // No direct track stop/mute, synthetic SDK events, or changes to SDK code.
 const localAudio=s=>s.localTracks.find(t=>t.sid===audioSid);
 const remoteAudio=s=>s.publications.find(t=>t.sid===audioSid);
 const permissionsBefore=(await all()).map(s=>s.localPermissions);
 const originalPublisher=await api.getParticipant(room,identities[0]);
 assert.equal(permissionsBefore[0]?.canPublish,true);assert.equal(permissionsBefore[0]?.canSubscribe,true);assert.equal(permissionsBefore[0]?.canPublishData,true);
 assert.ok(localAudio((await all())[0])&&!localAudio((await all())[0]).isMuted);
 assert((await all()).slice(1).every(v=>remoteAudio(v)?.isSubscribed===true&&remoteAudio(v)?.hasTrack===true),'actual audio subscription exists before admin mute');
 const restrictedToken=new AccessToken(key,secret,{identity:identities[1],ttl:'10m'});
 restrictedToken.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});
 const restrictedApi=new RoomServiceClient(url.replace(/^ws/,'http'),undefined,undefined,{token:await restrictedToken.toJwt(),requestTimeout:10,failover:false});
 let unauthorized;
 try{await restrictedApi.mutePublishedTrack(room,identities[0],audioSid,true);}catch(error){unauthorized=error;}
 assert.equal(unauthorized?.code,'unauthenticated');assert.equal(unauthorized?.message,'permissions denied');
 assert.equal((await api.getParticipant(room,identities[0])).tracks.find(t=>t.sid===audioSid)?.muted,false);
 report.observations.push({case:'admin_mute_requires_actual_room_admin',code:unauthorized.code,trackStillUnmuted:true});
 const muteBefore=await all();
 const mutedTrack=await api.mutePublishedTrack(room,identities[0],audioSid,true);
 assert.equal(mutedTrack.sid,audioSid);assert.equal(mutedTrack.muted,true);
 await until(async()=>{const s=await all();return s.every((v,i)=>eventCount(v,'muted',audioSid)>eventCount(muteBefore[i],'muted',audioSid))&&localAudio(s[0])?.isMuted===true&&localAudio(s[0])?.nativeEnabled===false&&s.slice(1).every(v=>remoteAudio(v)?.isMuted===true&&remoteAudio(v)?.isSubscribed===true&&remoteAudio(v)?.hasTrack===true);},'admin API must mute actual publisher track and both SDK remote publications');
 let mutedStable;
 await until(async()=>{const a=await all();await sleep(1000);const b=await all();if(b.slice(1).every((v,i)=>a[i+1].receiveAudioContext.state==='running'&&v.receiveAudioContext.state==='running'&&v.receiveAudioContext.currentTime>a[i+1].receiveAudioContext.currentTime&&v.audible===a[i+1].audible&&v.audioSamples>=a[i+1].audioSamples+5&&v.frames>=a[i+1].frames+10)&&b.every(v=>v.connection==='connected')){mutedStable={before:a,after:b};return true;}return false;},'actual audio must become inaudible while both receivers keep decoding original video');
 assert.deepEqual((await all()).map(s=>s.localPermissions),permissionsBefore);
 assert.deepEqual((await api.getParticipant(room,identities[0])).permission,originalPublisher.permission);
 assert.equal((await api.getParticipant(room,identities[0])).tracks.find(t=>t.sid===audioSid)?.muted,true);
 report.observations.push({case:'actual_admin_audio_mute',localNativeDisabled:true,localAndRemoteMutedEvents:true,bothRemotePublicationsMuted:true,audioStableByOrdinal:[1,2],receiverAudioContextsRunning:true,receiverAudioContextClocksAdvanced:true,videoFrameDeltas:mutedStable.after.slice(1).map((v,i)=>v.frames-mutedStable.before[i+1].frames),permissionsUnchanged:true});
 const unmuteBefore=await all();
 if(remoteUnmute){
  const unmutedTrack=await api.mutePublishedTrack(room,identities[0],audioSid,false);
  assert.equal(unmutedTrack.sid,audioSid);assert.equal(unmutedTrack.muted,false);
 }else{
  let denied;try{await api.mutePublishedTrack(room,identities[0],audioSid,false);}catch(error){denied=error;}
  assert.equal(denied?.code,'failed_precondition');assert.equal(denied?.message,'remote unmute not enabled');
  const still=await all();assert.equal(localAudio(still[0])?.isMuted,true);assert.equal(localAudio(still[0])?.nativeEnabled,false);
  assert.ok(still.slice(1).every(v=>remoteAudio(v)?.isMuted===true&&remoteAudio(v)?.isSubscribed===true&&remoteAudio(v)?.hasTrack===true));
  assert.equal((await api.getParticipant(room,identities[0])).tracks.find(t=>t.sid===audioSid)?.muted,true);
  // Explicit source-local user action is separate from forbidden remote API.
  // It proves publication permission remains usable; never report this as
  // successful remote unmute when the config disables it.
  await pages[0].evaluate(sid=>fixture.localUnmute(sid),audioSid);
  report.observations.push({case:'remote_unmute_disabled_denial',code:denied.code,message:denied.message,realMutedTrackRetained:true,subsequentRecovery:'explicit-local-sdk-user-unmute'});
 }
 await until(async()=>{const s=await all();return s.every((v,i)=>eventCount(v,'unmuted',audioSid)>eventCount(unmuteBefore[i],'unmuted',audioSid))&&localAudio(s[0])?.isMuted===false&&localAudio(s[0])?.nativeEnabled===true&&s.slice(1).every((v,i)=>remoteAudio(v)?.isMuted===false&&remoteAudio(v)?.isSubscribed===true&&remoteAudio(v)?.hasTrack===true&&v.audible>=unmuteBefore[i+1].audible+5&&v.frames>=unmuteBefore[i+1].frames+10);},'same audio SID must unmute through actual source-permitted API/user action and real audible samples');
 assert.deepEqual((await all()).map(s=>s.localPermissions),permissionsBefore);
 assert.deepEqual((await api.getParticipant(room,identities[0])).permission,originalPublisher.permission);
 report.observations.push({case:remoteUnmute?'actual_admin_remote_unmute_enabled':'actual_local_unmute_after_api_denial',sameTrackSid:true,localNativeEnabled:true,allSdkUnmutedEvents:true,bothReceiverAudioAudible:true,permissionsUnchanged:true});
 stage='remove-participant';
 // Remove receiverB; publisherA and receiverC must keep actual source media.
 const beforeRemove=await all();const beforeOut=await pages[0].evaluate(()=>fixture.outbound());
 await api.removeParticipant(room,identities[1]);
 await until(async()=>{const s=await all();return s[1].connection==='disconnected'&&s[1].events.some(e=>e.event==='room-disconnected'&&e.reason===4)&&[0,2].every(i=>s[i].connection==='connected'&&s[i].events.filter(e=>e.event==='participant-disconnected'&&e.identity===identities[1]).length>beforeRemove[i].events.filter(e=>e.event==='participant-disconnected'&&e.identity===identities[1]).length);},'RemoveParticipant must yield actual SDK reason4 and observer participant leave');
 assert.deepEqual((await snapshot(pages[1])).events.filter(e=>e.event==='room-disconnected').map(e=>e.reason),[4]);
 const remaining=await api.listParticipants(room);assert.deepEqual(remaining.map(p=>p.identity).sort(),[identities[0],identities[2]].sort());
 await assert.rejects(api.getParticipant(room,identities[1]),error=>error.code==='not_found');
 await api.sendData(room,new TextEncoder().encode(JSON.stringify({seq:6,topic:'after-remove'})),0,{topic:'after-remove'});
 await until(async()=>{const s=await all();return [0,2].every(i=>s[i].data.some(d=>d.seq===6))&&s[2].frames>=beforeRemove[2].frames+15&&s[2].audible>=beforeRemove[2].audible+5;},'other two peers must retain actual media and reliable API data after removal');
 const afterRemove=await all();const afterOut=await pages[0].evaluate(()=>fixture.outbound());
 assert.equal(afterRemove[1].data.some(d=>d.seq===6),false);
 assert.ok(afterOut.audioPackets>beforeOut.audioPackets&&afterOut.videoPackets>beforeOut.videoPackets&&afterOut.videoFrames>beforeOut.videoFrames,'actual unchanged publisher native RTC source must keep advancing');
 assert.ok([0,2].every(i=>afterRemove[i].errors===0&&afterRemove[i].connection==='connected'));
 for(const i of [0,2])assertTopology(afterRemove[i].topology,single);
 report.observations.push({case:'actual_remove_participant_reason4',removedOrdinal:1,sdkDisconnectReasons:[4],otherTwoConnected:true,otherParticipantLeaveEvents:true,actualListRemainingOrdinals:[0,2],removedGetCode:'not_found',survivorVideoFrameDelta:afterRemove[2].frames-beforeRemove[2].frames,survivorAudioAudibleDelta:afterRemove[2].audible-beforeRemove[2].audible,publisherAudioPacketDelta:afterOut.audioPackets-beforeOut.audioPackets,publisherVideoPacketDelta:afterOut.videoPackets-beforeOut.videoPackets,publisherVideoFrameDelta:afterOut.videoFrames-beforeOut.videoFrames,reliableDataAfterRemoval:true,removedReceivedNoLaterData:true});
 stage='delete-live-room';
 // Live-room DeleteRoom is a business action, BEFORE fixture manual cleanup.
 // B was removed with reason4; all still-connected A/C must receive reason5.
 const resourceBefore=await all();
 for(const i of [0,2]){const r=resourceBefore[i].resources;assert(r.pcStates.length>0);assert(r.channelStates.some(s=>s==='open'));assert(r.signalStates.some(s=>s===1));assert.equal(r.engineClosed,false);}
 assert.equal(resourceBefore[0].resources.sdkStopsLocalTracks,true,'fixed SDK default local-track cleanup profile required');assert.equal(resourceBefore[0].resources.sourceTracks.length,2);assert(resourceBefore[0].resources.sourceTracks.every(t=>t.readyState==='live'));
 const liveBefore=await api.listRooms([room]);assert.equal(liveBefore.length,1);assert(liveBefore[0].sid);const oldRoomSid=liveBefore[0].sid;
 await api.deleteRoom(room);
 await until(async()=>{const s=await all();return [0,2].every(i=>s[i].connection==='disconnected'&&s[i].events.some(e=>e.event==='room-disconnected'&&e.reason===5));},'DeleteRoom must terminate every remaining actual SDK peer with source reason5');
 await until(async()=>{const s=await all();return [0,2].every(i=>s[i].resources.engineClosed&&s[i].resources.pcStates.length>0&&s[i].resources.pcStates.every(p=>p.connection==='closed'&&p.signaling==='closed')&&s[i].resources.channelStates.length>0&&s[i].resources.channelStates.every(v=>v==='closed')&&s[i].resources.signalStates.length>0&&s[i].resources.signalStates.every(v=>v===3));},'server Leave must cause original SDK actual retained PC/DC/WS resources to close before fixture cleanup');
 const deleted=await all();for(const i of [0,2])assert.deepEqual(deleted[i].events.filter(e=>e.event==='room-disconnected').map(e=>e.reason),[5]);assert.deepEqual(deleted[1].events.filter(e=>e.event==='room-disconnected').map(e=>e.reason),[4]);
 // SDK stopLocalTrackOnUnpublish default is true in this fixed profile. This is
 // SDK teardown of its published tracks, NOT a server promise about arbitrary
 // external oscillators/contexts/tracks retained independently by an app.
 assert(deleted[0].resources.sourceTracks.every(t=>t.readyState==='ended'));
 await until(async()=>(await api.listRooms([room])).length===0,'actual room catalog must lose deleted live room');
 // Owner RPC topics can already be retired here; no fabricated NOT_FOUND
 // assumption for a Get/List request sent after handler retirement.
 created=false;
 report.observations.push({case:'actual_live_delete_room',connectedOrdinalsBefore:[0,2],allRemainingDisconnectReasons:[5,5],previouslyRemovedStillReason4:true,actualRoomAbsent:true,roomCatalogFilteredEmpty:true,retainedNativePCsClosed:true,retainedDataChannelsClosed:true,retainedSignalSocketsClosed:true,sdkPublishedTracksEnded:true});
 const recreated=await api.createRoom({name:room});created=true;assert(recreated.sid);assert.notEqual(recreated.sid,oldRoomSid,'same room name must create a distinct fresh generation after live deletion');await api.deleteRoom(room);await until(async()=>(await api.listRooms([room])).length===0,'recreated empty room cleanup must complete');created=false;
 report.observations.push({case:'deleted_name_new_room_sid',freshSid:true,ownedRecreatedRoomDeleted:true});
 report.processAfter=await processProof();assert.equal(report.processAfter.binarySha256,report.processBefore.binarySha256);assert.equal(report.processAfter.configSha256,report.processBefore.configSha256);
 const contract=report.observations.map(o=>['actual_api_data_target_and_topic','actual_negotiated_pc_topology'].includes(o.case)?o:{case:o.case,passed:true});report.contract=contract;
 if(process.env.REFERENCE_RESULTS){const reference=JSON.parse(await readFile(process.env.REFERENCE_RESULTS,'utf8'));assert.equal(reference.status,'REFERENCE_CAPTURED');assert.equal(reference.single,single);assert.equal(reference.remoteUnmute,remoteUnmute);assert.deepEqual(contract,reference.contract);report.status='PASS';}else report.status='REFERENCE_CAPTURED';
}catch(error){report.status='FAIL';report.failure=safeFailure(error);process.exitCode=1;}
finally{
 for(const page of pages){try{await page.evaluate(()=>fixture.close());}catch{report.cleanupErrors.push('disconnect-owned-peer');}}
 if(browser){try{await browser.close();}catch{report.cleanupErrors.push('close-owned-browser');}}
 if(created){try{await api.deleteRoom(room);}catch{report.cleanupErrors.push('delete-owned-room');}}
 if(server){try{await new Promise((resolve,reject)=>server.close(e=>e?reject(e):resolve()));}catch{report.cleanupErrors.push('close-owned-fixture-http');}}
 if(report.cleanupErrors.length){report.status='FAIL';process.exitCode=1;}
 if(resultOwned)await writeFile(process.env.RESULT,JSON.stringify(report,null,2)+'\n',{mode:0o600});
}
