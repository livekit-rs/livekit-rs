// Three actual unchanged browser SDK peers and real RoomService calls. NOT_RUN draft.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFile,writeFile} from 'node:fs/promises';
import {randomUUID,createHash} from 'node:crypto';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url));
const interop=process.env.INTEROP_PACKAGE||join(here,'../package.json');const require=createRequire(interop);
import {sdkIdentity} from './sdk-identity.mjs';
const {chromium}=require('playwright');
const serverSDK=await sdkIdentity(interop,'livekit-server-sdk','2.19.2',true);
const clientSDK=await sdkIdentity(interop,'livekit-client','2.23.0',false);
const protocolSDK=await sdkIdentity(interop,'@livekit/protocol','1.52.1',true);
const {SignalRequest}=protocolSDK.module.default??protocolSDK.module;
const {RoomServiceClient,AccessToken}=serverSDK.module.default??serverSDK.module;
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const profile=process.env.SETTINGS_PROFILE;assert.ok(['cleanup','ttl','refresh','subscribe-clears'].includes(profile));
assert.ok(['false','true'].includes(process.env.RECONNECT_ON_SUBSCRIPTION_ERROR),'declare actual separately configured server reconnect flag');
const single=process.env.SINGLE_PC!=='false';const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10});
const room=`three-api-${randomUUID()}`,identities=['controlled-publisher','controlled-b','controlled-c'];
let browser,server,created=false;let campaignStage='original-baseline';const pages=[];const report={status:'RUNNING',scope:'original31 API/media assertions retained; actual SDK settings plus RoomService unsubscribe/subscribe and per-SID decoded media; original60s window/cadence not shortened',profile,reconnectFlag:process.env.RECONNECT_ON_SUBSCRIPTION_ERROR,sdkIdentity:{server:serverSDK.identity,client:clientSDK.identity,protocol:protocolSDK.identity},single,requestedSingle:single,observations:[],cleanupErrors:[]};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,message,timeout=10000){const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await fn()){if(Date.now()>deadline)throw Error(message);return;}await sleep(50);}throw Error(message);}
const snapshot=p=>p.evaluate(()=>fixture.snapshot());
async function all(){return Promise.all(pages.map(snapshot));}

// Read-only actual outgoing SDK wire observation, not a hand-built signal.
const signalWire=new Map(),ownerBarriers=[];
function watchSignal(page){
 const trace={frames:[],errors:[]};signalWire.set(page,trace);
 page.on('websocket',socket=>{
  const observed=new URL(socket.url()),expected=new URL(url);
  if(observed.origin!==expected.origin)return;
  if(!/^\/rtc(?:\/v1)?$/.test(observed.pathname)){trace.errors.push('unexpected-signal-path');return;}
  const socketOrdinal=(trace.nextSocket=(trace.nextSocket??0)+1);
  socket.on('framesent',event=>{
   try{
    const request=typeof event.payload==='string'?SignalRequest.fromJsonString(event.payload):SignalRequest.fromBinary(new Uint8Array(event.payload));
    const row={socket:socketOrdinal,index:trace.frames.length,kind:request.message.case};
    if(row.kind==='trackSetting'){const value=request.message.value;row.settings={trackSids:[...value.trackSids],disabled:value.disabled};}
    if(row.kind==='updateMetadata'){const value=request.message.value;row.metadata={requestId:value.requestId,metadata:value.metadata,name:value.name,attributes:{...value.attributes}};}
    trace.frames.push(row);
   }catch{trace.errors.push('signal-frame-decode-failed');}
  });
 });
}
async function settingsProcessed(page,wireStart,sid,disabled){
 // setMetadata can return a cached local noop; the unchanged SignalClient method
 // returns only an ID. Require the real owner response separately below.
 const sent=await page.evaluate(()=>fixture.settingsOwnerBarrier());
 assert.ok(Number.isInteger(sent.requestId)&&sent.requestId>0,'real SDK allocated metadata request ID');
 let actual;
 await until(async()=>{
  const trace=signalWire.get(page);assert.equal(trace.errors.length,0,'all original outgoing frames decoded');
  const responses=(await snapshot(page)).ownerResponses.filter(r=>r.requestId===sent.requestId);
  if(!responses.length)return false;
  assert.equal(responses.length,1,'one real owner response per SDK request');
  const response=responses[0];assert.equal(response.reason,2);assert.equal(response.message,'does not have permission to update own metadata');
  assert.equal(response.requestCase,'updateMetadata');
  const echo={requestId:sent.requestId,metadata:'',name:'',attributes:{}};assert.deepEqual(response.echo,echo);
  const metadata=trace.frames.filter(f=>f.kind==='updateMetadata'&&f.metadata.requestId===sent.requestId);
  if(!metadata.length)return false;
  assert.equal(metadata.length,1);assert.deepEqual(metadata[0].metadata,echo);
  const settings=trace.frames.slice(wireStart).filter(f=>f.kind==='trackSetting'&&f.settings.trackSids.includes(sid));
  assert.equal(settings.length,1,'exact original setting wire frame before barrier');
  assert.deepEqual(settings[0].settings,{trackSids:[sid],disabled});
  assert.equal(settings[0].socket,metadata[0].socket,'same actual signal socket');
  assert.ok(settings[0].index<metadata[0].index,'setting precedes owner RR request on actual wire');
  actual={ownerProcessedBy:Date.now(),requestId:sent.requestId,wireSettingIndex:settings[0].index,wireBarrierIndex:metadata[0].index};return true;
 },'actual owner processed prior SDK settings frame and exact RR echo',10000);
 ownerBarriers.push({disabled,sameSocket:true,orderedWire:true,actualOwnerResponse:true});
 return actual;
}

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
function topologyContract(s){const t=s.topology;return {mode:t.mode,publisherNative:t.publisherNative,subscriberNative:t.subscriberNative,sameNative:t.sameNative,uniqueNativeCount:t.uniqueNativeCount};}

try{
 assert.ok(process.env.RESULT,'explicit result path required');
 assert.ok(process.env.SERVER_CONFIG_FILE&&process.env.SERVER_BINARY,'actual coordinator server input paths required');
 report.serverInput={configSha256:createHash('sha256').update(await readFile(process.env.SERVER_CONFIG_FILE)).digest('hex'),binarySha256:createHash('sha256').update(await readFile(process.env.SERVER_BINARY)).digest('hex'),scope:'actual supplied files; coordinator must separately attest running process uses these inputs'};
 const sdk=clientSDK.entryBytes;const html=await readFile(join(here,'client.html'));
 assert.equal(typeof AccessToken,'function');assert.equal(typeof RoomServiceClient,'function');
 report.sdk={bundleSha256:createHash('sha256').update(sdk).digest('hex'),packageLockSha256:createHash('sha256').update(await readFile(join(dirname(interop),'package-lock.json'))).digest('hex')};
 server=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 await api.createRoom({name:room});created=true;
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
 const connections=[];
 for(const identity of identities){const page=await browser.newPage();pages.push(page);watchSignal(page);await page.goto(`http://127.0.0.1:${server.address().port}`);const token=new AccessToken(key,secret,{identity});token.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});connections.push(await page.evaluate(a=>fixture.connect(a),{url,token:await token.toJwt(),single}));}
 const {videoSid}=await pages[0].evaluate(()=>fixture.publish());
 await until(async()=>{const s=await all();return s.every(p=>p.connection==='connected')&&s.slice(1).every(p=>p.frames>=30&&p.audible>=10&&p.width===640&&p.height===360);},'three actual peers/initial decoded video and audible audio missing',30000);
 report.observations.push({case:'initial_actual_media',receiverFrames:(await all()).slice(1).map(p=>p.frames),receiverAudibleSamples:(await all()).slice(1).map(p=>p.audible)});
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

 // Original media/API business assertions above are preserved; original reference-contract assertions remain below.
 for(const page of pages)assert.equal((await snapshot(page)).sdkVersion,clientSDK.identity.version,'actual served bundle runtime version');

 campaignStage='settings-lifetime-'+profile;
 const lifecycleBase=(await all()).map(p=>p.lifecycle.length);
 async function alive(){const states=await all();for(let i=0;i<states.length;i++){assert.equal(states[i].connection,'connected');assert.equal(states[i].errors,0);assert.equal(states[i].lifecycle.length,lifecycleBase[i]);assertTopology(states[i].topology,single);}}
 const sidFrames=s=>s.bySid[videoSid]?.frames??0;
 async function pauseProof(){
  await until(async()=>{await alive();const first=await all();await sleep(1000);const last=await all();return sidFrames(last[1])===sidFrames(first[1])&&last[2].frames>=first[2].frames+10&&last[1].audible>=first[1].audible+5;},'same real track stopped while unrelated video and target audio continue',10000);
 }
 async function remove(){const previous=eventCount(await snapshot(pages[1]),'unsubscribed',videoSid);await api.updateSubscriptions(room,identities[1],[videoSid],false);await until(async()=>{await alive();const b=await snapshot(pages[1]);return eventCount(b,'unsubscribed',videoSid)>previous&&b.publications.some(p=>p.sid===videoSid&&!p.hasTrack&&!p.isSubscribed);},'actual API removal receipt',10000);}
 async function disable(){const wireStart=signalWire.get(pages[1]).frames.length;const window=await pages[1].evaluate(sid=>fixture.setDisabled(sid,true),videoSid);assert.equal(window.isDesired,true);assert.equal(window.isEnabled,false);const receipt=await settingsProcessed(pages[1],wireStart,videoSid,true);return {...window,...receipt};}
 async function enable(){const wireStart=signalWire.get(pages[1]).frames.length;const window=await pages[1].evaluate(sid=>fixture.setDisabled(sid,false),videoSid);assert.equal(window.isDesired,true);assert.equal(window.isEnabled,true);const receipt=await settingsProcessed(pages[1],wireStart,videoSid,false);return {...window,...receipt};}
 async function restoreMedia(){const before=await snapshot(pages[1]);await api.updateSubscriptions(room,identities[1],[videoSid],true);await until(async()=>{await alive();const b=await snapshot(pages[1]);return b.publications.some(p=>p.sid===videoSid&&p.hasTrack&&p.isSubscribed)&&sidFrames(b)>=sidFrames(before)+30&&b.bySid[videoSid].width===640&&b.bySid[videoSid].height===360&&b.audible>=before.audible+10;},'same track default enabled settings produce30 fresh decoded640x360 frames and10 new audible samples',30000);}
 async function through(deadline){while(Date.now()<deadline){await alive();await sleep(Math.min(200,Math.max(1,deadline-Date.now())));}}
 let windows=[],expectedUpdates=1;
 if(profile==='cleanup'){
  windows.push(await disable());await pauseProof();await remove();await restoreMedia();
 }else{
  await remove();windows.push(await disable());
  if(profile==='ttl'){
   // Exact public producer window; original60s retained plus source3s cadence allowance.
   await through(windows[0].ownerProcessedBy+65000);await restoreMedia();
  }else if(profile==='refresh'){
   await through(windows[0].ownerProcessedBy+40000);
   await enable();windows.push(await disable());expectedUpdates=3;
   await through(windows[0].ownerProcessedBy+65000);
   const previous=eventCount(await snapshot(pages[1]),'subscribed',videoSid);
   await api.updateSubscriptions(room,identities[1],[videoSid],true);
   await until(async()=>{const b=await snapshot(pages[1]);return eventCount(b,'subscribed',videoSid)>previous&&b.publications.some(p=>p.sid===videoSid&&p.hasTrack);},'real newly bound downtrack with refreshed settings');
   await pauseProof();
   await enable();expectedUpdates++;
   await restoreMedia();
  }else{
   const previous=eventCount(await snapshot(pages[1]),'subscribed',videoSid);
   await api.updateSubscriptions(room,identities[1],[videoSid],true);
   await until(async()=>eventCount(await snapshot(pages[1]),'subscribed',videoSid)>previous,'unexpired settings admission actually binds');await pauseProof();
   await remove();await restoreMedia();
  }
 }
 const final=await snapshot(pages[1]);assert.equal(final.settingsUpdates.length,expectedUpdates,'automatic or repeated SDK settings would change the source keepUntil premise; preserve and fail, do not reset timing');
 assert.ok((await api.getParticipant(room,identities[0])).tracks.some(t=>t.sid===videoSid));await alive();
 report.settingsContract={profile,reconnectFlag:report.reconnectFlag,sameSID:true,freshMedia640x360:true,expectedSettingsProducerEvents:expectedUpdates};
 report.observations.push({case:'actual_settings_lifetime_'+profile,source:'public SDK.setEnabled + actual RoomService API',windows:windows.map(w=>({durationMs:w.after-w.before})),settingsEventCount:final.settingsUpdates.length,sameSID:true,actualOwnerBarriers:ownerBarriers});
 const contract=report.observations.map(o=>['actual_api_data_target_and_topic','actual_negotiated_pc_topology'].includes(o.case)?o:{case:o.case,passed:true});report.contract=contract;
 if(process.env.REFERENCE_RESULTS){const reference=JSON.parse(await readFile(process.env.REFERENCE_RESULTS,'utf8'));assert.equal(reference.status,'REFERENCE_CAPTURED');assert.equal(reference.profile,profile);assert.equal(reference.reconnectFlag,report.reconnectFlag);assert.equal(reference.single,single);assert.deepEqual(report.settingsContract,reference.settingsContract);assert.deepEqual(contract,reference.contract);report.status='PASS';}else report.status='REFERENCE_CAPTURED';
}catch(error){report.status='FAIL';report.failure={name:error?.name||'Error',code:error?.code||null,stage:campaignStage};process.exitCode=1;report.failureSnapshots=await Promise.all(pages.map(page=>snapshot(page).then(p=>({connection:p.connection,frames:p.frames,audible:p.audible,width:p.width,height:p.height,errors:p.errors,topology:p.topology,missingErrors:p.missingErrors.map(e=>({error:e.error,at:e.at})),lifecycle:p.lifecycle,bySidFrameCounts:Object.values(p.bySid).map(t=>({frames:t.frames,width:t.width,height:t.height}))})).catch(()=>({unavailable:true}))));}
finally{
 for(const page of pages){try{await page.evaluate(()=>fixture.close());}catch{report.cleanupErrors.push('disconnect-owned-peer');}}
 if(browser){try{await browser.close();}catch{report.cleanupErrors.push('close-owned-browser');}}
 if(created){try{await api.deleteRoom(room);}catch{report.cleanupErrors.push('delete-owned-room');}}
 if(server){try{await new Promise((resolve,reject)=>server.close(e=>e?reject(e):resolve()));}catch{report.cleanupErrors.push('close-owned-fixture-http');}}
 if(report.cleanupErrors.length){report.status='FAIL';process.exitCode=1;}
 if(process.env.RESULT)await writeFile(process.env.RESULT,JSON.stringify(report,null,2)+'\n',{mode:0o600});
}
