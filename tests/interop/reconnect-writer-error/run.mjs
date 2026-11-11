import assert from 'node:assert/strict';
import {topologyValid,baseline,newMedia,recovered,assertDeadline,callBeforeDeadline,pollUntil} from './criteria.mjs';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomUUID} from 'node:crypto';
const here=dirname(fileURLToPath(import.meta.url));
const root=resolve(process.env.INTEROP_ROOT??join(process.cwd(),'tests/interop'));
const require=createRequire(join(root,'package.json'));
const {chromium}=require('playwright');
const {AccessToken,RoomServiceClient,DataPacket_Kind}=require('livekit-server-sdk');
const url=process.env.LIVEKIT_URL??'ws://127.0.0.1:7880';
const key=process.env.LIVEKIT_API_KEY??'devkey',secret=process.env.LIVEKIT_API_SECRET??'secret';
assert.ok(['true','false'].includes(process.env.RECONNECT_FLAG),'RECONNECT_FLAG must match the genuine prestarted server YAML');
const enabled=process.env.RECONNECT_FLAG==='true';
const modes=process.env.SINGLE_PC==='true'?[true]:process.env.SINGLE_PC==='false'?[false]:[true,false];
const output=resolve(process.env.RECONNECT_OUTPUT??join(here,'result.json'));
const sdkPath=require.resolve('livekit-client');const sdk=await readFile(sdkPath),html=await readFile(join(here,'client.html'));
const version=JSON.parse(await readFile(join(dirname(sdkPath),'../package.json'),'utf8')).version;
assert.equal(version,'2.23.0','unchanged locked official client required');
const evidence={status:'RUNNING',scope:'Actual official SDK reconnect consumers, not server unit event injection',label:process.env.SERVER_LABEL??'unspecified',flag:enabled,sdk:version,sdkSha256:createHash('sha256').update(sdk).digest('hex'),rows:[],cleanupErrors:[]};
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret);
let browser,host;const sleep=ms=>new Promise(r=>setTimeout(r,ms));const now=()=>performance.now();
const marker=name=>({fixture:'f107',phase:name});
const contains=(report,name)=>report.received.some(x=>x.value.fixture==='f107'&&x.value.phase===name);
const video=(r,identity)=>r.videos.filter(v=>v.identity===identity).reduce((s,v)=>s+v.frames,0);
const activeVideo=(r,identity)=>r.videos.some(v=>v.identity===identity&&v.active&&!v.ended&&v.width>=640&&v.height>=360);
const audible=(r,identity)=>r.audio.filter(v=>v.identity===identity).reduce((s,v)=>s+v.audible,0);
const reconnects=r=>r.events.filter(e=>['Reconnecting','Reconnected','SignalReconnecting'].includes(e.type));
async function token(room,identity){const t=new AccessToken(key,secret,{identity,ttl:600});t.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});return t.toJwt();}
async function report(page){return page.evaluate(()=>consumer.report());}
async function until(fn,timeout,message){return pollUntil(fn,now()+timeout,message);}
function physical(report,single){assert.ok(topologyValid(report.topology,single),'requested PC mode does not match actual native topology');}
async function send(room,kind,name){await api.sendData(room,new TextEncoder().encode(JSON.stringify(marker(name))),kind,{destinationIdentities:['victim']});}
async function row(single,kind){
 const result={single,kind:kind===DataPacket_Kind.RELIABLE?'reliable':'lossy',status:'RUNNING',cleanupErrors:[]};evidence.rows.push(result);
 const room='f107-'+randomUUID();let context,pages=[],roomCreated=false;
 try{
  await api.createRoom({name:room,emptyTimeout:600});roomCreated=true;
  context=await browser.newContext();
  for(const identity of ['victim','source','witness']){
   const page=await context.newPage();pages.push(page);await page.goto(`http://127.0.0.1:${host.address().port}`);
   await page.evaluate(async options=>consumer.connect(options),{url,token:await token(room,identity),single,publish:identity!=='witness'});
  }
  const[victim,source,witness]=pages;
  await until(async()=>{const v=await report(victim),w=await report(witness);return activeVideo(v,'source')&&activeVideo(w,'victim')&&video(v,'source')>=30&&audible(v,'source')>=10&&video(w,'victim')>=30&&audible(w,'victim')>=10;},30000,'initial real decoded media did not meet original 30-frame/10-audible-sample gates');
  assert.equal((await api.listParticipants(room)).length,3);
  const initial=await Promise.all(pages.map(report));for(const r of initial)physical(r,single);result.topologyBefore=initial.map(r=>r.topology);
  result.channel=await victim.evaluate(k=>consumer.selectChannel(k),kind);
  assert.equal(result.channel.label,kind===DataPacket_Kind.RELIABLE?'_reliable':'_lossy');
  const beforePrime=await report(victim);await send(room,kind,'prime');
  await until(async()=>contains(await report(victim),'prime'),10000,'actual server SendData did not reach selected native channel');
  assert.equal((await report(victim)).received.find(x=>x.value.phase==='prime')?.kind,kind,'actual prime must use requested delivery kind');
  assert.ok((await report(victim)).channelMessages>beforePrime.channelMessages,'native selected downstream handle must observe actual prime delivery');
  assert.ok(!contains(await report(witness),'prime'),'targeted data leaked to non-target');
  const beforeClose=await report(victim),witnessBefore=await report(witness);
  await victim.evaluate(()=>consumer.closeSelected());
  await until(async()=>(await report(victim)).selectedState==='closed',10000,'native data channel did not actually close');
  await sleep(5000);
  const closeOnly=await report(victim),witnessClose=await report(witness);
  assert.equal(closeOnly.connection,'connected');assert.equal(closeOnly.sidChanged,false);
  assert.equal(reconnects(closeOnly).length,reconnects(beforeClose).length,'channel close alone must not issue reconnect');
  assert.ok(video(closeOnly,'source')>video(beforeClose,'source')&&audible(closeOnly,'source')>audible(beforeClose,'source'),'close-only phase stalled media');
  assert.ok(video(witnessClose,'victim')>video(witnessBefore,'victim'),'victim publication stalled before write');
  physical(closeOnly,single);physical(witnessClose,single);const victimBefore=baseline(closeOnly),witnessBeforeGeneration=baseline(witnessClose);
  result.closeOnly={status:'PASS',closed:true,noReconnect:true,decodedVideoDelta:video(closeOnly,'source')-video(beforeClose,'source')};
  // This is the actual server API writer path, not a client publish or injected
  // MediaEvent. A single causal write follows the observed native-close-only phase.
  const triggerAt=now(),recoveryDeadline=triggerAt+30000;
  await callBeforeDeadline(()=>send(room,kind,'trigger'),recoveryDeadline,now,'causal SendData exceeded recovery deadline');
  if(enabled){
   await pollUntil(async()=>{const r=await report(victim);return r.events.slice(closeOnly.events.length).some(e=>e.type==='Reconnecting');},Math.min(recoveryDeadline,triggerAt+10000),'flag true: no official SDK Reconnecting after real closed-channel server write');
   await pollUntil(async()=>{const r=await report(victim);return r.connection==='connected'&&r.sidChanged&&r.selectedReplacement&&r.replacementOpen&&topologyValid(r.topology,single)&&r.events.slice(closeOnly.events.length).some(e=>e.type==='Reconnected');},recoveryDeadline,'full official SDK session rebuild/new SID/new native topology/channel did not complete');
   await pollUntil(async()=>{const v=await report(victim),w=await report(witness);return recovered(newMedia(v,victimBefore,'source','local-rebuilt'))&&recovered(newMedia(w,witnessBeforeGeneration,'victim','remote-rebuilt'))&&v.localTracks.filter(t=>t.hasTrack).length===2;},recoveryDeadline,'new-session real media did not meet original30frames/10audible gates');
   const gatedV=await report(victim),gatedW=await report(witness);physical(gatedV,single);physical(gatedW,single);assertDeadline(now(),recoveryDeadline,'media gate completed late');
   result.newGenerationMedia={victim:newMedia(gatedV,victimBefore,'source','local-rebuilt'),witness:newMedia(gatedW,witnessBeforeGeneration,'victim','remote-rebuilt'),elapsedMs:now()-triggerAt};
   await callBeforeDeadline(()=>send(room,kind,'after-rebuild'),recoveryDeadline,now,'post-rebuild SendData exceeded deadline');
   await pollUntil(async()=>contains(await report(victim),'after-rebuild'),recoveryDeadline,'rebuilt selected data channel is not functional');
   const resumed=await report(victim);physical(resumed,single);assertDeadline(now(),recoveryDeadline,'recovery completed after30s');
   result.rebuild={status:'PASS',newSid:true,newNativeHandle:true,topology:resumed.topology,elapsedMs:now()-triggerAt,localTrackCount:resumed.localTracks.filter(t=>t.hasTrack).length};
  }else{
   await sleep(5000);const no=await report(victim);
   assert.equal(reconnects(no).length,reconnects(closeOnly).length,'flag false issued reconnect');assert.equal(no.sidChanged,false);assert.equal(no.connection,'connected');physical(no,single);assert.deepEqual(no.topology.nativeIds,closeOnly.topology.nativeIds,'flag false changed actual native PC generation');
   assert.equal(contains(no,'trigger'),false,'write unexpectedly arrived through the closed selected channel');
   const other=kind===DataPacket_Kind.RELIABLE?DataPacket_Kind.LOSSY:DataPacket_Kind.RELIABLE;
   await send(room,other,'other-kind');await until(async()=>contains(await report(victim),'other-kind'),10000,'healthy alternate data channel failed');
   assert.equal((await report(victim)).received.find(x=>x.value.phase==='other-kind')?.kind,other,'healthy alternate delivery kind mismatched');
   result.noReconnect={status:'PASS',stableSid:true,otherKindDelivered:true};
  }
  const sustainV=await report(victim),sustainW=await report(witness);
  const sustainGenerationV=enabled?newMedia(sustainV,victimBefore,'source','local-rebuilt'):null,sustainGenerationW=enabled?newMedia(sustainW,witnessBeforeGeneration,'victim','remote-rebuilt'):null;
  await sleep(5000);
  const endV=await report(victim),endW=await report(witness);
  assert.equal(endV.connection,'connected');assert.ok(video(endV,'source')>video(sustainV,'source'),'recovery did not sustain video');assert.ok(audible(endV,'source')>audible(sustainV,'source'),'recovery did not sustain audio');
  assert.ok(video(endW,'victim')>video(sustainW,'victim'),'victim republished media did not sustain');
  for(const r of [endV,endW])physical(r,single);
  if(enabled){const v=newMedia(endV,victimBefore,'source','local-rebuilt'),w=newMedia(endW,witnessBeforeGeneration,'victim','remote-rebuilt');assert.ok(v.videoFrames>sustainGenerationV.videoFrames&&v.audibleSamples>sustainGenerationV.audibleSamples&&w.videoFrames>sustainGenerationW.videoFrames&&w.audibleSamples>sustainGenerationW.audibleSamples,'new generations did not sustain after the already-satisfied30/10gate');result.newGenerationSustain={victimVideo:v.videoFrames-sustainGenerationV.videoFrames,victimAudible:v.audibleSamples-sustainGenerationV.audibleSamples,witnessVideo:w.videoFrames-sustainGenerationW.videoFrames,witnessAudible:w.audibleSamples-sustainGenerationW.audibleSamples};}
  assert.equal((await api.listParticipants(room)).length,3,'session rebuild leaked/removed room participants');
  assert.ok(!contains(endW,'trigger')&&!contains(endW,'after-rebuild')&&!contains(endW,'other-kind'),'targeted payload leaked');
  result.topologyAfter=[endV.topology,endW.topology];
  result.final={state:endV.connection,events:endV.events.map(e=>({type:e.type,detail:e.detail})),videoProgress:video(endV,'source')-video(sustainV,'source'),audioProgress:audible(endV,'source')-audible(sustainV,'source'),witnessVideoProgress:video(endW,'victim')-video(sustainW,'victim'),playbackErrors:endV.errors};
  assert.deepEqual(endV.errors,[],'browser playback/SDK payload errors');result.status='PASS';
 }catch(error){result.status='FAIL';result.error=String(error);result.failureSnapshots=await Promise.all(pages.map(async p=>{try{return await report(p);}catch(e){return {snapshotError:String(e)};}}));throw error;}
 finally{
  for(const page of pages){try{await page.evaluate(()=>consumer.close());}catch(error){result.cleanupErrors.push('client.close: '+String(error));}}
  if(context){try{await context.close();}catch(error){result.cleanupErrors.push('context.close: '+String(error));}}
  if(roomCreated){try{await api.deleteRoom(room);}catch(error){result.cleanupErrors.push('deleteRoom: '+String(error));}}
  if(result.cleanupErrors.length){result.status='FAIL';throw Error('owned row cleanup failed: '+result.cleanupErrors.join('; '));}
 }
}
try{
 host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(resolve=>host.listen(0,'127.0.0.1',resolve));
 browser=await chromium.launch({headless:true,args:['--autoplay-policy=no-user-gesture-required']});evidence.browser=browser.version();
 for(const single of modes)for(const kind of [DataPacket_Kind.RELIABLE,DataPacket_Kind.LOSSY])await row(single,kind);
 evidence.status='PASS_SCOPED';
}catch(error){evidence.status='FAIL';evidence.error=String(error);process.exitCode=1;}
finally{
 if(browser){try{await browser.close();}catch(error){evidence.cleanupErrors.push('browser.close: '+String(error));}}
 if(host){try{await new Promise((resolve,reject)=>host.close(error=>error?reject(error):resolve()));}catch(error){evidence.cleanupErrors.push('host.close: '+String(error));}}
 if(evidence.cleanupErrors.length){evidence.status='FAIL';process.exitCode=1;}
 await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
}
