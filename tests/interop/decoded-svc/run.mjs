import {sourceStride,compareCadence} from './cadence-gate.mjs';
import {closeOwnedBrowser,observeChildStderr} from './owned-browser-exit.mjs';
import {execFileSync} from 'node:child_process';
// Source-only delivery. Coordinator executes real browser/server campaigns.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir,realpath} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
const here=dirname(fileURLToPath(import.meta.url));
const dependencies=process.env.INTEROP_DEPENDENCY_ROOT;
assert.ok(dependencies&&isAbsolute(dependencies),'INTEROP_DEPENDENCY_ROOT must be absolute pinned tests/interop');
const require=createRequire(join(dependencies,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const output=process.env.INTEROP_OUTPUT_DIR;assert.ok(output&&isAbsolute(output),'fresh absolute INTEROP_OUTPUT_DIR required');await mkdir(output,{recursive:false});
const codec=process.env.VIDEO_CODEC||'vp9',mode=process.env.SCALABILITY_MODE||'S3T3',single=process.env.SINGLE_PC!=='false';
assert.ok(['vp9','av1'].includes(codec));assert.match(mode,/^[LS][123]T[123](?:_KEY(?:_SHIFT)?)?$/);
const entries=JSON.parse(process.env.LIVEKIT_PEER_URLS||JSON.stringify([process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',process.env.LIVEKIT_URL||'ws://127.0.0.1:7880']));
assert.ok(entries.length===2&&entries.every(u=>['ws:','wss:'].includes(new URL(u).protocol)));
const apiKey=process.env.LIVEKIT_API_KEY||'devkey',apiSecret=process.env.LIVEKIT_API_SECRET||'secret';
const api=new RoomServiceClient(entries[0].replace(/^ws/,'http'),apiKey,apiSecret);
const sdk=await readFile(require.resolve('livekit-client'));
const pkg=JSON.parse(await readFile(join(dependencies,'package.json'),'utf8'));
assert.equal(pkg.dependencies['livekit-client'],'2.23.0');assert.equal(pkg.dependencies['livekit-server-sdk'],'2.19.2');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(['enabled','disabled','original'].includes(process.env.C21_STATE),'C21_STATE must name the externally owned server configuration');
const evidence={status:'RUNNING',codec,mode,single,c21Requested:process.env.C21_STATE||'UNSPECIFIED',serverLabel:process.env.SERVER_LABEL,entries:entries.map(s=>new URL(s).origin),sdkVersion:'2.23.0',sdkSha256:sha(sdk),serverSdkVersion:'2.19.2',stages:[],blocked:[],notApplicable:[],errors:[],startedAt:new Date().toISOString()};
if(process.env.SERVER_CONFIG_FILE)evidence.serverConfig={path:process.env.SERVER_CONFIG_FILE,sha256:sha(await readFile(process.env.SERVER_CONFIG_FILE)),effectiveC21VerifiedBy:'external owned server runner; this browser cannot infer configuration from a label'};
if(process.env.SERVER_BINARY)evidence.serverBinary={path:process.env.SERVER_BINARY,sha256:sha(await readFile(process.env.SERVER_BINARY))};
const modeParserPath=join(dependencies,'node_modules/livekit-client/src/room/participant/publishUtils.ts');
const modeParserSource=await readFile(modeParserPath,'utf8');
const pinnedModePattern=/^L(\d)T(\d)(h|_KEY|_KEY_SHIFT){0,1}$/;
assert.ok(modeParserSource.includes("throw new Error('invalid scalability mode')")&&modeParserSource.includes('scalabilityMode.match(/^L('),'installed SDK mode parser shape drift');
evidence.sdkModeParser={path:modeParserPath,sha256:sha(modeParserSource),supportsRequested:pinnedModePattern.test(mode)};
let originalReference;
if(process.env.ORIGINAL_GO_RECEIPT){
 const bytes=await readFile(process.env.ORIGINAL_GO_RECEIPT);originalReference=JSON.parse(bytes);
 assert.equal(originalReference.status,'PASS','original decoded reference must truly pass');
 assert.equal(originalReference.codec,codec);assert.equal(originalReference.mode,mode);assert.equal(originalReference.single,single);
 assert.equal(originalReference.sdkSha256,evidence.sdkSha256,'reference must use identical official SDK bytes');
 assert.equal(originalReference.browserExit?.code,0,'actual original browser child exit0 required');
 assert.equal(originalReference.serverReleased,true);
 evidence.originalReference={path:process.env.ORIGINAL_GO_RECEIPT,sha256:sha(bytes),browser:originalReference.browser,temporal:originalReference.temporal};
}
if(process.env.C21_STATE!=='original'&&evidence.sdkModeParser.supportsRequested)assert.ok(originalReference,'Rust decoder acceptance requires ORIGINAL_GO_RECEIPT for this exact codec/mode/PC profile');
const sources=Object.fromEntries(await Promise.all(['client.html','worker.js'].map(async f=>[f,await readFile(join(here,f))])));
const host=createServer((req,res)=>{const key=req.url==='/'?'client.html':req.url.slice(1);const b=key==='sdk.js'?sdk:sources[key];if(!b){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':key.endsWith('.js')?'text/javascript':'text/html','cross-origin-opener-policy':'same-origin','cross-origin-embedder-policy':'require-corp','cache-control':'no-store'});res.end(b);});
let browser,browserServer,finishBrowserStderr,contexts=[],pages=[],room,failed,hostStarted=false;
let interrupted=false;for(const signal of ['SIGTERM','SIGINT'])process.once(signal,()=>{interrupted=true;});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function reports(){return await Promise.all(pages.map(p=>p.evaluate(()=>decoded.report())));}
async function poll(fn,message,timeout=30000){const end=Date.now()+timeout;let last;while(Date.now()<end){if(interrupted)throw Error('owned campaign interrupted');last=await fn();if(last)return last;await sleep(250);}throw Error(message);}
async function stage(name,{epoch,quality='high',fps=15,policy='all',dimensions,minimum=12}={}){
 const before=await reports();
 const workerBefore=await pages[0].evaluate(()=>decoded.worker());
 await Promise.all(pages.map((p,i)=>p.evaluate(v=>decoded.stage(v),{nextEpoch:epoch,quality:i===1?quality:i===2?'low':undefined,fps:i===1?fps:i===2?15:undefined,policy:i===0?policy:undefined})));
 const start=Date.now();evidence.pendingStage={name,epoch,quality,fps,policy,workerBefore,beforeStats:before.map(r=>({outbound:r.outbound,inbound:r.inbound}))};
 await poll(async()=>{const r=await pages[1].evaluate(()=>decoded.report());const all=r.pixels.filter(p=>p.epoch===epoch);const pixels=all.filter(p=>!dimensions||(p.width===dimensions[0]&&p.height===dimensions[1]));return new Set(pixels.map(p=>p.frame)).size>=minimum&&pixels.length>=minimum&&(!dimensions||all.slice(-5).every(p=>p.width===dimensions[0]&&p.height===dimensions[1]));},`${name}: new decoded source marker/dimension requirement`);
 const after=await reports(),workerAfter=await pages[0].evaluate(()=>decoded.worker());
 const pixels=after[1].pixels.filter(p=>p.epoch===epoch);
 const source=new Set(after[0].source.filter(p=>p.epoch===epoch).map(p=>p.frame));
 assert.ok(pixels.every(p=>source.has(p.frame)),`${name}: decoded marker must correspond to an actually rendered source frame`);
 assert.ok(new Set(pixels.map(p=>p.hash)).size>=3,`${name}: frozen or repeated pixels`);
 const maxDecoded=r=>r.inbound.reduce((n,s)=>n+(s.framesDecoded||0),0);
 assert.ok(maxDecoded(after[1])>maxDecoded(before[1]),`${name}: actual decoder counter did not advance`);
 assert.ok(after[1].inbound.some(s=>s.mimeType?.toLowerCase()===`video/${codec}`),`${name}: actual inbound codec`);
 assert.ok(after[0].outbound.some(s=>s.mimeType?.toLowerCase()===`video/${codec}`&&s.scalabilityMode===mode),`${name}: actual source codec/scalability mode changed`);
 assert.ok(after.every(r=>r.errors.length===0&&!r.workerFatal),`${name}: browser or worker errors`);
 const receipt={name,epoch,elapsedMs:Date.now()-start,quality,fps,policy,pixels,source:after[0].source.filter(p=>p.epoch===epoch),beforeStats:before.map(r=>({outbound:r.outbound,inbound:r.inbound})),afterStats:after.map(r=>({outbound:r.outbound,inbound:r.inbound})),workerBefore,workerAfter};
 evidence.stages.push(receipt);delete evidence.pendingStage;await writeFile(join(output,'evidence.json'),JSON.stringify(evidence,null,2));return receipt;
}
try{
 await new Promise(resolve=>host.listen(0,'127.0.0.1',resolve));hostStarted=true;
 browserServer=await chromium.launchServer({...(process.env.BROWSER_EXECUTABLE?{executablePath:process.env.BROWSER_EXECUTABLE}:{channel:process.env.BROWSER_CHANNEL||'chrome'}),headless:true,args:['--autoplay-policy=no-user-gesture-required',...(process.env.TLS_SPKI?[`--ignore-certificate-errors-spki-list=${process.env.TLS_SPKI}`]:[])]});
 finishBrowserStderr=observeChildStderr(browserServer.process());
 const child=browserServer.process();assert.ok(child?.pid,'actual owned browser child required');
 const birth=pid=>execFileSync('/bin/ps',['-p',String(pid),'-o','lstart='],{encoding:'utf8'}).trim();
 const uid=pid=>Number(execFileSync('/bin/ps',['-p',String(pid),'-o','uid='],{encoding:'utf8'}).trim());
 const linuxStart=async pid=>{if(process.platform!=='linux')return null;const stat=await readFile(`/proc/${pid}/stat`,'utf8');return stat.slice(stat.lastIndexOf(')')+1).trim().split(/\s+/)[19];};
 const executable=process.platform==='linux'?await realpath(`/proc/${child.pid}/exe`):child.spawnfile;
 evidence.ownership={nodePid:process.pid,nodeBirth:birth(process.pid),nodeUid:uid(process.pid),nodeExecutable:process.execPath,nodeExecutableSha256:sha(await readFile(process.execPath)),nodeLinuxStart:await linuxStart(process.pid),browserPid:child.pid,browserBirth:birth(child.pid),browserUid:uid(child.pid),browserExecutable:executable,browserExecutableSha256:sha(await readFile(executable)),browserLaunchEntry:child.spawnfile,browserLinuxStart:await linuxStart(child.pid)};
 assert.equal(evidence.ownership.nodeUid,evidence.ownership.browserUid,'owned browser UID mismatch');
 await writeFile(join(output,'ownership.json'),JSON.stringify(evidence.ownership,null,2),{flag:'wx',mode:0o600});
 browser=await chromium.connect(browserServer.wsEndpoint());
 evidence.browser=browser.version();
 room=`decoded-${Date.now()}-${process.pid}`;evidence.room=room;await api.createRoom({name:room,emptyTimeout:10,departureTimeout:2});
 for(let i=0;i<3;i++){
  const c=await browser.newContext();contexts.push(c);const p=await c.newPage();pages.push(p);p.on('pageerror',e=>evidence.errors.push({client:i,error:String(e)}));
  await p.goto(`http://127.0.0.1:${host.address().port}`);
  const token=new AccessToken(apiKey,apiSecret,{identity:`decoded-${i}`,ttl:'15m'});token.addGrant({roomJoin:true,room,canPublish:i===0,canSubscribe:true,canPublishData:true});
  await p.evaluate(opts=>decoded.connect(opts),{url:entries[Math.min(i,1)],token:await token.toJwt(),single});
 }
 evidence.capabilities=await pages[0].evaluate(c=>decoded.capabilities(c),codec);
 if(!evidence.capabilities.codec||!evidence.capabilities.rvfc){evidence.blocked.push({scope:'whole codec pixel campaign',reason:'BROWSER_CODEC_OR_RVFC_UNSUPPORTED',actual:evidence.capabilities});}
 else{
  await pages[0].evaluate(o=>decoded.publish(o),{codec,mode});
  const actual=await poll(async()=>{const r=await pages[0].evaluate(()=>decoded.report());return r.outbound.find(s=>s.framesEncoded>5);},'source encoder never produced frames');
  evidence.actualEncoder=actual;
  if(actual.mimeType?.toLowerCase()!==`video/${codec}`||actual.scalabilityMode!==mode){evidence.blocked.push({scope:'exact requested encoder mode',reason:'BROWSER_ENCODER_MODE_NOT_ACTIVE',requested:{codec,mode},actual});}
  else{
   const warmup=await stage('warmup-high',{epoch:2,dimensions:[960,544]});
   evidence.actualTopology=await Promise.all(pages.map(async p=>{const report=await p.evaluate(()=>decoded.report());return {participantSid:report.participantSid,pcs:report.transports,connectedPcs:report.transports.filter(t=>t.connection==='connected').length};}));
   assert.equal(evidence.actualTopology[0].connectedPcs,single?1:2,'actual source PC topology differs from requested single/dual; SDK option may fall back');
   assert.ok(evidence.actualTopology[0].pcs.some(pc=>pc.connection==='connected'&&pc.activeOutboundVideo.length),'no actual source video-sending PC');
   assert.ok(evidence.actualTopology.slice(1).every(p=>p.pcs.some(pc=>pc.connection==='connected'&&pc.activeInboundVideo.length)),'no actual receiving PC for a decoded subscriber');
   const low=await stage('low',{epoch:3,quality:'low',dimensions:[240,136]});
   await stage('high-after-low',{epoch:4,dimensions:[960,544]});
   await stage('low-repeat',{epoch:5,quality:'low',dimensions:[240,136]});
   await stage('high-repeat',{epoch:6,dimensions:[960,544]});
   const slow=await stage('temporal-low',{epoch:7,fps:1,minimum:10});
   const fast=await stage('temporal-resume',{epoch:8,fps:15,minimum:20});
   const cadence=pixels=>{const intervals=pixels.slice(1).map((p,i)=>p.at-pixels[i].at).filter(ms=>ms>0).sort((a,b)=>a-b);return 1000/intervals[Math.floor(intervals.length/2)];};
   evidence.temporal={lowDecodedFps:cadence(slow.pixels),highDecodedFps:cadence(fast.pixels),lowFrameValues:slow.pixels.map(p=>p.frame),highFrameValues:fast.pixels.map(p=>p.frame),interpretation:'Actual markers and frame times; compare Go original temporal ratios rather than requested FPS alone'};
   evidence.temporal.lowSourceFrameStride=sourceStride(evidence.temporal.lowFrameValues);
   evidence.temporal.highSourceFrameStride=sourceStride(evidence.temporal.highFrameValues);
   if(originalReference){
    evidence.temporal.referenceGate=compareCadence(evidence.temporal,originalReference.temporal);
    assert.ok(evidence.temporal.referenceGate.pass,`actual decoded cadence/source stride differs from original: ${JSON.stringify(evidence.temporal.referenceGate.comparisons)}; ratio-only or keyframe-only progression is insufficient`);
   }
   assert.ok(evidence.temporal.highDecodedFps>evidence.temporal.lowDecodedFps*1.25,'temporal layer did not resume actual decoded marker cadence');
   { // Actual dependency graph, not L/S label, decides high-only eligibility.
    const w=await pages[0].evaluate(()=>decoded.worker());
    if(evidence.capabilities.transform&&(!w||w.pipelineStarted===0||w.total===0))throw Error('HARNESS_TRANSFORM_NOT_PROCESSING_ACTUAL_ENCODED_FRAMES');
    if(!evidence.capabilities.transform||!w||w.missing||!Object.keys(w.layers).some(k=>k.startsWith('2/'))){evidence.blocked.push({scope:'independent high-only',reason:'BROWSER_TRANSFORM_OR_ACTUAL_LAYER_METADATA_UNSUPPORTED',actual:w});}
    else{
     const sparse=await stage('independent-high-only',{epoch:9,policy:'high-only',dimensions:[960,544],minimum:20});
     assert.ok(sparse.workerAfter.dropped>sparse.workerBefore.dropped,'no actual lower encoded spatial frames were dropped');
     assert.equal(sparse.workerAfter.dependencyViolation,sparse.workerBefore.dependencyViolation,'unsafe dependent frame passed transform');
     assert.ok(sparse.workerAfter.highPassed>sparse.workerBefore.highPassed,'no independently admissible high frame passed');
     evidence.observedDependentFrameRejections=sparse.workerAfter.highDependencyRejected-sparse.workerBefore.highDependencyRejected;
     assert.equal(sparse.workerAfter.missing,sparse.workerBefore.missing,'missing metadata during high-only stage');
     assert.ok(sparse.workerAfter.recent.filter(f=>f.generation===sparse.workerAfter.generation&&f.keep).every(f=>f.spatial===2),'high-only transform passed lower frames');
     await stage('restore-low-after-high-only',{epoch:10,quality:'low',policy:'all',dimensions:[240,136]});
     await stage('restore-high-after-high-only',{epoch:11,dimensions:[960,544]});
    }
   }
   await pages[0].evaluate(()=>decoded.data('before-resume'));await poll(async()=>(await pages[1].evaluate(()=>decoded.report())).data.includes('before-resume'),'real SDK data before resume');
   const beforeReconnect=await reports();await pages[1].evaluate(()=>decoded.reconnect());
   await poll(async()=>{const r=await pages[1].evaluate(()=>decoded.report());return r.events.length>beforeReconnect[1].events.length&&r.connection==='connected'&&r.events.slice(beforeReconnect[1].events.length).some(e=>e.state==='reconnecting'||e.state==='signalReconnecting');},'receiver reconnect lifecycle');
   await stage('after-receiver-resume',{epoch:12,dimensions:[960,544]});
   assert.equal((await pages[1].evaluate(()=>decoded.report())).participantSid,beforeReconnect[1].participantSid,'resume must retain actual participant SID');
   await pages[0].evaluate(()=>decoded.data('after-resume'));await poll(async()=>(await pages[1].evaluate(()=>decoded.report())).data.includes('after-resume'),'real SDK data after resume');
   const retired=await pages[0].evaluate(()=>decoded.unpublish());const beforeRetire=await pages[1].evaluate(()=>decoded.report());await sleep(1500);const afterRetire=await pages[1].evaluate(()=>decoded.report());
   // An old buffered frame may finish at unpublish; the settled interval must stop.
   const settled=afterRetire.pixels.length;await sleep(1000);assert.equal((await pages[1].evaluate(()=>decoded.report())).pixels.length,settled,'retired source keeps producing decoded frames');
   evidence.retirement={sid:retired,before:beforeRetire.pixels.length,settled};
   const fresh=await pages[0].evaluate(o=>decoded.publish(o),{codec,mode});
   assert.notEqual(fresh.sid,retired,'republished source must have a fresh real track SID');evidence.retirement.freshSid=fresh.sid;
   const freshReceipt=await stage('fresh-source-after-retirement',{epoch:13,dimensions:[960,544]});
   assert.ok(freshReceipt.pixels.every(p=>p.sid===fresh.sid),'retired source generation contaminated new decoded epoch');
   // The high subscriber retires while a distinct low subscriber stays live.
   const lowBefore=await pages[2].evaluate(()=>decoded.report());
   await pages[1].evaluate(()=>decoded.close());
   await Promise.all([pages[0].evaluate(()=>decoded.stage({nextEpoch:14})),pages[2].evaluate(()=>decoded.stage({nextEpoch:14,quality:'low',fps:15}))]);
   await poll(async()=>{const r=await pages[2].evaluate(()=>decoded.report());return new Set(r.pixels.filter(p=>p.epoch===14&&p.width===240&&p.height===136).map(p=>p.frame)).size>=12;},'low subscriber lost media when high subscriber retired');
   const lowAfter=await pages[2].evaluate(()=>decoded.report());
   assert.ok(lowAfter.inbound.reduce((n,s)=>n+(s.framesDecoded||0),0)>lowBefore.inbound.reduce((n,s)=>n+(s.framesDecoded||0),0));
   evidence.highRetirementWithLiveLow={before:lowBefore.inbound,after:lowAfter.inbound,pixels:lowAfter.pixels.filter(p=>p.epoch===14)};
  }
 }
 assert.equal(evidence.errors.length,0,'page errors occurred');
 evidence.status=evidence.blocked.length?'BLOCKED_SCOPED':'PASS';
 }catch(error){
 failed=error;const actualError=String(error.stack||error);
 if(!pinnedModePattern.test(mode)&&/invalid scalability mode/.test(actualError)&&/ScalabilityMode|computeVideoEncodings|publishUtils/.test(actualError)){
  evidence.blocked.push({scope:'exact requested SDK publish mode',reason:'BLOCKED_SDK_MODE',mode,actualError,source:evidence.sdkModeParser});
  evidence.status='BLOCKED_SCOPED';
 }else{evidence.status='FAIL';evidence.failure=actualError;}
}
finally{
 evidence.finalWorker=await pages[0]?.evaluate(()=>decoded.worker()).catch(error=>({error:String(error)}));
 evidence.finalReports=await reports().catch(error=>[{error:String(error)}]);
 const closing=await Promise.all(pages.map(p=>p.evaluate(()=>decoded.close()).catch(error=>({error:String(error)}))));evidence.closing=closing;
 await sleep(500);
 const closed=await reports().catch(()=>[]);evidence.closedReports=closed;
 if(closed.length&&closed.some(r=>r.connection!=='disconnected'||r.postCloseFrames!==0)){evidence.status='FAIL';evidence.cleanupFailure='browser media callbacks or connection survived close';}
 // Let each renderer acknowledge close before disposing its context and
 // shutting down the owned Chromium process. Keep the actual media callbacks
 // and disconnect checks above, while those pages are still observable.
 const pageCloseFailures=[];
 evidence.pageCleanup=[];
 for(const [index,page] of pages.entries()){
  try{
   await page.close({runBeforeUnload:false});
   const closed=page.isClosed();
   evidence.pageCleanup.push({index,closed});
   if(!closed)pageCloseFailures.push('owned-browser-page-remains');
  }catch(error){
   evidence.pageCleanup.push({index,closed:false,error:String(error)});
   pageCloseFailures.push('owned-browser-page-close');
  }
 }
 const closedBrowser=await closeOwnedBrowser(browser,browserServer);
 closedBrowser.failures.unshift(...pageCloseFailures);
 evidence.browserCleanup=closedBrowser;evidence.browserExit=closedBrowser.exit;
 evidence.browserExited=Boolean(browserServer&&closedBrowser.exit?.code===0&&closedBrowser.exit?.signal==null&&!closedBrowser.exit?.timeout&&closedBrowser.contextsAfter===0&&closedBrowser.failures.length===0);
 if(browserServer&&!evidence.browserExited){evidence.status='FAIL';evidence.cleanupFailure='actual owned Chromium child exit0/context0 required';}
 if(finishBrowserStderr){
  const stderr=finishBrowserStderr();evidence.browserStderr=stderr.metadata;
  await writeFile(join(output,'owned-browser.stderr'),stderr.bytes,{flag:'wx',mode:0o600});
 }
 if(room){try{await poll(async()=>!(await api.listRooms([room])).length||(await api.listParticipants(room)).length===0,'participants did not release',15000);if((await api.listRooms([room])).length)await api.deleteRoom(room);assert.equal((await api.listRooms([room])).length,0);evidence.serverReleased=true;}catch(error){evidence.status='FAIL';evidence.cleanupFailure=String(error);}}
 if(hostStarted)await new Promise(resolve=>host.close(resolve));evidence.staticServerClosed=!host.listening;evidence.finishedAt=new Date().toISOString();
 await writeFile(join(output,'evidence.json'),JSON.stringify(evidence,null,2));console.log(JSON.stringify({status:evidence.status,output,stages:evidence.stages.length,blocked:evidence.blocked}));
}
process.exitCode=evidence.status==='PASS'?0:evidence.status==='BLOCKED_SCOPED'?2:1;
