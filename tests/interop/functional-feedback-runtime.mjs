/** Independent functional SDK fixture. NOT_RUN; does not start a LiveKit server.
 * Native performance/accuracy campaigns remain separately mandatory/deferred.
 */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {randomBytes,createHash} from 'node:crypto';
const interop=resolve(process.env.INTEROP_ROOT||'tests/interop');
const require=createRequire(join(interop,'package.json'));
const {chromium}=require('playwright'),{AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const mode=process.env.FEEDBACK_MODE;
assert.ok(['remote','interceptor','native'].includes(mode),'explicit externally configured FEEDBACK_MODE required');
const url=process.env.LIVEKIT_URL;assert.ok(url,'explicit actual server URL required');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const output=resolve(process.env.FEEDBACK_OUTPUT||`/tmp/feedback-${mode}.json`);
const sdk=await readFile(require.resolve('livekit-client')),client=await readFile(join(interop,'client.html'));
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret);
const evidence={profile:'actual-feedback-runtime-functional',mode,status:'RUNNING',startedAt:new Date().toISOString(),cases:[],sourceInputs:{
 sdkSha256:createHash('sha256').update(sdk).digest('hex'),unchangedClientSha256:createHash('sha256').update(client).digest('hex')},
 scope:'Official unchanged SDK, existing unchanged audio/video client, actual media/data/SDP. Does not replace native five-round accuracy or prove capacity/performance/Pion numerical parity.'};
const host=createServer((req,res)=>{if(!['/','/sdk.js'].includes(req.url)){res.writeHead(404);return res.end();}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(req.url==='/'?client:sdk);});
let browser,context,room;
const errors=[];
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check,message){const stop=Date.now()+30000;while(Date.now()<stop){if(await check())return;await delay(50);}throw Error(message);}
try {
 await new Promise(r=>host.listen(0,'127.0.0.1',r));
 browser=await chromium.launch({headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});evidence.browser=browser.version();
 for(const single of [true,false]) {
  room=`f12-feedback-${mode}-${single?'single':'dual'}-${randomBytes(5).toString('hex')}`;
  const row={single,status:'RUNNING'};evidence.cases.push(row);
  await api.createRoom({name:room,emptyTimeout:30});context=await browser.newContext();
  const pages=await Promise.all([context.newPage(),context.newPage()]);
  await Promise.all(pages.map(async(page,index)=>{
   await page.goto(`http://127.0.0.1:${host.address().port}`);
   const token=new AccessToken(key,secret,{identity:`feedback-${index}`});token.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});
   await page.evaluate(args=>interop.connect(args),{url,token:await token.toJwt(),single,frequency:440+index*440});
  }));
  await Promise.all(pages.map(page=>page.evaluate(()=>interop.video({codec:'vp8',complex:true}))));
  await until(async()=>{const reports=await Promise.all(pages.map(p=>p.evaluate(()=>interop.report())));return reports.every(r=>r.videoFrames>=30&&r.audibleSamples>=10);},'actual original-client video/audio missing');
  await Promise.all(pages.map(p=>p.evaluate(()=>interop.data({count:10,reliable:true,topic:'feedback-functional'}))));
  await until(async()=>{const reports=await Promise.all(pages.map(p=>p.evaluate(()=>interop.report())));return reports.every(r=>r.received.filter(d=>d.topic==='feedback-functional').length===10);},'actual reliable SDK data missing');
  row.reports=await Promise.all(pages.map(p=>p.evaluate(()=>interop.report())));
  assert.ok(row.reports.every(r=>r.errors.length===0));
  row.negotiation=await Promise.all(pages.map(p=>p.evaluate(()=>{
   const manager=room.engine.pcManager;
   const instances=[['publisher',manager.publisher?._pc],['subscriber',manager.subscriber?._pc]].filter(([,pc])=>pc);
   if(instances.some(([,pc])=>!(pc instanceof RTCPeerConnection)))throw Error('actual native PC unavailable');
   return {mode:manager.mode,physicalCount:new Set(instances.map(([,pc])=>pc)).size,descriptions:instances.map(([role,pc])=>{
    const descriptor=pc.currentRemoteDescription??pc.remoteDescription;if(!descriptor)throw Error('actual negotiated remote description unavailable');
    const sections=[];let current;
    for(const line of descriptor.sdp.split(/\r?\n/)){
     if(line.startsWith('m=')){current={kind:line.slice(2).split(' ')[0],feedback:[],extensions:[],direction:null};sections.push(current);}
     else if(current&&/^a=(sendrecv|sendonly|recvonly|inactive)$/.test(line))current.direction=line.slice(2);
     else if(current&&line.startsWith('a=rtcp-fb:'))current.feedback.push(line.slice(10).replace(/^\d+ /,''));
     else if(current&&line.startsWith('a=extmap:'))current.extensions.push(line.split(/\s+/)[1]);
    }
    return {role,type:descriptor.type,sections};
   })};
  })));
  for(const captured of row.negotiation){
   assert.equal(captured.physicalCount,single?1:2,'actual physical PC count must match declared profile');
   assert.ok(single?captured.mode==='publisher-only':['publisher-primary','subscriber-primary'].includes(captured.mode),'actual SDK topology mode');
   const publisherVideo=captured.descriptions.filter(d=>d.role==='publisher').flatMap(d=>d.sections).filter(s=>s.kind==='video');
   assert.ok(publisherVideo.length>0&&publisherVideo.every(s=>s.feedback.includes('transport-cc')&&s.extensions.some(e=>e.includes('transport-wide-cc'))),'publisher TWCC receiver path must remain available in every mode');
   if(single){assert.ok(publisherVideo.every(s=>s.feedback.includes('goog-remb')&&s.extensions.some(e=>e.includes('abs-send-time'))),'singlePC publisher/subscriber union must not be globally stripped');continue;}
   const sendVideo=captured.descriptions.filter(d=>d.role==='subscriber'&&d.type==='offer').flatMap(d=>d.sections).filter(s=>s.kind==='video'&&s.direction==='sendonly');
   assert.ok(sendVideo.length>0,'actual subscriber server video offer missing');
   if(mode==='remote')assert.ok(sendVideo.every(s=>s.feedback.includes('goog-remb')&&!s.feedback.includes('transport-cc')&&s.extensions.some(e=>e.includes('abs-send-time'))));
   else if(mode==='interceptor')assert.ok(sendVideo.every(s=>s.feedback.includes('transport-cc')&&!s.feedback.includes('goog-remb')&&!s.extensions.some(e=>e.includes('abs-send-time'))));
   else assert.ok(sendVideo.every(s=>s.feedback.includes('transport-cc')),'preserved native path');
  }
  row.status='PASS';
  await Promise.all(pages.map(p=>p.evaluate(()=>interop.close())));await context.close();context=undefined;
  await api.deleteRoom(room);room=undefined;
 }
 evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.error=String(error).replace(/eyJ[\w-]+\.[\w-]+\.[\w-]+/g,'<token>');process.exitCode=1;}
finally {
 for(const cleanup of [async()=>context?.close(),async()=>browser?.close(),async()=>room&&api.deleteRoom(room),async()=>new Promise(r=>host.listening?host.close(r):r())])try{await cleanup();}catch(error){errors.push(String(error));}
 if(errors.length){evidence.status='FAIL';evidence.cleanupErrors=errors;process.exitCode=1;}
 evidence.finishedAt=new Date().toISOString();await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
}
