/** Optional independent functional profile. Does not import or alter run.mjs gates. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { readFile,mkdir,writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname,join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { AccessToken,RoomServiceClient } from 'livekit-server-sdk';
import { SignalResponse } from '@livekit/protocol';

const root=dirname(fileURLToPath(import.meta.url)),require=createRequire(import.meta.url);
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880';
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const pc=process.env.FUNCTIONAL_PC||'both',e2ee=process.env.FUNCTIONAL_E2EE||'both';
assert.ok(['single','dual','both'].includes(pc));assert.ok(['true','false','both'].includes(e2ee));
const modes=(pc==='both'?[true,false]:[pc==='single']).flatMap(single=>(e2ee==='both'?[false,true]:[e2ee==='true']).map(encryption=>({single,e2ee:encryption})));
const sdk=await readFile(require.resolve('livekit-client')),worker=await readFile(fileURLToPath(import.meta.resolve('livekit-client/e2ee-worker'))),html=await readFile(join(root,'functional-data-track.html'));
const sdkPackage=JSON.parse(await readFile(join(dirname(require.resolve('livekit-client')),'../package.json')));
assert.equal(sdkPackage.version,'2.23.0','run requires the existing locked official SDK');
const host=createServer((req,res)=>{const content=req.url==='/sdk.js'?sdk:req.url==='/e2ee-worker.mjs'?worker:html;res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(content);});
await new Promise(resolve=>host.listen(0,'127.0.0.1',resolve));
const pageUrl=`http://127.0.0.1:${host.address().port}`;
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret);
const evidence={profile:'functional-data-track',sdk:sdkPackage.version,status:'RUNNING',startedAt:new Date().toISOString(),cases:[],scope:'Unmodified official SDK public data-track APIs; browser channel observation forwards native calls unchanged. Not congestion/performance, schema/blob or cross-SDK acceptance.'};
const output=join(root,'output','functional-data-track.json');await mkdir(dirname(output),{recursive:true});
let browser,context,currentRoom;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const redactError=value=>String(value).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>').replace(/([?&](?:access_token|token|api_secret)=)[^&\s]+/gi,'$1<redacted>');
async function until(predicate,message){const deadline=Date.now()+10000;while(Date.now()<deadline){if(await predicate())return;await sleep(25);}throw new Error(message);}
const digest=(length,seed)=>createHash('sha256').update(Uint8Array.from({length},(_,i)=>(i*31+seed)%256)).digest('hex');
try {
 browser=await chromium.launch({headless:true,args:['--no-sandbox']});evidence.browser=browser.version();
 for(const mode of modes){
  const result={...mode,status:'RUNNING',steps:[],signals:[],decodeErrors:[]};evidence.cases.push(result);
  context=await browser.newContext();const pages=await Promise.all([context.newPage(),context.newPage()]);
  for(const [client,page] of pages.entries())page.on('websocket',socket=>socket.on('framereceived',frame=>{
   try{const response=typeof frame.payload==='string'?SignalResponse.fromJsonString(frame.payload):SignalResponse.fromBinary(frame.payload);const one=response.message;
    if(['publishDataTrackResponse','unpublishDataTrackResponse','dataTrackSubscriberHandles','requestResponse'].includes(one.case)){
     if(result.signals.length>=256)throw new Error('bounded signal capture overflow');
     const value=one.value;
     result.signals.push({client,type:one.case,info:value.info?{sid:value.info.sid,name:value.info.name,encryption:value.info.encryption}:undefined,handles:value.subHandles?Object.fromEntries(Object.entries(value.subHandles).map(([h,t])=>[h,{trackSid:t.trackSid,publisherIdentity:t.publisherIdentity}])):undefined,reason:value.reason});
    }
   }catch(error){result.decodeErrors.push(String(error));}
  }));
  currentRoom=`functional-data-track-${Date.now()}-${mode.single?'single':'dual'}-${mode.e2ee?'encrypted':'plain'}`;await api.createRoom({name:currentRoom});
  const identities=['functional-a','functional-b'];
  await Promise.all(pages.map(async(page,i)=>{await page.goto(pageUrl);const token=new AccessToken(key,secret,{identity:identities[i]});token.addGrant({roomJoin:true,room:currentRoom,canPublish:true,canSubscribe:true,canPublishData:true});await page.evaluate(args=>functional.connect(args),{url,token:await token.toJwt(),...mode});}));
  const names=['opaque-a','opaque-b'];
  const publications=await Promise.all(pages.map((page,i)=>page.evaluate(name=>functional.publish(name),names[i])));result.publications=publications;
  assert.ok(publications.every(p=>p.sid.startsWith('DTR_')&&p.usesE2ee===mode.e2ee));
  await Promise.all(pages.map((page,i)=>page.waitForFunction(({identity,name})=>functional.hasRemote(identity,name),{identity:identities[1-i],name:names[1-i]},{timeout:10000})));
  await Promise.all(pages.map((page,i)=>page.evaluate(({identity,name})=>functional.subscribe(identity,name),{identity:identities[1-i],name:names[1-i]})));
  const handlesFor=client=>result.signals.filter(s=>s.client===client&&s.type==='dataTrackSubscriberHandles').at(-1)?.handles||{};
  await until(()=>[0,1].every(i=>Object.values(handlesFor(i)).some(h=>h.trackSid===publications[1-i].sid)),'subscriber handle maps not established');
  for(const [index,length] of [37,4096,32768].entries()){
   for(let i=0;i<2;i++){
    const seed=10+i*20+index,timestamp=await pages[i].evaluate(({name,length,seed})=>functional.push(name,length,seed),{name:names[i],length,seed});
    await pages[1-i].waitForFunction(({name,count})=>functional.received(name).length>=count,{name:names[i],count:index+1},{timeout:10000});
    const frame=(await pages[1-i].evaluate(name=>functional.received(name),names[i]))[index];
    assert.deepEqual(frame,{bytes:length,sha256:digest(length,seed),userTimestamp:timestamp});
   }
  }
  result.steps.push('bidirectional exact bytes and bigint timestamp extension, including fragmented 32768-byte frame');
  await pages[0].evaluate(()=>functional.permission(false));
  await until(()=>Object.keys(handlesFor(1)).length===0,'permission revocation did not clear subscriber mappings');
  const before=(await pages[1].evaluate(name=>functional.received(name),names[0])).length;
  await pages[0].evaluate(name=>functional.push(name,37,99),names[0]);await sleep(500);
  assert.equal((await pages[1].evaluate(name=>functional.received(name),names[0])).length,before,'revoked data still delivered');
  await pages[0].evaluate(()=>functional.permission(true));
  await until(()=>Object.values(handlesFor(1)).some(h=>h.trackSid===publications[0].sid),'permission regrant did not restore mapping');
  const timestamp=await pages[0].evaluate(name=>functional.push(name,37,101),names[0]);
  await pages[1].waitForFunction(({name,count})=>functional.received(name).length>count,{name:names[0],count:before},{timeout:10000});
  const restored=(await pages[1].evaluate(name=>functional.received(name),names[0])).at(-1);assert.deepEqual(restored,{bytes:37,sha256:digest(37,101),userTimestamp:timestamp});
  result.steps.push('publisher subscription ACL revoke blocks frames; regrant restores real receive');
  await pages[0].evaluate(name=>functional.unpublish(name),names[0]);
  await pages[1].waitForFunction(({identity,name})=>!functional.hasRemote(identity,name),{identity:identities[0],name:names[0]},{timeout:10000});
  assert.equal((await pages[0].evaluate(name=>functional.pushUnpublished(name),names[0])).rejected,true);
  assert.ok(result.signals.some(s=>s.client===0&&s.type==='unpublishDataTrackResponse'));
  result.steps.push('unpublish acknowledged, remote publication removed, later SDK push rejected');
  result.reports=await Promise.all(pages.map(page=>page.evaluate(()=>functional.report())));
  for(const report of result.reports){assert.equal(report.overflow,false);assert.deepEqual(report.errors,[]);const channels=report.channels.filter(c=>c.label==='_data_track');assert.ok(channels.length>0);assert.ok(channels.every(c=>!c.ordered&&c.maxRetransmits===0));assert.ok(channels.some(c=>c.sent>0)&&channels.some(c=>c.received>0),'independent data-track channels carry both directions, including distinct dual PCs');assert.ok(report.packets.length>0);assert.ok(report.packets.every(p=>!p.error&&p.e2ee===mode.e2ee));assert.ok(report.packets.some(p=>p.userTimestamp!==null));}
  assert.deepEqual(result.decodeErrors,[]);result.status='PASS';
  await Promise.all(pages.map(page=>page.evaluate(()=>functional.close())));await context.close();context=undefined;await api.deleteRoom(currentRoom);currentRoom=undefined;
 }
 evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.error={name:error.name,message:redactError(error.message)};if(context){const pages=context.pages();evidence.failedReports=await Promise.all(pages.map(page=>page.evaluate(()=>functional.report()).catch(()=>({unavailable:true}))));}process.exitCode=1;
}finally{await context?.close();await browser?.close();if(currentRoom)await api.deleteRoom(currentRoom).catch(()=>{});await new Promise(resolve=>host.close(resolve));evidence.finishedAt=new Date().toISOString();await writeFile(output,JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({status:evidence.status,profile:evidence.profile,cases:evidence.cases.map(c=>({single:c.single,e2ee:c.e2ee,status:c.status})),output}));}
