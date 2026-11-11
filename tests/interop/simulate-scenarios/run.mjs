// Official SDK simulateScenario differential: one server per process (pinned Go or Rust), same SDK bytes.
// For every scenario: fresh room, client A publishes audio, client B subscribes; A runs room.simulateScenario(...)
// and the observable outcome is normalized (connection states, SID continuity, what B sees, speaker events,
// audio and reliable data afterwards, selected candidate protocol).
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {LossyUdpProxy} from '../media-recovery/lossy-proxy.mjs';
import {coalesceRejoinSignal} from './rejoin-signal.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const deps=process.env.INTEROP_DEPENDENCY_ROOT;assert.ok(deps&&isAbsolute(deps));
const require=createRequire(join(deps,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const {SignalRequest}=require('@livekit/protocol');
const out=process.env.INTEROP_OUTPUT_DIR;assert.ok(out&&isAbsolute(out));await mkdir(out,{recursive:false});
const url=process.env.LIVEKIT_URL,single=process.env.SINGLE_PC!=='false';
const SCENARIOS=(process.env.SCENARIOS||'speaker,signal-reconnect,resume-reconnect,full-reconnect,node-failure,migration,server-leave,disconnect-signal-on-resume,disconnect-signal-on-resume-no-messages,leave-full-reconnect,force-tcp,force-tls').split(',');
const api=new RoomServiceClient(url.replace(/^ws/,'http'),'devkey','secret');
const sdk=await readFile(require.resolve('livekit-client'));const page=await readFile(join(here,'client.html'));
const sha=b=>createHash('sha256').update(b).digest('hex');
const host=createServer((q,r)=>{const b=q.url==='/'?page:q.url==='/sdk.js'?sdk:null;if(!b){r.writeHead(404);r.end();return;}r.writeHead(200,{'content-type':q.url==='/'?'text/html':'text/javascript','cache-control':'no-store'});r.end(b);});
await new Promise(r=>host.listen(0,'127.0.0.1',r));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function poll(fn,timeout){const end=Date.now()+timeout;while(Date.now()<end){const v=await fn();if(v)return v;await sleep(200);}return null;}
const turnTlsPort=Number(process.env.LK_SIM_TURN_TLS_PORT||0);
const turnUdpPort=Number(process.env.LK_SIM_TURN_UDP_PORT||0);
const tlsSpki=process.env.LK_SIM_TURN_TLS_SPKI;
assert.ok(!SCENARIOS.includes('transport-switch') || (turnTlsPort && tlsSpki), 'transport-switch requires an owned TLS listener and certificate pin');
const browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chromium',headless:true,
 args:['--autoplay-policy=no-user-gesture-required',...(tlsSpki?[`--ignore-certificate-errors-spki-list=${tlsSpki}`]:[])]});
const evidence={sdkSha256:sha(sdk),single,url,browser:browser.version(),scenarios:{}};
async function client(room,identity,udpRoute,signalDelivery){const c=await browser.newContext();const p=await c.newPage();const errors=[];p.on('pageerror',e=>errors.push(String(e)));
 if(signalDelivery)await coalesceRejoinSignal(p,SignalRequest,signalDelivery);
 if(turnTlsPort)await p.addInitScript(port=>{window.turnTlsPort=port;},turnTlsPort);
 if(udpRoute)await p.addInitScript(({upstream,route})=>{window.turnUdpUpstream=upstream;window.turnUdpRoute=route;},{upstream:turnUdpPort,route:udpRoute});
 await p.goto(`http://127.0.0.1:${host.address().port}/`);
 const t=new AccessToken('devkey','secret',{identity,ttl:'10m'});t.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});
 const sid=await p.evaluate(o=>sim.connect(o),{url,token:await t.toJwt(),single});return {c,p,sid,errors};}
async function bidirectional(A,B,step,name){
 const [beforeA,beforeB]=await Promise.all([A,B].map(x=>x.p.evaluate(()=>sim.inboundAudioPackets())));
 [step.audioA,step.audioB]=await Promise.all([[A,'b',beforeA],[B,'a',beforeB]].map(async ([client,peer,before])=>
  !!await poll(()=>client.p.evaluate(async ({peer,before})=>sim.rms(peer)>0.01&&(await sim.inboundAudioPackets())>=before+5,{peer,before}),5000)));
 await A.p.evaluate(text=>sim.send(text),`after-${name}-a`);
 await B.p.evaluate(text=>sim.send(text),`after-${name}-b`);
 step.dataA=!!await poll(async()=>(await A.p.evaluate(()=>sim.report())).data.some(d=>d.from==='b'&&d.text===`after-${name}-b`),5000);
 step.dataB=!!await poll(async()=>(await B.p.evaluate(()=>sim.report())).data.some(d=>d.from==='a'&&d.text===`after-${name}-a`),5000);
 assert.ok(step.audioA&&step.audioB&&step.dataA&&step.dataB,`${name}: bidirectional media and reliable data must recover`);
}
async function switchTransport(A,B,row){
 await B.p.evaluate(()=>sim.publishAudio());
 const baseline=await poll(async()=>{
  const active=await A.p.evaluate(()=>sim.protocols());
  return active.length===(single?1:2)&&active.every(p=>p.connection==='connected'&&p.local==='udp'&&!p.relay)?active:null;
 },15000);
 assert.ok(baseline,'initial media must select direct UDP on each PC');
 row.transportSteps=[{name:'udp',selected:baseline}];
 let previous=baseline,previousSid=A.sid;
 for(const [name,scenario,qualify] of [
  ['tcp','force-tcp',p=>p.local==='tcp'&&p.remote==='tcp'&&!p.relay],
  ['tls','force-tls',p=>p.relay&&p.relayProtocol==='tls'&&p.turnUrl===`turns:turn.localhost:${turnTlsPort}?transport=tcp`],
 ]){
  const step={name};row.transportSteps.push(step);
  await A.p.evaluate(s=>sim.simulate(s),scenario);
  let latest;
  const selected=await poll(async()=>{
   latest=await A.p.evaluate(()=>sim.protocols());
   return latest.length===previous.length&&latest.every(p=>p.connection==='connected'&&p.localUfrag&&p.remoteUfrag
      &&previous.every(old=>old.localUfrag!==p.localUfrag&&old.remoteUfrag!==p.remoteUfrag)&&qualify(p))
      &&await A.p.evaluate(()=>sim.report().state==='connected')?latest:null;
  },20000);
  step.selected=selected??latest;
  assert.ok(selected,`${name}: expected fresh local and remote ICE credentials and the requested route`);
  step.replacedPcs=selected.filter(p=>previous.every(old=>old.pcIndex!==p.pcIndex)).length;
  const a=await A.p.evaluate(()=>sim.report());
  assert.ok(a.sid,`${name}: participant identity missing`);
  step.sidChanged=a.sid!==previousSid;
  assert.equal(a.state,'connected');
  await bidirectional(A,B,step,name);
  previous=selected;
  previousSid=a.sid;
 }
}
async function networkChange(A,B,row,[oldRoute,newRoute]){
 const initial=await A.p.evaluate(()=>sim.protocols());
 const selectedRoute=(p,route)=>p.connection==='connected'&&p.relay&&p.relayProtocol==='udp'
  &&p.turnUrl===`turn:127.0.0.1:${route.port}?transport=udp`;
 row.networkChange={initial};
 assert.ok(initial.length===(single?1:2)&&initial.every(p=>selectedRoute(p,oldRoute)),'initial media bypassed the owned TURN/UDP route');
 await B.p.evaluate(()=>sim.publishAudio());
 await bidirectional(A,B,row.networkChange.before={},'before-network-change');
 oldRoute.set('up',{blackout:true});
 oldRoute.set('down',{blackout:true});
 await A.p.evaluate(port=>{window.turnUdpRoute=port;},newRoute.port);
 assert.ok(await poll(()=>oldRoute.stats().counters.up.dropped>0,3000),'old media route did not observe loss');
 await A.p.evaluate(()=>sim.simulate('signal-reconnect'));
 const selected=await poll(async()=>{
  const latest=await A.p.evaluate(()=>sim.protocols());
  row.networkChange.restored=latest;
  return latest.length===initial.length&&latest.every(p=>{
   const old=initial.find(o=>o.pcIndex===p.pcIndex);
   return old&&selectedRoute(p,newRoute)&&p.localUfrag!==old.localUfrag&&p.remoteUfrag!==old.remoteUfrag;
  })&&await A.p.evaluate(()=>sim.report().state==='connected');
 },20000);
 assert.ok(selected,'expected an ICE restart on the same PCs over the replacement TURN/UDP route');
 assert.equal((await A.p.evaluate(()=>sim.report())).sid,A.sid,'an ICE restart must retain participant SID');
 row.networkChange.sidRetained=true;
 await bidirectional(A,B,row.networkChange.after={},'network-change');
 row.networkChange.proxies=[oldRoute.stats(),newRoute.stats()];
}
for(const scenario of SCENARIOS){
 const room=`sim-${scenario}-${Date.now()}`;await api.createRoom({name:room,emptyTimeout:10,departureTimeout:5});
 const row={scenario};let A,B;
 const routes=[];
 try{
  if(scenario==='ice-network-change'){
   assert.ok(turnUdpPort,'ice-network-change requires an owned TURN/UDP listener');
   for(let i=0;i<2;i++){const proxy=new LossyUdpProxy({upstreamPort:turnUdpPort});routes.push(proxy);await proxy.start();}
  }
  if(process.env.LK_SIM_COALESCE_REJOIN_SDP==='true')row.signalDelivery=[];
  A=await client(room,'a',routes[0]?.port,row.signalDelivery);B=await client(room,'b');
  await A.p.evaluate(()=>sim.publishAudio());
  row.audioBefore=!!await poll(()=>B.p.evaluate(()=>sim.rms('a')>0.01),15000);
  await B.p.evaluate(()=>sim.send('pre'));row.dataBefore=!!await poll(async()=>(await A.p.evaluate(()=>sim.report())).data.some(d=>d.text==='pre'),5000);
  const t0=await A.p.evaluate(()=>performance.now()),tb=await B.p.evaluate(()=>performance.now());
  if(scenario==='transport-switch')await switchTransport(A,B,row);
  else if(scenario==='ice-network-change')await networkChange(A,B,row,routes);
  else await A.p.evaluate(s=>sim.simulate(s,s==='speaker'?3:undefined),scenario).catch(e=>{row.simulateError=String(e);});
  // settle: A ends connected or disconnected and stays there for 2 s
  await sleep(2000);let stable=0,last;
  for(let i=0;i<115&&stable<10;i++){const s=(await A.p.evaluate(()=>sim.report())).state;stable=(s===last&&(s==='connected'||s==='disconnected'))?stable+1:0;last=s;await sleep(200);}
  if(scenario==='speaker')await sleep(4000);
  const a=await A.p.evaluate(()=>sim.report()),b=await B.p.evaluate(()=>sim.report());
  const after=(evs,t)=>evs.filter(e=>e.at>=t);
  const ae=after(a.events,t0),be=after(b.events,tb);
  row.finalState=a.state;row.sidChanged=a.sid!==A.sid;
  row.aStates=ae.filter(e=>e.name==='ConnectionStateChanged').map(e=>e.value).filter((v,i,x)=>v!==x[i-1]);
  row.aEvents=[...new Set(ae.filter(e=>!['ConnectionStateChanged','simulate','ActiveSpeakersChanged','TrackSubscribed','TrackUnsubscribed','ParticipantConnected','ParticipantDisconnected'].includes(e.name)).map(e=>e.name))].sort();
  row.disconnectReason=ae.find(e=>e.name==='Disconnected')?.reason??null;
  row.bSawLeave=be.some(e=>e.name==='ParticipantDisconnected'&&e.identity==='a');
  row.bSawRejoin=be.some(e=>e.name==='ParticipantConnected'&&e.identity==='a');
  row.bSubscribedAfter=be.some(e=>e.name==='TrackSubscribed'&&e.identity==='a');
  row.speakerOnA=ae.some(e=>e.name==='ActiveSpeakersChanged'&&e.speakers.includes('a'));
  row.speakerOnB=be.some(e=>e.name==='ActiveSpeakersChanged'&&e.speakers.includes('a'));
  row.speakerClearedOnB=scenario==='speaker'?be.some(e=>e.name==='ActiveSpeakersChanged'&&!e.speakers.includes('a')):null;
  if(a.state==='connected'){
   row.audioAfter=!!await poll(()=>B.p.evaluate(()=>sim.rms('a')>0.01),10000);
   await A.p.evaluate(()=>sim.send('post'));row.dataAfter=!!await poll(async()=>(await B.p.evaluate(()=>sim.report())).data.some(d=>d.text==='post'),5000);
   row.protocols=[...new Set((await A.p.evaluate(()=>sim.protocols())).map(x=>x.local))].sort();
  }else{row.audioAfter=null;row.dataAfter=null;row.protocols=null;}
  row.raw={a:{sidBefore:A.sid,sidAfter:a.sid,events:ae},b:{events:be}};
  row.pageErrors=[...A.errors,...B.errors];
 }catch(e){row.error=String(e);}
 finally{for(const x of [A,B])if(x){await x.p.evaluate(()=>sim.close()).catch(e=>{row.cleanupError=String(e);});await x.c.close();}
  await Promise.all(routes.map(route=>route.close()));
  try{await api.deleteRoom(room);
   row.roomDeleted=!!await poll(async()=>!(await api.listRooms([room])).some(x=>x.name===room),5000);
  }catch(e){row.cleanupError=String(e);}}
 evidence.scenarios[scenario]=row;await writeFile(join(out,'evidence.json'),JSON.stringify(evidence,null,2));
 console.log(scenario,JSON.stringify(Object.fromEntries(Object.entries(row).filter(([k])=>k!=='raw'))));
}
await browser.close();host.close();
