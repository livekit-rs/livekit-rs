import {closeOwnedBrowser,observeChildStderr} from '../owned-browser-exit.mjs';
import {execFileSync} from 'node:child_process';
// Coordinator-owned actual fixed-Go/Rust standalone server. Starts no server,
// Redis or worker. Unchanged pinned browser SDK is the real method consumer.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile,writeFile,realpath } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createServer } from 'node:http';
import { randomUUID,createHash } from 'node:crypto';
const env=process.env;for(const key of ['POLICY_BROWSER_STDERR','POLICY_OWNERSHIP'])assert(env[key],key+' required');
const birth=pid=>execFileSync('/bin/ps',['-p',String(pid),'-o','lstart='],{encoding:'utf8'}).trim();
async function linuxStart(pid){if(process.platform!=='linux')return null;const value=await readFile('/proc/'+pid+'/stat','utf8');return value.slice(value.lastIndexOf(')')+1).trim().split(/\s+/)[19];}
for(const key of ['INTEROP_PROJECT','LIVEKIT_URL','RPC_OUTPUT','RPC_DEPLOYMENT'])assert(env[key],`${key} required`);
assert.equal(env.RPC_DEPLOYMENT,'standalone','this profile must use an actual no-Redis server configuration');
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-client/package.json'),'utf8')).version,'2.23.0');assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8')).version,'2.19.2');
const sdk=await readFile(require.resolve('livekit-client'));
const html=`<!doctype html><script src="/sdk.js"></script><script>
let room,context,oscillator,localAudio;const received=[],calls=[];
window.fixture={async connect(url,token,publish){
 room=new LivekitClient.Room({singlePeerConnection:true});
 room.localParticipant.registerRpcMethod('fixture.budget',async options=>{calls.push({kind:'budget',id:options.requestId,budget:options.responseTimeout,at:performance.now()});return String(options.responseTimeout);});
 room.localParticipant.registerRpcMethod('fixture.delayed',async options=>{const call={kind:'delayed',id:options.requestId,budget:options.responseTimeout,at:performance.now(),finished:false};calls.push(call);await new Promise(r=>setTimeout(r,3500));call.finished=true;return 'accepted:'+options.payload;});
 room.on(LivekitClient.RoomEvent.TrackSubscribed,track=>{if(track.kind==='audio'){received.push(track);const el=track.attach();el.muted=true;document.body.append(el);el.play().catch(()=>{});}});
 await room.connect(url,token);if(publish){context=new AudioContext();await context.resume();oscillator=context.createOscillator();oscillator.frequency.value=440;const dest=context.createMediaStreamDestination();oscillator.connect(dest);oscillator.start();localAudio=new LivekitClient.LocalAudioTrack(dest.stream.getAudioTracks()[0]);await room.localParticipant.publishTrack(localAudio);}
},calls(){return calls.map(x=>({...x}));},async packets(){let packets=0;for(const track of received){for(const item of (await track.receiver.getStats()).values()){if(item.type==='inbound-rtp')packets+=item.packetsReceived||0;}}return packets;},async close(){await room?.disconnect();localAudio?.stop();oscillator?.stop();await context?.close();}};
</script>`;
let browser,browserServer,finishBrowserStderr,host,pages=[];const room='rpc-'+randomUUID();const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret';
const api=new RoomServiceClient(env.LIVEKIT_URL.replace(/^ws/,'http'),key,secret);
const report={status:'RUNNING',scope:'actual standalone typed PSRPC policy + genuine subscribed-media RR response budget; not all RPC failure classes',sdk:'2.23.0',server_sdk:'2.19.2',observations:[],cleanup_errors:[]};
const hash=x=>createHash('sha256').update(x).digest('hex');
// Keep bounded actionable failure diagnostics while preserving hashes of the
// original bytes. Do not leak bearer tokens, URL credentials or fixture keys.
const sanitize=value=>{
 let text=String(value).replace(/\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,'[REDACTED-JWT]')
  .replace(/(Bearer\s+)[^\s"'<>]+/gi,'$1[REDACTED]')
  .replace(/([?&](?:access_token|token|api_key|api_secret)=)[^\s&#"']+/gi,'$1[REDACTED]');
 for(const secret of [env.LIVEKIT_API_SECRET,'secret'])if(secret)text=text.split(secret).join('[REDACTED-FIXTURE-SECRET]');
 return text;
};
function failureDetails(error){const message=String(error?.message??error),stack=String(error?.stack??'');return{name:sanitize(error?.name??'Error').slice(0,256),message_sha256:hash(message),stack_sha256:hash(stack),message:sanitize(message).slice(0,8192),stack:sanitize(stack).slice(0,24576),message_truncated:sanitize(message).length>8192,stack_truncated:sanitize(stack).length>24576};}
await writeFile(env.RPC_OUTPUT,JSON.stringify(report)+'\n',{flag:'wx',mode:0o600});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
try{
 host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 await api.createRoom({name:room});browserServer=await chromium.launchServer({...(env.BROWSER_EXECUTABLE?{executablePath:env.BROWSER_EXECUTABLE}:{channel:env.BROWSER_CHANNEL||'chrome'}),headless:true,args:['--autoplay-policy=no-user-gesture-required']});finishBrowserStderr=observeChildStderr(browserServer.process());const child=browserServer.process();assert(child?.pid,'actual owned Chromium child required');report.ownership={nodePid:process.pid,nodeBirth:birth(process.pid),nodeExecutable:process.execPath,browserPid:child.pid,browserBirth:birth(child.pid),browserExecutable:process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile,browserLaunchEntry:child.spawnfile,browserExecutableSha256:hash(await readFile(process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile)),browserUid:process.getuid(),nodeLinuxStart:await linuxStart(process.pid),browserLinuxStart:await linuxStart(child.pid)};await writeFile(env.POLICY_OWNERSHIP,JSON.stringify(report.ownership)+'\n',{flag:'wx',mode:0o600});browser=await chromium.connect(browserServer.wsEndpoint());report.browser=browser.version();
 for(const [identity,publish]of [['receiver',false],['publisher',true]]){const token=new AccessToken(key,secret,{identity});token.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true});const page=await browser.newPage();pages.push(page);await page.goto('http://127.0.0.1:'+host.address().port);await page.evaluate(args=>fixture.connect(...args),[env.LIVEKIT_URL,await token.toJwt(),publish]);}
 const admin=new AccessToken(key,secret);admin.addGrant({roomAdmin:true,room});const bearer=await admin.toJwt();
 async function rpc(method,response_timeout_ms){const response=await fetch(env.LIVEKIT_URL.replace(/^ws/,'http')+'/twirp/livekit.RoomService/PerformRpc',{method:'POST',headers:{authorization:'Bearer '+bearer,'content-type':'application/json'},body:JSON.stringify({room,destination_identity:'receiver',method,payload:'original-sdk',response_timeout_ms})});const body=await response.json();assert.equal(response.status,200,JSON.stringify(body));return body.payload;}
 await pages[0].waitForFunction(async()=>await fixture.packets()>100,null,{timeout:20000});
 const until=Date.now()+20000;let reduced;
 while(Date.now()<until){const budget=Number(await rpc('fixture.budget',10000));assert(Number.isInteger(budget)&&budget>0&&budget<=10000);report.observations.push({budget,packets:await pages[0].evaluate(()=>fixture.packets())});if(budget<10000){reduced=budget;break;}await sleep(1000);}
 assert(reduced,'actual subscribed-media RR must produce a nonzero wire-budget reduction; no ICE/TWCC/signal substitute');
 const before=(await pages[0].evaluate(()=>fixture.calls())).length;const started=Date.now();assert.equal(await rpc('fixture.delayed',6000),'accepted:original-sdk');
 const calls=(await pages[0].evaluate(()=>fixture.calls())).slice(before).filter(x=>x.kind==='delayed');assert.equal(calls.length,2,'default 3s first attempt expires; second 5s attempt accepts 3.5s original SDK method');assert.notEqual(calls[0].id,calls[1].id,'each accepted attempt has its own actual RPC request id');assert(calls.every(x=>x.finished),'earlier accepted handler must finish despite caller retry');assert(calls.every(x=>x.budget>3500&&x.budget<6000));
 report.delayed={actual_invocations:calls.length,distinct_request_ids:true,all_accepted_finished:true,budgets:calls.map(x=>x.budget),elapsed_ms:Date.now()-started};report.status='PASS';
}catch(error){report.status='FAIL';report.failure=failureDetails(error);}
finally{report.last_calls=[];for(const page of pages){try{report.last_calls.push(await page.evaluate(()=>fixture.calls()));await page.evaluate(()=>fixture.close());await page.close();}catch{report.cleanup_errors.push('owned SDK cleanup failed');}}{const closed=await closeOwnedBrowser(browser,browserServer);report.browserExit=closed.exit;report.browserCleanup=closed;report.cleanup_errors.push(...closed.failures);if(browserServer&&(!closed.exit||closed.exit.code!==0||closed.exit.signal!=null))report.cleanup_errors.push('actual-owned-browser-exit-not-zero');if(finishBrowserStderr){const stderr=finishBrowserStderr();report.browserStderr=stderr.metadata;try{await writeFile(env.POLICY_BROWSER_STDERR,stderr.bytes,{flag:'wx',mode:0o600});}catch{report.cleanup_errors.push('owned-browser-stderr-persist');}}}try{await api.deleteRoom(room);}catch(error){if(error?.code!=='not_found')report.cleanup_errors.push('owned room cleanup failed');}if(host)await new Promise(r=>host.close(r));if(report.cleanup_errors.length)report.status='FAIL';await writeFile(env.RPC_OUTPUT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status!=='PASS')process.exitCode=1;}
