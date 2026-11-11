import {closeOwnedBrowser,observeChildStderr} from '../owned-browser-exit.mjs';
import {execFileSync} from 'node:child_process';
// Coordinator-owned unchanged Go/Rust standalone server; only this browser/HTTP fixture is owned here.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,writeFile,realpath} from 'node:fs/promises';
import {resolve} from 'node:path';
import {createServer} from 'node:http';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
const env=process.env;for(const key of ['POLICY_BROWSER_STDERR','POLICY_OWNERSHIP'])assert(env[key],key+' required');
const birth=pid=>execFileSync('/bin/ps',['-p',String(pid),'-o','lstart='],{encoding:'utf8'}).trim();
async function linuxStart(pid){if(process.platform!=='linux')return null;const value=await readFile('/proc/'+pid+'/stat','utf8');return value.slice(value.lastIndexOf(')')+1).trim().split(/\s+/)[19];}for(const key of ['INTEROP_PROJECT','LIVEKIT_URL','SENDDATA_OUTPUT','SENDDATA_DEPLOYMENT'])assert(env[key],key+' required');
assert.equal(env.SENDDATA_DEPLOYMENT,'standalone');
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
for(const [pkg,version]of [['livekit-client','2.23.0'],['livekit-server-sdk','2.19.2']])assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules',pkg,'package.json'),'utf8')).version,version);
const sdk=await readFile(require.resolve('livekit-client'));
const html=`<!doctype html><script src="/sdk.js"></script><script>
let room;const messages=[];window.fixture={async connect(url,token){room=new LivekitClient.Room({singlePeerConnection:true});room.on(LivekitClient.RoomEvent.DataReceived,(bytes,participant,kind,topic)=>messages.push({bytes:Array.from(bytes),sender:participant?.identity||'',kind,topic}));await room.connect(url,token);await room.localParticipant.publishData(new Uint8Array([1]),{reliable:true,topic:'fixture-ready'});return room.localParticipant.sid;},messages(){return messages;},async close(){await room?.disconnect();}};
</script>`;
const report={status:'RUNNING',scope:'actual public standalone SendData through unchanged SDK reliable data consumers; identity/SID/broadcast/nonce and room replacement; component tests separately cover caller lifetime/retry budgets',observations:[],cleanup_errors:[]};
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
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
await writeFile(env.SENDDATA_OUTPUT,JSON.stringify(report)+'\n',{flag:'wx',mode:0o600});
let host,browser,browserServer,finishBrowserStderr;const pages=[];const room='local-data-'+randomUUID();const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret';const api=new RoomServiceClient(env.LIVEKIT_URL.replace(/^ws/,'http'),key,secret);
async function token(identity){const t=new AccessToken(key,secret,{identity});t.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});return t.toJwt();}
const admin=new AccessToken(key,secret);admin.addGrant({roomAdmin:true,room});const bearer=await admin.toJwt();
async function send(payload,fields={}){const response=await fetch(env.LIVEKIT_URL.replace(/^ws/,'http')+'/twirp/livekit.RoomService/SendData',{method:'POST',headers:{authorization:'Bearer '+bearer,'content-type':'application/json'},body:JSON.stringify({room,data:Buffer.from(payload).toString('base64'),kind:0,topic:'actual-senddata',...fields})});const body=await response.json();assert.equal(response.status,200,JSON.stringify(body));}
async function observed(page,payload,count=1){await page.waitForFunction(args=>fixture.messages().filter(m=>m.topic==='actual-senddata'&&m.bytes.length===args.payload.length&&m.bytes.every((x,i)=>x===args.payload[i])).length>=args.count,{payload:Array.from(payload),count},{timeout:15000});}
async function count(page,payload){return page.evaluate(payload=>fixture.messages().filter(m=>m.topic==='actual-senddata'&&m.bytes.length===payload.length&&m.bytes.every((x,i)=>x===payload[i])).length,Array.from(payload));}
try{
 host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 await api.createRoom({name:room});browserServer=await chromium.launchServer({...(env.BROWSER_EXECUTABLE?{executablePath:env.BROWSER_EXECUTABLE}:{channel:env.BROWSER_CHANNEL||'chrome'}),headless:true,args:['--autoplay-policy=no-user-gesture-required']});finishBrowserStderr=observeChildStderr(browserServer.process());const child=browserServer.process();assert(child?.pid,'actual owned Chromium child required');report.ownership={nodePid:process.pid,nodeBirth:birth(process.pid),nodeExecutable:process.execPath,browserPid:child.pid,browserBirth:birth(child.pid),browserExecutable:process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile,browserLaunchEntry:child.spawnfile,browserExecutableSha256:hash(await readFile(process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile)),browserUid:process.getuid(),nodeLinuxStart:await linuxStart(process.pid),browserLinuxStart:await linuxStart(child.pid)};await writeFile(env.POLICY_OWNERSHIP,JSON.stringify(report.ownership)+'\n',{flag:'wx',mode:0o600});browser=await chromium.connect(browserServer.wsEndpoint());report.browser=browser.version();const sids=[];
 for(const identity of ['first','second']){const page=await browser.newPage();pages.push(page);await page.goto('http://127.0.0.1:'+host.address().port);sids.push(await page.evaluate(args=>fixture.connect(...args),[env.LIVEKIT_URL,await token(identity)]));}
 const directed=Buffer.from([0,255,7,13]);await send(directed,{destination_identities:['first']});await observed(pages[0],directed);await sleep(250);assert.equal(await count(pages[1],directed),0);
 const sidOnly=Buffer.from([42,0,254]);await send(sidOnly,{destination_sids:[sids[1]]});await observed(pages[1],sidOnly);await sleep(250);assert.equal(await count(pages[0],sidOnly),0);
 const broadcast=randomBytes(60000);await send(broadcast);await Promise.all(pages.map(page=>observed(page,broadcast)));report.observations.push({case:'broadcast',bytes:broadcast.length,sha256:hash(broadcast),receivers:2});
 const duplicate=Buffer.from([5,4,3,2,1]),nonce=randomBytes(16).toString('base64');await send(duplicate,{nonce});await Promise.all(pages.map(page=>observed(page,duplicate)));await send(duplicate,{nonce});await sleep(500);for(const page of pages)assert.equal(await count(page,duplicate),1,'actual nonce suppresses duplicate accepted sends');
 await Promise.all(pages.map(page=>page.evaluate(()=>fixture.close())));await api.deleteRoom(room);await api.createRoom({name:room});
 const replacement=await pages[0].evaluate(args=>fixture.connect(...args),[env.LIVEKIT_URL,await token('first')]);assert.notEqual(replacement,sids[0]);
 const stale=Buffer.from([21,22,23]);await send(stale,{destination_sids:[sids[0]]});await sleep(500);assert.equal(await count(pages[0],stale),0,'old room participant SID must not resolve reusable identity');
 const current=Buffer.from([31,32,33]);await send(current,{destination_sids:[replacement]});await observed(pages[0],current);
 report.observations.push({case:'identity-and-sid',passed:true},{case:'nonce-deduplication',passed:true},{case:'room-generation-replacement',passed:true});report.status='PASS';
}catch(error){report.status='FAIL';report.failure=failureDetails(error);}
finally{for(const page of pages){try{await page.evaluate(()=>fixture.close());await page.close();}catch{report.cleanup_errors.push('owned page cleanup failed');}}{const closed=await closeOwnedBrowser(browser,browserServer);report.browserExit=closed.exit;report.browserCleanup=closed;report.cleanup_errors.push(...closed.failures);if(browserServer&&(!closed.exit||closed.exit.code!==0||closed.exit.signal!=null))report.cleanup_errors.push('actual-owned-browser-exit-not-zero');if(finishBrowserStderr){const stderr=finishBrowserStderr();report.browserStderr=stderr.metadata;try{await writeFile(env.POLICY_BROWSER_STDERR,stderr.bytes,{flag:'wx',mode:0o600});}catch{report.cleanup_errors.push('owned-browser-stderr-persist');}}}try{await api.deleteRoom(room);}catch(error){if(error?.code!=='not_found')report.cleanup_errors.push('owned room cleanup failed');}if(host)await new Promise(r=>host.close(r));if(report.cleanup_errors.length)report.status='FAIL';await writeFile(env.SENDDATA_OUTPUT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status!=='PASS')process.exitCode=1;}
