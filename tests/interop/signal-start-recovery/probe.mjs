// Original pinned browser SDK; no SDP, SignalRequest or worker emulation.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {resolve} from 'node:path';
import {readFile,writeFile} from 'node:fs/promises';
import {createServer} from 'node:http';
import {randomUUID,createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {closeOwnedBrowser,observeChildStderr} from '../owned-browser-exit.mjs';
const env=process.env;
for(const k of ['INTEROP_PROJECT','LIVEKIT_URL','SIGNAL_OWNER_URL','SIGNAL_OWNER_NODE','SIGNAL_CONTROL','SIGNAL_PHASE','SIGNAL_RELEASE','SIGNAL_RESULT','SIGNAL_STDERR','SIGNAL_BROWSER_OWNER'])assert(env[k],k);
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {chromium}=require('playwright');
const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-client/package.json'),'utf8')).version,'2.23.0');
assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8')).version,'2.19.2');
const sourcePath=require.resolve('livekit-client'),serverSdkPath=require.resolve('livekit-server-sdk');
const source=await readFile(sourcePath);
const sdkImages={[sourcePath]:createHash('sha256').update(source).digest('hex'),[serverSdkPath]:createHash('sha256').update(await readFile(serverSdkPath)).digest('hex')};
const roomName='signal-recovery-'+randomUUID(),identity='signal-original-sdk';
const service=new RoomServiceClient(env.SIGNAL_OWNER_URL,'devkey','secret');
const result={status:'RUNNING',scope:'single original browser SDK WebSocket; real Redis first Join stream failure and original server internal start retries',room:roomName,identity,expected_connected:env.SIGNAL_EXPECT_CONNECTED==='1',sdk_images:sdkImages};
const html='<!doctype html><script src="/sdk.js"></script><script>window.fixture={async connect(url,token){window.room=new LivekitClient.Room({singlePeerConnection:true});await room.connect(url,token,{maxRetries:0,websocketTimeout:20000});return {sid:room.sid,identity:room.localParticipant.identity,participantSid:room.localParticipant.sid,state:room.state};},async close(){await window.room?.disconnect();}};</script>';
let host,browser,browserServer,finishStderr,page;
const wait=async(check,label,ms=25000)=>{const end=Date.now()+ms;while(Date.now()<end){try{const value=await check();if(value)return value;}catch{}await new Promise(r=>setTimeout(r,30));}throw Error(label+' deadline');};
try{
 const created=await service.createRoom({name:roomName,nodeId:env.SIGNAL_OWNER_NODE});
 assert.match(created.sid,/^RM_/);result.room_sid=created.sid;
 await writeFile(env.SIGNAL_CONTROL,JSON.stringify({room:roomName,first_join_stream_fault:env.SIGNAL_FIRST_JOIN_FAULT!=='0'})+'\n');
 host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'application/javascript':'text/html'});res.end(req.url==='/sdk.js'?source:html);});
 await new Promise(r=>host.listen(0,'127.0.0.1',r));
 browserServer=await chromium.launchServer({headless:true,...(env.SIGNAL_BROWSER_EXECUTABLE?{executablePath:env.SIGNAL_BROWSER_EXECUTABLE}:{channel:'chrome'}),args:['--no-sandbox','--disable-dev-shm-usage','--autoplay-policy=no-user-gesture-required']});
 const child=browserServer.process();assert(child&&child.pid);
 finishStderr=observeChildStderr(child);
 const stat=await readFile('/proc/'+child.pid+'/stat','utf8');
 const start=stat.slice(stat.lastIndexOf(')')+1).trim().split(/\s+/)[19];
 // /proc/exe is a symlink, the owned runner verifies its actual target/inode.
 await writeFile(env.SIGNAL_BROWSER_OWNER,JSON.stringify({pid:child.pid,start,birth:execFileSync('/bin/ps',['-p',String(child.pid),'-o','lstart='],{encoding:'utf8'}).trim()})+'\n');
 browser=await chromium.connect(browserServer.wsEndpoint());
 const context=await browser.newContext();page=await context.newPage();
 const sockets=[];page.on('websocket',ws=>sockets.push({url_kind:ws.url().includes('/rtc')?'rtc':'other',created_at:Date.now()}));
 await page.goto('http://127.0.0.1:'+host.address().port);
 const token=new AccessToken('devkey','secret',{identity});
 token.addGrant({roomJoin:true,room:roomName,canSubscribe:true,canPublish:true});
 const startAt=Date.now();let connected,connectError;
 try{connected=await page.evaluate(([url,jwt])=>window.fixture.connect(url,jwt),[env.LIVEKIT_URL.replace(/^http/,'ws'),await token.toJwt()]);}
 catch(error){connectError=String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'[JWT]');}
 result.elapsed_ms=Date.now()-startAt;result.connected=connected??null;result.connect_error=connectError??null;result.websockets=sockets;
 assert.equal(sockets.filter(s=>s.url_kind==='rtc').length,1,'public SDK retries are disabled; must be one HTTP WebSocket attempt');
 if(result.expected_connected){
  assert(connected,'server must recover the first real failed response stream');
  assert.equal(connected.identity,identity);assert.match(connected.participantSid,/^PA_/);
  const actual=await service.getParticipant(roomName,identity);assert.equal(actual.sid,connected.participantSid);
  await writeFile(env.SIGNAL_PHASE,JSON.stringify({phase:'connected',room:roomName,room_sid:created.sid,participant_sid:actual.sid,identity})+'\n');
  await wait(async()=>JSON.parse(await readFile(env.SIGNAL_RELEASE,'utf8')).release===true,'coordinator live owner and wire retry proof');
 }else{
  assert(!connected&&connectError,'one server attempt with first response suppressed must fail');
  await writeFile(env.SIGNAL_PHASE,JSON.stringify({phase:'failed-as-expected',room:roomName,room_sid:created.sid,identity})+'\n');
  await wait(async()=>JSON.parse(await readFile(env.SIGNAL_RELEASE,'utf8')).release===true,'coordinator single-attempt proof');
 }
 result.status='PASS';
}catch(error){result.status='FAIL';result.error=String(error.stack||error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'[JWT]');}
finally{
 const errors=[];
 if(page)try{await page.evaluate(()=>window.fixture.close());}catch(error){errors.push('SDK disconnect: '+error.message);}
 if(browserServer){
  const close=await closeOwnedBrowser(browser,browserServer);result.browser=close;
  if(close.failures.length)errors.push(...close.failures);
  const stderr=finishStderr();await writeFile(env.SIGNAL_STDERR,stderr.bytes);result.browser_stderr=stderr.metadata;
 }
 if(host)await new Promise(resolve=>host.close(resolve));
 try{await service.deleteRoom(roomName);}catch(error){if(!/does not exist|not_found/i.test(error.message))errors.push('room cleanup: '+error.message);}
 for(const [path,digest]of Object.entries(sdkImages))if(createHash('sha256').update(await readFile(path)).digest('hex')!==digest)errors.push('actual SDK source drift');
 result.cleanup_errors=errors;if(errors.length)result.status='FAIL';
 await writeFile(env.SIGNAL_RESULT,JSON.stringify(result,null,2)+'\n');
}
if(result.status!=='PASS'){console.error(JSON.stringify(result));process.exitCode=1;}
