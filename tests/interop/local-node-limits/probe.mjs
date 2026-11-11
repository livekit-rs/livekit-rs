import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname, join, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';
import { chromium } from 'playwright';
import { AccessToken } from 'livekit-server-sdk';
const here=dirname(fileURLToPath(import.meta.url));const require=createRequire(import.meta.url);
const url=process.env.LOCAL_QUOTA_URL;const output=process.env.LOCAL_QUOTA_OUTPUT;
assert.ok(url && output && isAbsolute(output));await mkdir(output,{recursive:false});
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const api=new URL(url);api.protocol=api.protocol==='wss:'?'https:':'http:';api.pathname='/';
const label=process.env.LOCAL_QUOTA_CASE||'capacity';const room=`quota-media-${Date.now()}-${label}`;
const probeRoom=`quota-probe-${Date.now()}-${label}`;const joinRoom=`quota-blocked-join-${Date.now()}-${label}`;
const report={status:'RUNNING',scope:'single node/no Redis; unchanged livekit-client2.23.0 publishes real tone, actual sampled admission rejects and recovers',label,room,api:[],rtcHandshakes:[],senderStats:[]};
async function token(room,identity,admin=false) {const at=new AccessToken(key,secret,{identity,ttl:'2m'});at.addGrant(admin?{roomCreate:true,roomAdmin:true,room}:{roomJoin:true,room,canPublish:true,canSubscribe:true});return at.toJwt();}
async function call(method,room) {
 const response=await fetch(new URL(`/twirp/livekit.RoomService/${method}`,api),{method:'POST',headers:{authorization:`Bearer ${await token(room,'quota-api',true)}`,'content-type':'application/json'},body:JSON.stringify(method==='CreateRoom'?{name:room}:{room})});
 const body=await response.json();const observation={method,room,status:response.status,code:body.code,msg:body.msg,sid:body.sid};report.api.push(observation);return observation;
}
async function actualUpgradeStatus(accessToken) {
 const target=new URL('/rtc',api);target.searchParams.set('access_token',accessToken);target.searchParams.set('protocol','17');target.searchParams.set('sdk','js');target.searchParams.set('version','2.23.0');
 return new Promise((resolve,reject)=>{
  const req=httpRequest(target,{headers:{connection:'Upgrade',upgrade:'websocket','sec-websocket-version':'13','sec-websocket-key':'dGhlIHNhbXBsZSBub25jZQ=='}},response=>{
   let body='';response.setEncoding('utf8');response.on('data',part=>{body+=part;});response.on('end',()=>resolve({status:response.statusCode,body}));
  });
  req.on('upgrade',(response,socket)=>{socket.destroy();resolve({status:response.statusCode,body:''});});req.on('error',reject);req.setTimeout(10000,()=>req.destroy(new Error('actual RTC Upgrade observation deadline')));req.end();
 });
}
async function poll(fn,description,timeout=35000) {const until=Date.now()+timeout;let last;while(Date.now()<until){last=await fn();if(last)return last;await new Promise(r=>setTimeout(r,250));}throw new Error(`deadline: ${description}`);}
const sdk=await readFile(require.resolve('livekit-client'));report.sdkSha256=createHash('sha256').update(sdk).digest('hex');const html=await readFile(join(here,'client.html'));
const host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
let browser,browserServer,context,publisher,challenger;
let cancelled=false;
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{cancelled=true;report.status='FAIL';report.error=`owned runner cancelled by ${signal}`;if(browserServer)void browserServer.close().catch(()=>{});});
try {
 browserServer=await chromium.launchServer({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--autoplay-policy=no-user-gesture-required']});report.browserPid=browserServer.process().pid;await writeFile(join(output,'ownership.json'),JSON.stringify({browserPid:report.browserPid,nodePid:process.pid})+'\n');browser=await chromium.connect(browserServer.wsEndpoint());report.browser=browser.version();context=await browser.newContext();
 publisher=await context.newPage();await publisher.goto(`http://127.0.0.1:${host.address().port}`);
 await publisher.evaluate(args=>window.quota.connect(args),{url,token:await token(room,'quota-publisher'),publish:true});
 await poll(async()=>{const stats=await publisher.evaluate(()=>window.quota.stats());report.senderStats=stats;return stats.some(s=>s.bytesSent>4096 && s.packetsSent>50);},'real official SDK RTP sender evidence');
 // Sampling, not an arbitrary sleep: a genuine public request must actually
 // move from allowed to the fixed capacity error while the publisher stays live.
 const full=await poll(async()=>{const result=await call('CreateRoom',probeRoom);if(result.status===200){await call('DeleteRoom',probeRoom);return false;}assert.equal(result.status,500);assert.equal(result.msg,'reached configured limit for node');return result;},'real sampled node limit reached');report.full=full;
 const existing=await call('CreateRoom',room);assert.equal(existing.status,500);assert.equal(existing.msg,'reached configured limit for node');
 challenger=await context.newPage();await challenger.goto(`http://127.0.0.1:${host.address().port}`);
 const cdp=await context.newCDPSession(challenger);await cdp.send('Network.enable');const rtc=new Set();
 cdp.on('Network.webSocketCreated',event=>{if(new URL(event.url).pathname.startsWith('/rtc'))rtc.add(event.requestId);});
 cdp.on('Network.webSocketHandshakeResponseReceived',event=>{if(rtc.has(event.requestId))report.rtcHandshakes.push({status:event.response.status});});
 const attempt=await challenger.evaluate(async args=>{try{await window.quota.connect(args);return {accepted:true};}catch(error){return {accepted:false,error:String(error.message)};}},{url,token:await token(joinRoom,'quota-rejected'),publish:false});report.rejectedJoin=attempt;assert.equal(attempt.accepted,false,'unchanged official SDK must fail a new RTC admission while at capacity');
 // Chrome does not emit HandshakeResponseReceived for every failed Upgrade.
 // Keep its passive diagnostics, and independently observe the real HTTP wire
 // response with the same authenticated room/identity after the actual SDK
 // refusal. The required HTTP503 and exact quota body assertion are not relaxed.
 report.rawUpgrade=await actualUpgradeStatus(await token(joinRoom,'quota-rejected'));
 assert.equal(report.rawUpgrade.status,503,'actual RTC opening handshake must carry fixed HTTP503');
 assert.equal(report.rawUpgrade.body.trim(),'node has exceeded its configured limit','raw Upgrade must fail for actual node capacity, not auth/SDP or a client exception');
 await challenger.evaluate(()=>window.quota.disconnect());await challenger.close();challenger=undefined;
 await publisher.evaluate(()=>window.quota.unpublish());report.unpublished=true;
 // Media stop is real; leave is also explicit so no RTP/RTCP/signalling writer
 // can hold the old allocation alive. Wait for actual sampler-driven recovery.
 await publisher.evaluate(()=>window.quota.disconnect());report.publisherLeft=true;
 const recovered=await poll(async()=>{const result=await call('CreateRoom',probeRoom);if(result.status===200)return result;assert.equal(result.status,500);assert.equal(result.msg,'reached configured limit for node');return false;},'node limit recovers after actual unpublish and leave');assert.ok(recovered.sid);report.recoveredRoom=recovered;
 const fresh=await context.newPage();await fresh.goto(`http://127.0.0.1:${host.address().port}`);await fresh.evaluate(args=>window.quota.connect(args),{url,token:await token(joinRoom,'quota-accepted-after-release'),publish:false});await fresh.evaluate(()=>window.quota.disconnect());await fresh.close();report.rtcRecovered=true;
 for(const name of [probeRoom,joinRoom,room]){const result=await call('DeleteRoom',name);assert.ok(result.status===200||result.status===404);}
 assert.equal(cancelled,false);report.status='PASS';
} catch(error) {report.status='FAIL';report.error=String(error.stack||error);}
finally {
 if(challenger)try{await challenger.evaluate(()=>window.quota.disconnect());}catch{}
 if(publisher)try{await publisher.evaluate(()=>window.quota.disconnect());}catch{}
 if(context)try{await context.close();}catch{}if(browser)try{await browser.close();}catch{}if(browserServer)try{await browserServer.close();}catch{}await new Promise(r=>host.close(r));
 await writeFile(join(output,'result.json'),JSON.stringify(report,null,2)+'\n');
}
if(report.status!=='PASS')process.exitCode=1;
