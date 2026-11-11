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
async function linuxStart(pid){if(process.platform!=='linux')return null;const value=await readFile('/proc/'+pid+'/stat','utf8');return value.slice(value.lastIndexOf(')')+1).trim().split(/\s+/)[19];}for(const key of ['INTEROP_PROJECT','LIVEKIT_URL','PUBLISHER_OUTPUT','PUBLISHER_DEPLOYMENT'])assert(env[key],key+' required');
assert.equal(env.PUBLISHER_DEPLOYMENT,'standalone');
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
for(const [pkg,version]of [['livekit-client','2.23.0'],['livekit-server-sdk','2.19.2']])assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules',pkg,'package.json'),'utf8')).version,version);
const sdk=await readFile(require.resolve('livekit-client'));
const html=`<!doctype html><script src="/sdk.js"></script><script>
let room,localTrack,context,oscillator;
window.fixture={
 async connect(url,token){room=new LivekitClient.Room({singlePeerConnection:true});await room.connect(url,token);return room.localParticipant.sid;},
 async publish(){context=new AudioContext();await context.resume();const destination=context.createMediaStreamDestination();oscillator=context.createOscillator();oscillator.frequency.value=440;oscillator.connect(destination);oscillator.start();localTrack=new LivekitClient.LocalAudioTrack(destination.stream.getAudioTracks()[0]);const pub=await room.localParticipant.publishTrack(localTrack,{source:LivekitClient.Track.Source.Microphone,dtx:false});return pub.trackSid;},
 async packets(){if(!localTrack)return 0;const stats=await localTrack.getRTCStatsReport();let n=0;stats?.forEach(s=>{if(s.type==='outbound-rtp'&&!s.isRemote)n+=s.packetsSent||0;});return n;},
 async unpublish(){await room.localParticipant.unpublishTrack(localTrack,false);localTrack.stop();localTrack=undefined;oscillator.stop();oscillator=undefined;await context.close();context=undefined;},
 async close(){if(localTrack)await this.unpublish();await room?.disconnect();room=undefined;}
};
</script>`;
const report={status:'RUNNING',scope:'actual original SDK microphone publication/unpublication plus public ParticipantInfo/RoomInfo and SetPermission publisher identity; replacement SID must start fresh',observations:[],cleanup_errors:[]};
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
await writeFile(env.PUBLISHER_OUTPUT,JSON.stringify(report)+'\n',{flag:'wx',mode:0o600});
let host,browser,browserServer,finishBrowserStderr;const pages=[];const room='publisher-identity-'+randomUUID();const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret';const api=new RoomServiceClient(env.LIVEKIT_URL.replace(/^ws/,'http'),key,secret);
async function token(identity){const t=new AccessToken(key,secret,{identity});t.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true,canPublishData:true});return t.toJwt();}
async function until(check,label){const deadline=Date.now()+15000;while(true){const value=await check();if(value)return value;if(Date.now()>=deadline)throw Error(label);await sleep(25);}}
async function snapshot(label){const p=await api.getParticipant(room,'publisher');report.observations.push({case:label,sid:p.sid,isPublisher:p.isPublisher,tracks:p.tracks.length});return p;}
try{
 host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 await api.createRoom({name:room,departureTimeout:60});browserServer=await chromium.launchServer({...(env.BROWSER_EXECUTABLE?{executablePath:env.BROWSER_EXECUTABLE}:{channel:env.BROWSER_CHANNEL||'chrome'}),headless:true,args:['--autoplay-policy=no-user-gesture-required']});finishBrowserStderr=observeChildStderr(browserServer.process());const child=browserServer.process();assert(child?.pid,'actual owned Chromium child required');report.ownership={nodePid:process.pid,nodeBirth:birth(process.pid),nodeExecutable:process.execPath,browserPid:child.pid,browserBirth:birth(child.pid),browserExecutable:process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile,browserLaunchEntry:child.spawnfile,browserExecutableSha256:hash(await readFile(process.platform==='linux'?await realpath('/proc/'+child.pid+'/exe'):child.spawnfile)),browserUid:process.getuid(),nodeLinuxStart:await linuxStart(process.pid),browserLinuxStart:await linuxStart(child.pid)};await writeFile(env.POLICY_OWNERSHIP,JSON.stringify(report.ownership)+'\n',{flag:'wx',mode:0o600});browser=await chromium.connect(browserServer.wsEndpoint());report.browser=browser.version();const sids=[];
 const page=await browser.newPage();pages.push(page);await page.goto('http://127.0.0.1:'+host.address().port);
 const firstSid=await page.evaluate(args=>fixture.connect(...args),[env.LIVEKIT_URL,await token('publisher')]);
 let p=await snapshot('fresh-connected-without-publication');assert.equal(p.sid,firstSid);assert.equal(p.isPublisher,false);assert.equal(p.tracks.length,0);
 const track=await page.evaluate(()=>fixture.publish());
 await until(async()=>{const n=await page.evaluate(()=>fixture.packets());return n>=20?n:0;},'actual native RTP sender did not reach20 packets');
 p=await until(async()=>{const p=await api.getParticipant(room,'publisher');return p.tracks.some(t=>t.sid===track)&&p.isPublisher?p:null;},'actual published track absent from public store');
 report.observations.push({case:'actual-rtp-publisher',sid:p.sid,isPublisher:p.isPublisher,track,packetsSent:await page.evaluate(()=>fixture.packets())});
 await page.evaluate(()=>fixture.unpublish());
 await until(async()=>{const p=await api.getParticipant(room,'publisher');return p.tracks.length===0?p:null;},'last actual audio track did not retire');
 p=await snapshot('last-media-unpublished-sticky');assert.equal(p.isPublisher,true,'original publisher identity remains true after last unpublish');assert.equal(p.tracks.length,0);
 const permission={canPublish:false,canSubscribe:true,canPublishData:true,canPublishSources:[],canUpdateMetadata:false,hidden:false,recorder:false,agent:false,canSubscribeMetrics:true,canManageAgentSession:false};
 p=await api.updateParticipant(room,'publisher',{permission});assert.equal(p.isPublisher,false);assert.equal(p.tracks.length,0);report.observations.push({case:'permission-revoked',isPublisher:p.isPublisher});
 await until(async()=>{const rows=await api.listRooms([room]);return rows.length===1&&rows[0].numPublishers===0;},'public room publisher count did not follow permission revocation');
 permission.canPublish=true;p=await api.updateParticipant(room,'publisher',{permission});assert.equal(p.isPublisher,true,'same publisher PC remains established with no current tracks');assert.equal(p.tracks.length,0);report.observations.push({case:'permission-restored-on-established-pc',isPublisher:p.isPublisher});
 const rooms=await until(async()=>{const rows=await api.listRooms([room]);return rows.length===1&&rows[0].numPublishers===1?rows:null;},'public room field did not follow restored original publisher identity');report.observations.push({case:'room-publisher-count-restored',numPublishers:rooms[0].numPublishers});
 await page.evaluate(()=>fixture.close());await until(async()=>!(await api.listParticipants(room)).some(p=>p.identity==='publisher'),'old participant did not retire');
 await api.deleteRoom(room);await api.createRoom({name:room,departureTimeout:60});
 const replacement=await page.evaluate(args=>fixture.connect(...args),[env.LIVEKIT_URL,await token('publisher')]);assert.notEqual(replacement,firstSid);
 p=await snapshot('new-room-new-participant-fresh');assert.equal(p.sid,replacement);assert.equal(p.isPublisher,false);assert.equal(p.tracks.length,0);
 report.sdkEntrySha256=hash(sdk);assert.equal(hash(await readFile(require.resolve('livekit-client'))),report.sdkEntrySha256,'original client SDK source changed during probe');report.status='PASS';
}catch(error){report.status='FAIL';report.failure=failureDetails(error);}
finally{for(const page of pages){try{await page.evaluate(()=>fixture.close());await page.close();}catch{report.cleanup_errors.push('owned page cleanup failed');}}{const closed=await closeOwnedBrowser(browser,browserServer);report.browserExit=closed.exit;report.browserCleanup=closed;report.cleanup_errors.push(...closed.failures);if(browserServer&&(!closed.exit||closed.exit.code!==0||closed.exit.signal!=null))report.cleanup_errors.push('actual-owned-browser-exit-not-zero');if(finishBrowserStderr){const stderr=finishBrowserStderr();report.browserStderr=stderr.metadata;try{await writeFile(env.POLICY_BROWSER_STDERR,stderr.bytes,{flag:'wx',mode:0o600});}catch{report.cleanup_errors.push('owned-browser-stderr-persist');}}}try{await api.deleteRoom(room);}catch(error){if(error?.code!=='not_found')report.cleanup_errors.push('owned room cleanup failed');}if(host)await new Promise(r=>host.close(r));if(report.cleanup_errors.length)report.status='FAIL';await writeFile(env.PUBLISHER_OUTPUT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status!=='PASS')process.exitCode=1;}
