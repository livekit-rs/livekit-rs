import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createServer} from 'node:http';
import {readFile,writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join} from 'node:path';
import {setTimeout as sleep} from 'node:timers/promises';
import {closeOwnedBrowser,observeChildStderr} from '../owned-browser-exit.mjs';

const require = createRequire(import.meta.url);
const {chromium} = require('playwright');
const {AccessToken, RoomServiceClient} = require('livekit-server-sdk');
const {FRONT_URL: front, OWNER_URL: owner, OWNER_NODE: ownerNode, TEST_ROOM: roomName,
  LIVEKIT_API_KEY: key, LIVEKIT_API_SECRET: secret, CASE_DIRECTORY: directory,
  OWNER_FAULT: fault, HELD_PAYLOADS: payloadsJson, SINGLE_PC: singlePc} = process.env;
const payloads = JSON.parse(payloadsJson);
const before = "bounded(state.rpc,{payload,callerIdentity});return 'remote:'+payload;";
const after = "bounded(state.rpc,{payload,callerIdentity});await new Promise(resolve=>{(window.heldRpc??=new Map()).set(payload,resolve);});return 'remote:'+payload;";
let html = await readFile(new URL('../cluster-room-api/client.html', import.meta.url), 'utf8');
assert.equal(html.split(before).length, 2, 'known browser handler must be found exactly once');
html = html.replace(before, after);
const originalRoom = 'new Room({adaptiveStream:false,dynacast:false})';
assert.equal(html.split(originalRoom).length,2);
if (singlePc !== 'default') {
  html = html.replace(originalRoom, `new Room({adaptiveStream:false,dynacast:false,singlePeerConnection:${singlePc}})`);
}
const sdkPath = require.resolve('livekit-client');
const sdk = await readFile(sdkPath);
const sdkPackage = JSON.parse(await readFile(join(dirname(sdkPath),'../package.json'),'utf8'));
const host = createServer((request, response) => {
  const script = request.url === '/sdk.js';
  response.writeHead(200, {'content-type': script ? 'application/javascript' : 'text/html'});
  response.end(script ? sdk : html);
});
const output = {fault, status:'FAIL', room:roomName, requestedSinglePc:singlePc,
  sdkVersion:sdkPackage.version,
  sdkSha256:createHash('sha256').update(sdk).digest('hex')};
let browserServer, browser, observer, publisher, finishBrowserStderr;
let bound = false;
const files = name => join(directory, name);
const save = (name, value) => writeFile(files(name), JSON.stringify(value, null, 2)+'\n', {flag:'wx'});
async function wait(check, label, timeout=25000) {
  const end = Date.now()+timeout;
  while (Date.now()<end) {
    if (await check()) return;
    await sleep(40);
  }
  throw new Error(label);
}
const snapshot = page => page.evaluate(() => roomApi.snapshot());
function healthy(state) {
  return state.tracks.some(t=>t.kind==='audio' && t.tone?.tone440>.025 && t.bytes>1000)
      && state.tracks.some(t=>t.kind==='video' && t.frames>=8 && t.bytes>1000);
}
async function token(identity) {
  const t = new AccessToken(key, secret, {identity, ttl:'5m'});
  t.addGrant({room:roomName, roomJoin:true, canPublish:true, canSubscribe:true});
  return t.toJwt();
}
try {
  const ownerApi = new RoomServiceClient(owner, key, secret);
  const room = await ownerApi.createRoom({name:roomName, nodeId:ownerNode, emptyTimeout:60});
  assert.match(room.sid, /^RM_/);
  output.roomSid = room.sid;
  await new Promise(resolve => host.listen(0,'127.0.0.1',resolve));
  bound = true;
  browserServer = await chromium.launchServer({channel:process.env.BROWSER_CHANNEL||'chrome', headless:true,
    args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
  finishBrowserStderr=observeChildStderr(browserServer.process());
  await save('browser-pid.json', {pid:browserServer.process().pid});
  browser = await chromium.connect(browserServer.wsEndpoint());
  const context = await browser.newContext();
  await context.addInitScript(() => {
    const Native = window.RTCPeerConnection;
    window.observedPCs = [];
    window.RTCPeerConnection = class extends Native {
      constructor(...args) {super(...args);window.observedPCs.push(this);}
    };
  });
  publisher = await context.newPage();
  observer = await context.newPage();
  for (const [page,identity,publish] of [[publisher,'publisher',true],[observer,'observer',false]]) {
    await page.goto(`http://127.0.0.1:${host.address().port}`);
    await page.evaluate(args=>roomApi.connect(args), {url:front.replace('http:','ws:'),token:await token(identity),publish});
  }
  await wait(async()=>healthy(await snapshot(observer)),'owner did not send actual audio and video');
  output.before = await snapshot(observer);
  if (singlePc !== 'default') {
    const expected = singlePc === 'true' ? 1 : 2;
    await wait(async()=>(await snapshot(observer)).peerConnections.filter(pc=>pc.connectionState==='connected').length===expected,
      'actual observer peer connection count disagrees with requested mode');
  }
  output.connectedPeerConnections = (await snapshot(observer)).peerConnections.filter(pc=>pc.connectionState==='connected').length;
  await save('ready.json',{roomSid:room.sid,connectedPeerConnections:output.connectedPeerConnections});
  await wait(()=>observer.evaluate(expected=>expected.every(payload=>window.heldRpc?.has(payload)),payloads),
    'SDK RPC handlers not entered');
  output.callback = (await snapshot(observer)).rpc;
  assert.deepEqual(output.callback.map(call=>call.payload).sort(),[...payloads].sort());
  await save('accepted.json',output.callback);
  await wait(async()=>readFile(files('fault.json')).then(()=>true,()=>false),'coordinator did not apply fault');
  if (fault==='drain') {
    const mid = await snapshot(observer);
    const frames = mid.tracks.find(t=>t.kind==='video')?.frames;
    await wait(async()=>{
      const state = await snapshot(observer);
      return healthy(state) && state.tracks.find(t=>t.kind==='video')?.frames > frames;
    },'decoded media did not continue while owner drains',2500);
    output.after = await snapshot(observer);
    await save('drain-media.json',{framesBefore:frames,framesAfter:output.after.tracks.find(t=>t.kind==='video').frames});
    for (const [index,payload] of [...payloads].reverse().entries()) {
      await wait(async()=>readFile(files(`release-${index}.json`)).then(()=>true,()=>false),'coordinator did not release SDK callback');
      await observer.evaluate(key=>{
        const release = window.heldRpc.get(key);
        window.heldRpc.delete(key);
        release();
      },payload);
    }
  }
  await wait(async()=>readFile(files('rpc-done.json')).then(()=>true,()=>false),'coordinator did not observe terminal RPC');
  output.callbacks = (await snapshot(observer)).rpc;
  assert.deepEqual(output.callbacks.map(call=>call.payload).sort(),[...payloads].sort(),'SDK handler re-entered after fault');
  output.status='PASS';
} catch(error) {
  output.error=String(error.message).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
} finally {
  for (const [page,id] of [[observer,'observer'],[publisher,'publisher']]) {
    if (!page) continue;
    try {
      await page.evaluate(()=>roomApi.close());
      const closed = await snapshot(page);
      output[id+'Closed']=closed.peerConnections.length>0 &&
        closed.peerConnections.every(pc=>pc.connectionState==='closed'&&pc.signalingState==='closed') &&
        closed.localTracks.every(track=>track.state==='ended');
      if (!output[id+'Closed']) output.status='FAIL';
    } catch(error) {
      output[id+'CloseError']=String(error.message);
      output.status='FAIL';
    }
  }
  output.browserCleanup=await closeOwnedBrowser(browser,browserServer);
  if (output.browserCleanup.failures.length) output.status='FAIL';
  if (finishBrowserStderr) {
    const stderr=finishBrowserStderr();
    output.browserStderr=stderr.metadata;
    await writeFile(files('browser.stderr'),stderr.bytes,{flag:'wx'});
  }
  if (bound) await new Promise(resolve=>host.close(resolve));
  await save('probe.json',output);
  process.exitCode=output.status==='PASS'?0:1;
}
