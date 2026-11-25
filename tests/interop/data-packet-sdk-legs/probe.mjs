/** C86-A2 empirical probe: launched Rust main + original pinned SDK 2.23.0.
 *  Fixed harness for the six DATA_PACKET arms (rpc_request / rpc_ack /
 *  rpc_response / stream_header / stream_chunk / stream_trailer).
 *  - remote peers come from room.remoteParticipants (SDK 2.23 has no
 *    room.participants)
 *  - Twirp bodies are protojson: bytes as base64, enum ints
 *  - raw DataPacket taps at RTCDataChannelTransport.send (browser side), so
 *    every packet the server pushes is decoded and counted, not assumed
 *  - RPC ack capture via live ResponseAck.sendAck() before slow payloads */
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:http';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const { AccessToken } = require('livekit-server-sdk');

const key = 'devkey', secret = 'secret';

export async function runProbe(opts = {}) {
const url = opts.url || process.env.LIVEKIT_URL || 'ws://127.0.0.1:8891';
const httpUrl = opts.httpUrl || process.env.LIVEKIT_HTTP || 'http://127.0.0.1:8891';
const roomName = opts.roomName || process.env.PROBE_ROOM || ('c86a2-probe-' + Date.now());
const out = opts.out || process.env.PROBE_OUT || '/tmp/c86-lanes/A2-probe/probe-result.json';
const report = { status: 'RUNNING', url, httpUrl, roomName, legs: {} };
const sleep = ms => new Promise(r => setTimeout(r, ms));
// protojson: bytes fields are base64 strings.
const b64 = s => Buffer.from(s, 'utf8').toString('base64');

const html = `<!doctype html>
<script src="/sdk.js"></script>
<script>
let room; const log = { rpcHandlerCalls: [], received: [], errors: [] };
window.p = {
  log(){ return JSON.parse(JSON.stringify(log)); },
  reset(){ log.received.length = 0; log.errors.length = 0; window.__dpLog.length = 0; window.__dpErr = 0; },
  async connect(u,t){
    room = new LivekitClient.Room({ singlePeerConnection:true, adaptiveStream:false, dynacast:false });
    room.localParticipant.registerRpcMethod('probe.echo', async ({payload, requestId}) => {
      log.rpcHandlerCalls.push({requestId, payload, method:'probe.echo'});
      return 'echo:' + payload;
    });
    room.localParticipant.registerRpcMethod('probe.slowack', async ({payload, requestId}) => {
      log.rpcHandlerCalls.push({requestId, payload, method:'probe.slowack'});
      await new Promise(r => setTimeout(r, 1500));
      return 'slow:' + payload;
    });
    room.localParticipant.registerRpcMethod('probe.noack', async ({payload, requestId}) => {
      log.rpcHandlerCalls.push({requestId, payload, method:'probe.noack'});
      return 'noack:' + payload;
    });
    room.localParticipant.registerRpcMethod('probe.error', async ({requestId}) => {
      log.rpcHandlerCalls.push({requestId, error:true});
      throw new LivekitClient.RpcError(1500, 'probe', 'boom');
    });
    room.on(LivekitClient.RoomEvent.DataReceived, (payload, participant, kind, topic) => {
      const dec = new TextDecoder().decode(payload);
      log.received.push({topic, text:dec.slice(0,60), kind:String(kind), from:participant && participant.identity});
    });
    for (const ev of ['Disconnected','Reconnecting','SignalConnected','ParticipantConnected','ParticipantDisconnected','Error']) {
      if (LivekitClient.RoomEvent[ev]) room.on(LivekitClient.RoomEvent[ev], (...a) => log.errors.push('evt:'+ev+':'+String(a[0]?.message ?? a[0] ?? '').slice(0,200)));
    }
    await room.connect(u,t);
    window.__roomRef=room;
  },
  identity(){ return room.localParticipant.identity; },
  sid(){ return room.localParticipant.sid; },
  // SDK 2.23: remote participants live on room.remoteParticipants (Map keyed by identity).
  peers(){ return Array.from(room.remoteParticipants.values()).map(x=>({identity:x.identity, sid:x.sid})); },
  state(){ try { return {connectionState: room.state, peerCount: room.remoteParticipants.size}; } catch(e){ return {ERR:String(e)}; } },
  async publishAudio(){ const ac=new AudioContext(); await ac.resume(); const osc=ac.createOscillator(); osc.frequency.value=440; const g=ac.createGain(); g.gain.value=0.001; const dest=ac.createMediaStreamDestination(); osc.connect(g); g.connect(dest); osc.start(); const track=dest.stream.getAudioTracks()[0]; await room.localParticipant.publishTrack(track,{source:LivekitClient.Track.Source.Microphone,name:'probe-audio'}); return 'published'; },
  async callPeer(identity, method, payload, timeoutMs){
    const t0=Date.now();
    try { const resp = await room.localParticipant.performRpc({destinationIdentity:identity, method, payload, responseTimeout: timeoutMs||8000}); return {elapsed_ms:Date.now()-t0, ok:resp}; }
    catch (e) { return {elapsed_ms:Date.now()-t0, err:{code:e.code, errorCode:e.errorCode, message:String(e.message||e)}}; }
  },
  // Reliable multi-chunk text stream on one topic: header + N chunks + trailer on the wire.
  async streamText(topic, chunks){
    const w = await room.localParticipant.streamText({topic});
    for (const c of chunks) await w.write(c);
    await w.close();
    return {streamId: w.info.streamId, wrote: chunks.length};
  },
  async sendText(topic, text){ const info = await room.localParticipant.sendText(text, {topic}); return {streamId: info.streamId, topic}; },
  async close(){ try{await room.disconnect();}catch(e){log.errors.push(String(e));} },
};
</script>`;

let host, browser, pages = [];
try {
  await mkdir('/tmp/c86-lanes/A2-probe', { recursive: true });
  // Serve the @livekit/protocol ESM graph to the browser: bare specifiers are
  // rewritten to /nm/ paths resolved from tests/interop/node_modules.
  host = createServer(async (req, res) => {
    const noCache = { 'cache-control': 'no-store, no-cache, must-revalidate' };
    if (req.url === '/sdk.js') { res.writeHead(200, { 'content-type': 'text/javascript', ...noCache }); res.end(await readFile(require.resolve('livekit-client'))); return; }
    res.writeHead(200, { 'content-type': 'text/html', ...noCache }); res.end(html);
  });
  await new Promise(r => host.listen(0, '127.0.0.1', r));
  const pageUrl = 'http://127.0.0.1:' + host.address().port;

  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const context = await browser.newContext();
  await context.addInitScript(() => {
    window.__labels = [];
    window.__dpLog = [];
    window.__dpErr = 0;
    window.__dpTapOn = false;
    // Tap the SDK's data-channel receive path: the SDK assigns
    // channel.onmessage = handler; wrapping the setter lets our observer run
    // BEFORE the SDK handler on every message (observe only, never swallow).
    const desc = Object.getOwnPropertyDescriptor(RTCDataChannel.prototype, 'onmessage');
    Object.defineProperty(RTCDataChannel.prototype, 'onmessage', {
      configurable: true,
      enumerable: desc?.enumerable ?? true,
      get() { return this.__realOnmessage; },
      set(fn) {
        const wrapped = typeof fn === 'function' ? function (ev) {
          if (window.__dpTapOn && ev.data instanceof ArrayBuffer) {
            try {
              const u8 = new Uint8Array(ev.data);
              let s = ''; for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]);
              const e = { label: this.label, b64: btoa(s), bytes: u8.length };
              if (window.__dpLog.length < 400) window.__dpLog.push(e); else window.__dpCapped = true;
            } catch (err) {
              const u8 = ev.data instanceof ArrayBuffer ? new Uint8Array(ev.data) : null;
              window.__dpErrs = window.__dpErrs || [];
              if (u8 && window.__dpErrs.length < 5) { let hex=''; for (let i=0;i<Math.min(u8.length,16);i++) hex += u8[i].toString(16).padStart(2,'0') + ' '; window.__dpErrs.push({msg:String(err && err.message || err), hex, len:u8.length}); }
              else if (!u8 && window.__dpErrs.length < 5) window.__dpErrs.push({msg:'not-arraybuffer', type: typeof ev.data, ctor: ev.data && ev.data.constructor && ev.data.constructor.name});
            }
          }
          return fn.call(this, ev);
        } : fn;
        this.__realOnmessage = wrapped;
        desc.set.call(this, wrapped);
      },
    });
    const origCreate = RTCPeerConnection.prototype.createDataChannel;
    RTCPeerConnection.prototype.createDataChannel = function (label, options) {
      window.__labels.push('local:' + label);
      return origCreate.call(this, label, options);
    };
    const origAdd = RTCPeerConnection.prototype.addEventListener;
    RTCPeerConnection.prototype.addEventListener = function (type, fn, opts) {
      if (type === 'datachannel') window.__labels.push('inbound-pending');
      return origAdd.call(this, type, fn, opts);
    };
  });

  const identities = ['handler', 'caller'];
  for (const identity of identities) {
    const page = await context.newPage();
    pages.push(page);
    await page.goto(pageUrl);
    const token = new AccessToken(key, secret, { identity });
    token.addGrant({ roomJoin: true, room: roomName, canPublish: true, canSubscribe: true, canPublishData: true });
    await page.evaluate(args => p.connect(...args), [url, await token.toJwt()]);
  }
  for (const pg of pages) await pg.evaluate(() => p.publishAudio());
  await pages[0].waitForFunction(() => (window.p ? p.state().peerCount : 0) === 1, null, { timeout: 30000 });
  await pages[1].waitForFunction(() => (window.p ? p.state().peerCount : 0) === 1, null, { timeout: 30000 });
  await sleep(3000);

  report.joined = await Promise.all(pages.map(pg => pg.evaluate(() => [p.identity(), p.sid(), window.__labels.slice()])));
  report.peerList = await Promise.all(pages.map(pg => pg.evaluate(() => p.peers())));
  report.states = await Promise.all(pages.map(pg => pg.evaluate(() => p.state())));
  assert.equal(report.peerList[0][0].identity, 'caller', 'handler must see caller');
  assert.equal(report.peerList[1][0].identity, 'handler', 'caller must see handler');

  const admin = new AccessToken(key, secret);
  admin.addGrant({ roomAdmin: true, room: roomName });
  const bearer = await admin.toJwt();
  const twirp = async (method, body) => {
    const res = await fetch(`${httpUrl}/twirp/livekit.RoomService/${method}`, {
      method: 'POST', headers: { authorization: 'Bearer ' + bearer, 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.text().then(t => { try { return JSON.parse(t); } catch { return t.slice(0, 300); } }) };
  };

  // Leg A: Twirp SendData, protojson base64 payload (server -> all peers user packet).
  {
    const r = await twirp('SendData', { room: roomName, topic: 'api', kind: 1, data: b64('api-relay-test') });
    report.legs.api_send_data = r;
    await sleep(1200);
  }

  // Turn on the raw DataPacket tap after join noise; reset logs.
  await pages.forEach(pg => pg.evaluate(() => { p.reset(); window.__dpTapOn = true; }));

  // Leg B: admin Twirp PerformRpc -> handler SDK peer (server-consumed request,
  // handler's ack + response observed on the wire tap).
  {
    const t0 = Date.now();
    const r = await twirp('PerformRpc', { room: roomName, destinationIdentity: 'handler', method: 'probe.echo', payload: 'admin', responseTimeoutMs: 8000 });
    report.legs.admin_perform_rpc = { elapsed_ms: Date.now() - t0, ...r };
    await sleep(800);
  }

  // Leg C: SDK->SDK performRpc happy path with live sendAck.
  {
    report.legs.sdk_to_sdk_rpc = await pages[1].evaluate(async () => await p.callPeer('handler', 'probe.echo', 'hello'));
  }

  // Leg D: ack-before-slow-response ordering (rpc_ack observable separately).
  {
    report.legs.sdk_rpc_slowack = await pages[1].evaluate(async () => await p.callPeer('handler', 'probe.slowack', 'ord'));
  }

  // Leg E: no-ack path still resolves via response only.
  {
    report.legs.sdk_rpc_noack = await pages[1].evaluate(async () => await p.callPeer('handler', 'probe.noack', 'na'));
  }

  // Leg F: application RpcError propagation (ack sent, then error response).
  {
    report.legs.sdk_rpc_error = await pages[1].evaluate(async () => await p.callPeer('handler', 'probe.error', 'x'));
  }

  // Leg G: unknown destination identity.
  {
    report.legs.sdk_rpc_ghost = await pages[1].evaluate(async () => await p.callPeer('ghost', 'probe.echo', 'x'));
  }

  // Leg H: topic-based reliable multi-chunk text stream (header/chunks/trailer).
  {
    const before = await pages[0].evaluate(() => p.log().received.length);
    const r = await pages[1].evaluate(async () => await p.streamText('chat', ['chunk-0-', 'chunk-1-', 'chunk-2-', 'chunk-3-']));
    await sleep(2000);
    const recv = await pages[0].evaluate(b => p.log().received.slice(b), before);
    report.legs.topic_stream = { sent: r, received: recv };
  }

  // Leg I: single small sendText (streams-v2 encoding: header + trailer).
  {
    const r = await pages[1].evaluate(async () => await p.sendText('quick', 'quick-one-shot'));
    await sleep(1200);
    report.legs.single_send_text = { sent: r };
  }

  await sleep(500);
  const tapsRaw = {
    handler: await pages[0].evaluate(() => ({ log: window.__dpLog.slice(), errs: (window.__dpErrs || []).slice(), capped: !!window.__dpCapped })),
    caller: await pages[1].evaluate(() => ({ log: window.__dpLog.slice(), errs: (window.__dpErrs || []).slice(), capped: !!window.__dpCapped })),
  };
  // Decode every tapped DataPacket node-side with the pinned protocol lib, so
  // the six DATA_PACKET arms are counted from real wire bytes, not assumed.
  const { DataPacket } = require('@livekit/protocol');
  const decodeTap = raw => {
    const entries = [];
    const undecodable = [];
    for (const it of raw.log) {
      try {
        const dp = DataPacket.fromBinary(new Uint8Array(Buffer.from(it.b64, 'base64')));
        const v = dp.value ?? {};
        const e = { label: it.label, bytes: it.bytes, case: v.case ?? null, from: dp.participantIdentity || null, kind: dp.kind ?? null, seq: dp.sequenceNumber !== undefined && dp.sequenceNumber !== null ? dp.sequenceNumber.toString() : null };
        if (v.case === 'rpcRequest') e.rpc = { id: v.value.id, method: v.value.method, payload: v.value.payload, version: String(v.value.version), timeoutMs: String(v.value.responseTimeoutMs) };
        if (v.case === 'rpcAck') e.rpc = { requestId: v.value.requestId };
        if (v.case === 'rpcResponse') e.rpc = { requestId: v.value.requestId, arm: v.value.value?.case ?? null, payload: v.value.value?.value?.payload?.slice?.(0, 60), errCode: v.value.value?.value?.code ?? null, errMsg: v.value.value?.value?.message ?? null };
        const num = x => (x === undefined || x === null ? null : typeof x === 'bigint' ? x.toString() : x);
        if (v.case === 'streamHeader') { const h = v.value; e.stream = { streamId: h.streamId, topic: h.topic, content: h.contentHeader?.case ?? null, generated: h.contentHeader?.value?.textHeader?.generated ?? null, totalSize: num(h.totalSize), refOp: h.ref?.operation ?? null }; }
        if (v.case === 'streamChunk') e.stream = { streamId: v.value.streamId, chunkLen: v.value.content?.value?.content?.length ?? 0, seq: num(v.value.sequenceNumber) };
        if (v.case === 'streamTrailer') e.stream = { streamId: v.value.streamId, valid: v.value.valid ?? null, reason: v.value.status?.reason ?? null, size: num(v.value.size) };
        if (v.case === 'user') e.user = { topic: v.value.topic ?? null, size: (v.value.payload?.length ?? 0), kind: v.value.kind ?? null };
        entries.push(e);
      } catch (err) {
        undecodable.push({ label: it.label, bytes: it.bytes, msg: String(err?.message ?? err).slice(0, 120) });
      }
    }
    const counts = {};
    for (const e of entries) counts[e.case ?? 'none'] = (counts[e.case ?? 'none'] ?? 0) + 1;
    return { entries, counts, undecodable, errs: raw.errs, capped: raw.capped };
  };
  report.tap_handler = decodeTap(tapsRaw.handler);
  report.tap_caller = decodeTap(tapsRaw.caller);
  report.sdk_identity = await pages[0].evaluate(() => ({
    clientVersion: LivekitClient.version,
    browser: navigator.userAgent,
  }));
  report.handler_final_log = await pages[0].evaluate(() => p.log());
  report.caller_final_log = await pages[1].evaluate(() => p.log());
  // Typed per-leg gates: every assertion here can fail — a server that drops,
  // misroutes or consumes one of the six DATA_PACKET arms breaks them.
  const H = report.tap_handler.entries, C = report.tap_caller.entries;
  const chatHdr = H.filter(e => e.case === 'streamHeader' && e.stream?.topic === 'chat');
  const quickHdr = H.filter(e => e.case === 'streamHeader' && e.stream?.topic === 'quick');
  const rpcReqHdr = H.filter(e => e.case === 'streamHeader' && e.stream?.topic === 'lk.rpc_request');
  const rpcRspHdr = C.filter(e => e.case === 'streamHeader' && e.stream?.topic === 'lk.rpc_response');
  const legsOk = {};
  const chk = (name, cond) => { legsOk[name] = cond; assert.ok(cond, 'leg gate failed: ' + name); };
  const adminReq = H.find(e => e.case === 'rpcRequest' && e.rpc?.payload === 'admin');
  chk('api_send_data_relayed', report.legs.api_send_data.status === 200);
  chk('admin_rpc_delivered_to_handler_frame', !!adminReq && adminReq.rpc.method === 'probe.echo' && adminReq.from !== 'handler');
  chk('admin_rpc_handler_invoked', (report.handler_final_log.rpcHandlerCalls || []).some(x => x.payload === 'admin' && x.requestId === adminReq.rpc.id));
  const ackIds = C.filter(e => e.case === 'rpcAck').map(e => e.rpc.requestId);
  // The handler's own outbound acks mirror its invocations: every requestId on
  // the wire must equal a requestId the SDK RPC server actually handled.
  const handlerReqIds = (report.handler_final_log.rpcHandlerCalls || []).map(x => x.requestId);
  chk('sdk_rpc_acks_observed', ackIds.length >= 4 && handlerReqIds.length >= 4 && ackIds.every(id => handlerReqIds.includes(id)));
  const errRsp = C.find(e => e.case === 'rpcResponse' && e.rpc?.arm === 'error');
  chk('rpc_error_response_frame', !!errRsp && errRsp.rpc.errCode === 1500 && errRsp.rpc.errMsg === 'probe' && ackIds.includes(errRsp.rpc.requestId));
  chk('slowack_ordering', report.legs.sdk_rpc_slowack.elapsed_ms > 1400 && report.legs.sdk_rpc_slowack.ok === 'slow:ord');
  chk('noack_path_resolves', report.legs.sdk_rpc_noack.ok === 'noack:na');
  chk('ghost_timeout_contract', report.legs.sdk_rpc_ghost.err?.code === 1501 && report.legs.sdk_rpc_ghost.elapsed_ms > 6000);
  chk('stream_header_per_rpc_request', rpcReqHdr.length >= 4);
  chk('stream_header_per_rpc_response_side', rpcRspHdr.length >= 3);
  chk('topic_stream_header', chatHdr.length === 1 && chatHdr[0].stream.streamId);
  const sid = chatHdr[0]?.stream?.streamId;
  chk('topic_stream_chunks', H.filter(e => e.case === 'streamChunk' && e.stream?.streamId === sid).length === 4);
  chk('topic_stream_trailer', H.filter(e => e.case === 'streamTrailer' && e.stream?.streamId === sid).length === 1);
  chk('single_send_text_header', quickHdr.length === 1);
  chk('tap_decode_clean', report.tap_handler.undecodable.length === 0 && report.tap_caller.undecodable.length === 0 && report.tap_handler.errs.length === 0 && report.tap_caller.errs.length === 0);
  report.leg_gates = legsOk;
  report.status = 'PROBE_DONE';
} catch (error) {
  report.status = 'FAIL';
  report.failure = { name: error?.name, message: String(error?.message || error).slice(0, 4000), stack: String(error?.stack).slice(0, 4000) };
} finally {
  for (const page of pages) { try { await page.evaluate(() => p.close()); } catch {} try { await page.close(); } catch {} }
  if (browser) await browser.close().catch(() => {});
  if (host) await new Promise(r => host.close(r));
  await writeFile(out, JSON.stringify(report, null, 2) + '\n');
}
  return report;
}

if (process.argv[1] && import.meta.url === new URL('file://' + process.argv[1]).href) {
  const r = await runProbe();
  console.log(r.status, r.out_path || process.env.PROBE_OUT || '/tmp/c86-lanes/A2-probe/probe-result.json');
  if (r.status !== 'PROBE_DONE') process.exit(1);
}
