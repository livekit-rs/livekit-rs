/** C86-A2 empirical probe: launched Rust main + original pinned SDK 2.23.0.
 *  Fixed harness for the six DATA_PACKET arms (rpc_request / rpc_ack /
 *  rpc_response / stream_header / stream_chunk / stream_trailer).
 *  Legs A-K. Leg J writes a bare rpcRequest on the real reliable channel because
 *  SDK 2.23 uses streams-v2 for client RPCs, and without it no inbound frame
 *  reaches the server's rpc_request arm. Leg K publishes an ack and a response for
 *  a live server-side request id from a non-owning participant, which is the only
 *  way the wire shows that consumption is sender-scoped rather than id-keyed.
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
// The pinned protocol lib is used both to encode the raw leg-J frame and to
// decode every tapped DataPacket, so the six DATA_PACKET arms are counted from
// real wire bytes, not assumed.
const { DataPacket, RpcRequest, RpcAck, RpcResponse, DataPacket_Kind } = require('@livekit/protocol');

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
  // Write a node-side-encoded DataPacket on the SDK's own reliable channel, so
  // a frame the SDK never emits by itself (a bare rpcRequest to a known peer)
  // still traverses the real client transport, SCTP association and server path.
  async sendRaw(label, b64){
    const ch = (window.__chans || {})[label];
    if (!ch) return { ERR: 'no-channel', labels: Object.keys(window.__chans || {}) };
    const s = atob(b64); const u8 = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) u8[i] = s.charCodeAt(i);
    ch.send(u8);
    return { bytes: u8.length, state: ch.readyState };
  },
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
    // Outbound tap: a frame the SDK publishes is distinguishable from one the
    // server relays, which is what an absent receive-side frame has to be
    // attributed to.
    const origSend = RTCDataChannel.prototype.send;
    RTCDataChannel.prototype.send = function (data) {
      // The SDK hands send a Uint8Array while the receive side gets an
      // ArrayBuffer: accept both views so an outbound frame is never skipped.
      let view = null;
      if (data instanceof ArrayBuffer) view = new Uint8Array(data);
      else if (ArrayBuffer.isView(data)) view = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      // Register the channel object so a leg can write a hand-encoded frame on
      // the same SCTP stream the SDK uses (node-side protobuf, browser send).
      window.__chans = window.__chans || {};
      if (this.label) window.__chans[this.label] = this;
      if (window.__dpTapOn && view) {
        try {
          const u8 = Uint8Array.prototype.slice.call(view);
          let s = ''; for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]);
          window.__dpOut = window.__dpOut || [];
          if (window.__dpOut.length < 400) window.__dpOut.push({ label: this.label, b64: btoa(s), bytes: u8.length });
        } catch {}
      }
      return origSend.apply(this, arguments);
    };
    const origCreate = RTCPeerConnection.prototype.createDataChannel;    RTCPeerConnection.prototype.createDataChannel = function (label, options) {
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
  await pages.forEach(pg => pg.evaluate(() => { p.reset(); window.__dpOut = []; window.__dpTapOn = true; }));

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

  // Shared raw-frame writer. The sequence must continue the SDK's own reliable
  // channel numbering, otherwise the receiving client treats the frame as a replay
  // of an old sequence and discards it before the tap can be trusted.
  const sendRawFrame = async (page, build) => {
    const last = await page.evaluate(() => (window.__dpOut || []).slice(-1)[0]?.b64 ?? null);
    let seq = 1;
    if (last) {
      const prev = DataPacket.fromBinary(new Uint8Array(Buffer.from(last, 'base64')));
      if (typeof prev.sequence === 'number') seq = prev.sequence + 1;
    }
    const frame = build(seq);
    const sent = await page.evaluate(args => p.sendRaw(...args), ['_reliable', Buffer.from(frame).toString('base64')]);
    return { seq, bytes: frame.length, sent };
  };

  // Leg J: a bare RpcRequest DataPacket written on the caller's real `_reliable`
  // channel. SDK 2.23 performs client RPCs to a known peer through streams-v2
  // headers, so without this leg no frame in the fixture reaches the server's
  // rpc_request arm as an inbound packet, and a mutant that drops that arm would
  // pass the whole campaign. The id is fresh, so the room registry has no pending
  // waiter for it and the ack/response must be relayed rather than consumed.
  {
    const rawId = 'c86a-raw-' + Date.now();
    const raw = await sendRawFrame(pages[1], seq => new DataPacket({
      kind: DataPacket_Kind.RELIABLE,
      sequence: seq,
      destinationIdentities: ['handler'],
      value: {
        case: 'rpcRequest',
        value: new RpcRequest({ id: rawId, method: 'probe.echo', payload: 'raw', responseTimeoutMs: 8000, version: 1 }),
      },
    }).toBinary());
    await sleep(2500);
    const calls = await pages[0].evaluate(id => p.log().rpcHandlerCalls.filter(x => x.requestId === id), rawId);
    report.legs.raw_rpc_request = { id: rawId, publisher: 'caller', ...raw, handler_calls: calls };
  }

  // Leg K + Leg L share one live server-initiated request id, because minting a
  // second admin PerformRpc in the same run was measured to dispatch twice and lose
  // the first attempt's ack (control receipt /tmp/c86-lanes/L-control: ids
  // c4f7207b/4cd3bc42, admin elapsed 4516 ms), which would confound both claims.
  //
  // Leg K (sender scope): a frame carrying the live id from a participant that does
  // not own the pending entry must be relayed, not consumed, and must not resolve the
  // Twirp caller. A registry keyed on request id alone would answer the caller with
  // 'spoof' and the caller-side taps would never see the frames.
  //
  // Leg L (consumption, the half Leg K cannot show): the SDK - like Go
  // participant.go:4719, which sends the callee ack to ParticipantID(requestId) -
  // addresses a callee's ack at the request id, which is not a participant, so a copy
  // the server failed to consume dies in destination routing and looks exactly like a
  // consumed one. That is why mutant A6 passed the original 28-gate campaign. Here the
  // owner publishes its ack for the same live id while the destination stays a real
  // participant: pristine consumes it, so the caller sees no copy; a server that stops
  // consuming relays it, and the caller's tap shows it.
  {
    const t0 = Date.now();
    const pending = twirp('PerformRpc', { room: roomName, destinationIdentity: 'handler', method: 'probe.slowack', payload: 'spoof-target', responseTimeoutMs: 8000 });
    let spoofId = null;
    for (let i = 0; i < 20 && !spoofId; i++) {
      const list = await pages[0].evaluate(() => window.__dpLog.slice(-10).map(it => it.b64));
      for (const b64 of list) {
        const dp = DataPacket.fromBinary(new Uint8Array(Buffer.from(b64, 'base64')));
        if (dp.value?.case === 'rpcRequest' && dp.value.value.payload === 'spoof-target') { spoofId = dp.value.value.id; break; }
      }
      if (!spoofId) await sleep(150);
    }
    // Let the SDK's own ack for this id leave the page first, so the injected frame
    // continues the channel sequence instead of colliding with it: a colliding
    // sequence would let a receiving client drop the relayed copy and turn the gate
    // into a vacuous pass.
    await sleep(250);
    const preOut = await pages[0].evaluate(() => (window.__dpOut || []).slice(-6).map(it => it.b64));
    const sdkAcks = preOut
      .map(b64 => DataPacket.fromBinary(new Uint8Array(Buffer.from(b64, 'base64'))))
      .filter(dp => dp.value?.case === 'rpcAck' && dp.value.value.requestId === spoofId);
    const ownerAck = spoofId ? await sendRawFrame(pages[0], seq => new DataPacket({
      kind: DataPacket_Kind.RELIABLE, sequence: seq, destinationIdentities: ['caller'],
      value: { case: 'rpcAck', value: new RpcAck({ requestId: spoofId }) },
    }).toBinary()) : null;
    const ack = spoofId ? await sendRawFrame(pages[1], seq => new DataPacket({
      kind: DataPacket_Kind.RELIABLE, sequence: seq, destinationIdentities: ['handler'],
      value: { case: 'rpcAck', value: new RpcAck({ requestId: spoofId }) },
    }).toBinary()) : null;
    const rsp = spoofId ? await sendRawFrame(pages[1], seq => new DataPacket({
      kind: DataPacket_Kind.RELIABLE, sequence: seq, destinationIdentities: ['handler'],
      value: { case: 'rpcResponse', value: new RpcResponse({ requestId: spoofId, value: { case: 'payload', value: 'spoof' } }) },
    }).toBinary()) : null;
    const admin = await pending;
    report.legs.sender_scope_spoof = { spoof_id: spoofId, publisher: 'caller', ack, rsp, admin, elapsed_ms: Date.now() - t0 };
    report.legs.owner_ack_consumed = {
      owner_id: spoofId, publisher: 'handler', destination: 'caller',
      sdk_ack_frames: sdkAcks.length, sdk_ack_seqs: sdkAcks.map(dp => String(dp.sequence)),
      injected_seq: ownerAck?.seq ?? null, ack: ownerAck, admin, elapsed_ms: Date.now() - t0,
    };
    await sleep(800);
  }

  await sleep(500);
  const tapsRaw = {
    handler: await pages[0].evaluate(() => ({ log: window.__dpLog.slice(), out: (window.__dpOut || []).slice(), errs: (window.__dpErrs || []).slice(), capped: !!window.__dpCapped })),
    caller: await pages[1].evaluate(() => ({ log: window.__dpLog.slice(), out: (window.__dpOut || []).slice(), errs: (window.__dpErrs || []).slice(), capped: !!window.__dpCapped })),
  };
  // Decode every tapped DataPacket node-side with the pinned protocol lib.
  const decodeTap = raw => {
    const decodeList = list => {
      const entries = [];
      const undecodable = [];
      for (const it of list) {
        try {
          const dp = DataPacket.fromBinary(new Uint8Array(Buffer.from(it.b64, 'base64')));
          const v = dp.value ?? {};
          const e = { label: it.label, bytes: it.bytes, case: v.case ?? null, from: dp.participantIdentity || null, kind: dp.kind ?? null, seq: dp.sequence !== undefined && dp.sequence !== null ? dp.sequence.toString() : null };
          // Destination fields decide who a non-consumed frame may reach, so the
          // consumed-vs-relayed split is only attributable when they are recorded.
          const dstSid = dp.destinationSid ?? [];
          const dstId = dp.destinationIdentities ?? [];
          if (dstSid.length || dstId.length) e.dst = { sid: dstSid, id: dstId };
          if (v.case === 'rpcRequest') e.rpc = { id: v.value.id, method: v.value.method, payload: v.value.payload, version: String(v.value.version), timeoutMs: String(v.value.responseTimeoutMs) };
          if (v.case === 'rpcAck') e.rpc = { requestId: v.value.requestId };
          // Protocol 1.52.1 puts the response arms inline on RpcResponse.value
          // (payload string / error / compressed_payload), so the arm value sits
          // one level shallower than the error object's own fields.
          if (v.case === 'rpcResponse') e.rpc = { requestId: v.value.requestId, arm: v.value.value?.case ?? null, payload: typeof v.value.value?.value === 'string' ? v.value.value.value.slice(0, 60) : null, errCode: v.value.value?.value?.code ?? null, errMsg: v.value.value?.value?.message ?? null };
          const num = x => (x === undefined || x === null ? null : typeof x === 'bigint' ? x.toString() : x);
          if (v.case === 'streamHeader') { const h = v.value; const th = h.contentHeader?.case === 'textHeader' ? h.contentHeader.value : null; e.stream = { streamId: h.streamId, topic: h.topic, content: h.contentHeader?.case ?? null, generated: th?.generated ?? null, bodyLen: th?.body?.length ?? null, reqId: (h.attributes || {})['lk.rpc_request_id'] ?? null, totalSize: num(h.totalSize), refOp: h.ref?.operation ?? null }; }
          if (v.case === 'streamChunk') e.stream = { streamId: v.value.streamId, chunkLen: v.value.content?.value?.content?.length ?? 0, seq: num(v.value.sequenceNumber) };
          if (v.case === 'streamTrailer') e.stream = { streamId: v.value.streamId, valid: v.value.valid ?? null, reason: v.value.status?.reason ?? null, size: num(v.value.size) };
          if (v.case === 'user') e.user = { topic: v.value.topic ?? null, size: (v.value.payload?.length ?? 0), kind: v.value.kind ?? null };
          entries.push(e);
        } catch (err) {
          undecodable.push({ label: it.label, bytes: it.bytes, msg: String(err?.message ?? err).slice(0, 120) });
        }
      }
      return { entries, undecodable };
    };
    const tally = list => { const c = {}; for (const e of list) c[e.case ?? 'none'] = (c[e.case ?? 'none'] ?? 0) + 1; return c; };
    const recv = decodeList(raw.log);
    const sent = decodeList(raw.out || []);
    return {
      entries: recv.entries, counts: tally(recv.entries), undecodable: recv.undecodable,
      outbound: sent.entries, outbound_counts: tally(sent.entries), outbound_undecodable: sent.undecodable,
      errs: raw.errs, capped: raw.capped,
    };
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
  // Every gate is evaluated even when an earlier one fails, so one campaign
  // run yields the full arm-by-gate matrix a mutant sweep is attributed from.
  const chk = (name, cond) => {
    let ok = false;
    try { ok = !!cond(); } catch (error) { ok = false; legsOk[name + '_probe_error'] = String(error?.message).slice(0, 160); }
    legsOk[name] = ok;
  };
  const adminReq = H.find(e => e.case === 'rpcRequest' && e.rpc?.payload === 'admin');
  chk('api_send_data_relayed', () => report.legs.api_send_data.status === 200);
  chk('admin_rpc_delivered_to_handler_frame', () => !!adminReq && adminReq.rpc.method === 'probe.echo' && adminReq.from !== 'handler');
  chk('admin_rpc_handler_invoked', () => (report.handler_final_log.rpcHandlerCalls || []).some(x => x.payload === 'admin' && x.requestId === adminReq.rpc.id));
  const ackIds = C.filter(e => e.case === 'rpcAck').map(e => e.rpc.requestId);
  // The handler's own outbound acks mirror its invocations: every requestId on
  // the wire must equal a requestId the SDK RPC server actually handled.
  const handlerReqIds = (report.handler_final_log.rpcHandlerCalls || []).map(x => x.requestId);
  const handlerAckSends = (report.tap_handler.outbound || []).filter(e => e.case === 'rpcAck');
  // Split the ack claim in two: the callee publishing it is client behaviour,
  // only the caller receiving it is a server relay fact.
  chk('handler_published_rpc_ack_frames', () => handlerAckSends.length >= 4);
  chk('sdk_rpc_acks_observed', () => ackIds.length >= 4 && handlerReqIds.length >= 4 && ackIds.every(id => handlerReqIds.includes(id)));
  // The server-initiated leg is the consumption discriminator: its ack id is
  // published by the handler but must not appear at the caller, because the
  // room registry holds a pending waiter bound to that participant (Go
  // handleIncomingRpcAck resolves p.rpcPendingAcks[id] on the publisher).
  const adminAckSent = handlerAckSends.some(e => e.rpc.requestId === adminReq?.rpc?.id);
  const adminRspSent = (report.tap_handler.outbound || []).some(e => e.case === 'rpcResponse' && e.rpc?.requestId === adminReq?.rpc?.id && e.rpc?.arm === 'payload');
  chk('admin_rpc_ack_consumed_by_server', () => adminAckSent && !ackIds.includes(adminReq.rpc.id));
  chk('admin_rpc_response_consumed_by_server', () => adminRspSent
    && !C.filter(e => e.case === 'rpcResponse').some(e => e.rpc.requestId === adminReq.rpc.id)
    && report.legs.admin_perform_rpc.status === 200 && report.legs.admin_perform_rpc.body?.payload === 'echo:admin');
  const errRsp = C.find(e => e.case === 'rpcResponse' && e.rpc?.arm === 'error');
  chk('rpc_error_response_frame', () => !!errRsp && errRsp.rpc.errCode === 1500 && errRsp.rpc.errMsg === 'probe' && ackIds.includes(errRsp.rpc.requestId));
  chk('slowack_ordering', () => report.legs.sdk_rpc_slowack.elapsed_ms > 1400 && report.legs.sdk_rpc_slowack.ok === 'slow:ord');
  chk('noack_path_resolves', () => report.legs.sdk_rpc_noack.ok === 'noack:na');
  chk('ghost_timeout_contract', () => report.legs.sdk_rpc_ghost.err?.code === 1501 && report.legs.sdk_rpc_ghost.elapsed_ms > 6000);
  chk('stream_header_per_rpc_request', () => rpcReqHdr.length >= 4);
  chk('stream_header_per_rpc_response_side', () => rpcRspHdr.length >= 3);
  chk('topic_stream_header', () => chatHdr.length === 1 && chatHdr[0].stream.streamId);
  const sid = chatHdr[0]?.stream?.streamId;
  chk('topic_stream_chunks', () => H.filter(e => e.case === 'streamChunk' && e.stream?.streamId === sid).length === 4);
  chk('topic_stream_trailer', () => H.filter(e => e.case === 'streamTrailer' && e.stream?.streamId === sid).length === 1);
  chk('single_send_text_header', () => quickHdr.length === 1);
  // Arm-traversal controls. The SDK writes no participant_identity of its own, so
  // any identity a peer sees is the server's rewrite; and no page here is an
  // agent-kind publisher, so the guarded `on_behalf` branch must stay inert. A
  // mutant that removes that guard stamps generated and skips the rewrite, which
  // is exactly what these two gates would then catch.
  const hdrIn = [...H.filter(e => e.case === 'streamHeader'), ...C.filter(e => e.case === 'streamHeader')];
  const hdrOut = [...(report.tap_handler.outbound || []), ...(report.tap_caller.outbound || [])]
    .filter(e => e.case === 'streamHeader');
  chk('header_client_sends_empty_identity', () => hdrOut.length >= 9 && hdrOut.every(e => e.from === null));
  chk('header_server_rewrites_sender_identity', () => H.filter(e => e.case === 'streamHeader').every(e => e.from === 'caller')
    && C.filter(e => e.case === 'streamHeader').every(e => e.from === 'handler'));
  chk('header_generated_not_stamped_for_plain_sender', () => hdrIn.length >= 9 && hdrIn.every(e => e.stream?.generated !== true));
  // Leg J controls: delivery proves the inbound rpc_request arm is traversed, and
  // an unregistered id proves consumption is scoped to the server's own pending
  // waiters instead of keyed on the request id alone.
  const rawId = report.legs.raw_rpc_request?.id;
  const rawAck = C.filter(e => e.case === 'rpcAck' && e.rpc?.requestId === rawId);
  const rawRsp = C.filter(e => e.case === 'rpcResponse' && e.rpc?.requestId === rawId);
  const rawRspStream = C.filter(e => e.case === 'streamHeader' && e.stream?.topic === 'lk.rpc_response' && e.stream?.reqId === rawId);
  chk('raw_rpc_request_reaches_handler_arm', () => {
    const call = (report.legs.raw_rpc_request?.handler_calls || [])[0];
    return !!call && call.method === 'probe.echo' && call.payload === 'raw'
      && H.some(e => e.case === 'rpcRequest' && e.rpc?.id === rawId);
  });
  chk('raw_rpc_request_roundtrip_relayed', () => rawAck.length === 1
    && (rawRsp.length === 1 || rawRspStream.length === 1));
  // The published/relayed ack split is attributed per frame instead of against a
  // hard-coded count: the server-initiated legs (admin PerformRpc stamps the request
  // id as its own sender) have their callee ack taken by the room registry, and every
  // ack for a client-originated request is relayed. Stated as an iff set identity so a
  // missing or extra frame fails the gate.
  const k = report.legs.sender_scope_spoof || {};
  const l = report.legs.owner_ack_consumed || {};
  const publishedAckIds = [...new Set(handlerAckSends.map(e => e.rpc.requestId))].sort();
  const relayedAckIds = [...new Set(ackIds)].sort();
  const consumedAckIds = publishedAckIds.filter(id => !relayedAckIds.includes(id)).sort();
  const serverInitiatedIds = [...new Set(H
    .filter(e => e.case === 'rpcRequest' && e.rpc && e.from === e.rpc.id)
    .map(e => e.rpc.id))].sort();
  const slowackDispatchIds = [...new Set(H
    .filter(e => e.case === 'rpcRequest' && e.rpc?.payload === 'spoof-target')
    .map(e => e.rpc.id))].sort();
  // Recorded, not gated: one admin PerformRpc whose destination replies slowly was
  // measured to execute twice (control /tmp/c86-lanes/L-control/receipt2.json: ids
  // 1d0962c5 + bab02ac6, 4514 ms for a 1500 ms handler). Two live queue-group members
  // subscribe to Participant/PerformRpc for one participant topic
  // (room_control/accepted.rs register_perform_rpc and local_participant.rs spec()),
  // and configured() re-publishes an attempt whose response cannot be correlated
  // (room_control/mod.rs:449-473 with workers/retry.rs 3s+2s steps).
  report.findings = {
    admin_psrpc_dispatch: {
      server_initiated_ids: serverInitiatedIds,
      slowack_dispatch_ids: slowackDispatchIds,
      duplicated: slowackDispatchIds.length > 1,
      admin_leg_elapsed_ms: report.legs.admin_perform_rpc?.elapsed_ms,
      spoof_leg_elapsed_ms: k.elapsed_ms,
      handler_invocations: (report.handler_final_log.rpcHandlerCalls || [])
        .filter(x => x.method === 'probe.slowack' && x.payload === 'spoof-target').length,
      retry_policy: { max_attempts: 3, timeout_s: 3, backoff_s: 2 },
    },
  };
  chk('rpc_ack_publish_relay_split_attributed', () => consumedAckIds.length >= 2
    && consumedAckIds.join() === publishedAckIds.filter(id => serverInitiatedIds.includes(id)).sort().join()
    && relayedAckIds.every(id => !serverInitiatedIds.includes(id) && publishedAckIds.includes(id))
    && relayedAckIds.length === publishedAckIds.length - consumedAckIds.length
    && publishedAckIds.every(id => handlerReqIds.includes(id)));
  // Leg K controls: the same live request id must be consumed when its owner
  // publishes and relayed when a different participant does. Absent frames are a
  // failure, never a pass.
  const spoofAckAtHandler = H.filter(e => e.case === 'rpcAck' && e.rpc?.requestId === k.spoof_id);
  const spoofRspAtHandler = H.filter(e => e.case === 'rpcResponse' && e.rpc?.requestId === k.spoof_id);
  chk('spoof_ack_from_non_owner_forwarded', () => !!k.spoof_id && spoofAckAtHandler.length === 1);
  chk('spoof_response_from_non_owner_forwarded', () => !!k.spoof_id && spoofRspAtHandler.length === 1
    && spoofRspAtHandler[0].rpc.arm === 'payload' && spoofRspAtHandler[0].rpc.payload === 'spoof');
  chk('spoof_frames_not_duplicated_to_caller', () => !!k.spoof_id && !C.some(e => (e.case === 'rpcAck' || e.case === 'rpcResponse') && e.rpc?.requestId === k.spoof_id));
  chk('spoof_does_not_resolve_admin_waiter', () => !!k.spoof_id && k.admin?.status === 200
    && k.admin?.body?.payload === 'slow:spoof-target' && k.elapsed_ms >= 1400);
  // Leg L control: the owner's ack for a live pending request must be taken by the
  // server, not forwarded. Every positive control is inside the gate — the injected
  // frame was really written on an open channel, the SDK's own ack for the same id
  // preceded it on the wire, a copy of that same id addressed to a participant IS
  // observable at that participant (the Leg K relay), the caller is receiving other
  // acks in this run, and the admin request still completed on its response phase. An
  // absent send or an unrecorded tap fails instead of passing.
  const ownerAckAtCaller = C.filter(e => e.case === 'rpcAck' && e.rpc?.requestId === l.owner_id);
  const ownerAckPublished = handlerAckSends.filter(e => e.rpc?.requestId === l.owner_id);
  chk('owner_ack_of_live_request_consumed', () => !!l.owner_id && l.owner_id === k.spoof_id
    && l.ack?.sent?.state === 'open' && Number(l.ack?.sent?.bytes) > 0
    && l.sdk_ack_frames === 1 && ownerAckPublished.length === 2
    && Number(l.injected_seq) > Number(l.sdk_ack_seqs?.[0])
    && spoofAckAtHandler.length === 1 && ackIds.length >= 4 && ownerAckAtCaller.length === 0
    && l.admin?.status === 200 && l.admin?.body?.payload === 'slow:spoof-target' && l.elapsed_ms >= 1400);
  chk('tap_decode_clean', () => report.tap_handler.undecodable.length === 0 && report.tap_caller.undecodable.length === 0 && report.tap_handler.errs.length === 0 && report.tap_caller.errs.length === 0);
  report.leg_gates = legsOk;
  const failedGates = Object.keys(legsOk).filter(k => legsOk[k] === false);
  report.failed_gates = failedGates;
  assert.equal(failedGates.length, 0, 'leg gates failed: ' + failedGates.join(','));
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
