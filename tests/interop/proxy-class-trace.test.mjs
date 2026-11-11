import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { classifyTurnPacket, inspectTurnHeader, observeAfterCall, ProxyClassTrace } from './proxy-class-trace.mjs';

function channel(body, padding = 0) {
  const b = Buffer.alloc(4 + body.length + padding); b.writeUInt16BE(0x4001); b.writeUInt16BE(body.length, 2); body.copy(b, 4); return b;
}
function indication(body) {
  const b = Buffer.alloc(24 + Math.ceil(body.length / 4) * 4); b.writeUInt16BE(0x17); b.writeUInt16BE(b.length - 20, 2); b.writeUInt32BE(0x2112a442, 4); b.writeUInt16BE(0x13, 20); b.writeUInt16BE(body.length, 22); body.copy(b, 24); return b;
}
test('bounded TURN ChannelData and DataIndication classify only inner header ranges', () => {
  for (const wrap of [channel, indication]) {
    assert.equal(classifyTurnPacket(wrap(Buffer.from([0x80, 205, 0, 1]))), 'srtcp_header_range');
    assert.equal(classifyTurnPacket(wrap(Buffer.from([0x80, 111, 0, 1]))), 'srtp_header_range');
    assert.equal(classifyTurnPacket(wrap(Buffer.from([0, 0]))), 'unknown');
  }
  assert.equal(classifyTurnPacket(channel(Buffer.from([0x80, 96]), 3)), 'srtp_header_range');
  assert.deepEqual(inspectTurnHeader(channel(Buffer.from([0x8f, 205, 0, 1]))), { kind: 'srtcp_header_range', controlPt: 205, countOrFmt: 15 });
  assert.deepEqual(inspectTurnHeader(indication(Buffer.from([0x80, 200, 0, 1]))), { kind: 'srtcp_header_range', controlPt: 200, countOrFmt: 0 });
  const control = indication(Buffer.from([0x80, 205])); control.writeUInt16BE(0x103); assert.equal(classifyTurnPacket(control), 'turn_control');
});
test('malformed/truncated/oversized envelopes fail boundedly without reading attributes as content', () => {
  for (const b of [Buffer.alloc(0), Buffer.alloc(3), Buffer.alloc(65536), channel(Buffer.from([0x80, 205]), 4)]) assert.equal(classifyTurnPacket(b), 'malformed');
  const truncated = channel(Buffer.from([0x80, 205])); truncated.writeUInt16BE(80, 2); assert.equal(classifyTurnPacket(truncated), 'malformed');
  const bad = indication(Buffer.from([0x80, 205])); bad.writeUInt16BE(99, 22); assert.equal(classifyTurnPacket(bad), 'malformed');
  const missing = indication(Buffer.from([0x80, 205])); missing.writeUInt16BE(0x6, 20); assert.equal(classifyTurnPacket(missing), 'malformed');
  const extra = Buffer.concat([indication(Buffer.from([0x80, 205])), Buffer.from([0, 6, 0, 99])]); extra.writeUInt16BE(extra.length - 20, 2); assert.equal(classifyTurnPacket(extra), 'malformed');
  assert.equal(classifyTurnPacket(Buffer.alloc(20, 1)), 'unknown');
});
test('original send receiver, args, return and synchronous callback precede observation exactly once', () => {
  const receiver = {}, packet = Buffer.from('not-recorded'), marker = {}, sequence = []; let calls = 0;
  const callback = value => { assert.equal(value, marker); sequence.push('callback'); };
  const wrapped = observeAfterCall(function (...args) { calls++; assert.equal(this, receiver); assert.equal(args[0], packet); assert.equal(args[1], callback); args[1](marker); sequence.push('original'); return marker; }, e => { assert.equal(e.threw, false); sequence.push('observe'); });
  assert.equal(wrapped.call(receiver, packet, callback), marker); assert.equal(calls, 1); assert.deepEqual(sequence, ['callback', 'original', 'observe']);
});
test('original thrown error identity and diagnostic failures never change original semantics', () => {
  const error = new Error('original'); let calls = 0, failures = 0;
  const wrapped = observeAfterCall(() => { calls++; throw error; }, e => { assert.equal(e.threw, true); throw new Error('diagnostic'); }, () => { failures++; });
  assert.throws(wrapped, e => e === error); assert.equal(calls, 1); assert.equal(failures, 1);
  const marker = {};
  assert.equal(observeAfterCall(() => marker, () => { throw error; }, () => { throw error; })(), marker);
});
function fixture() {
  let now = 0;
  const front = new EventEmitter(), backend = new EventEmitter(), client = { backend, queue: [], queuedBytes: 0 };
  const proxy = { front, clients: new Map(), counters: { droppedPackets: 0 }, sendCalls: 0,
    flush() { while (client.queue.length) { const item = client.queue.shift(); client.queuedBytes -= item.packet.length; front.send(item.packet, () => {}); } } };
  front.send = function (_packet, cb) { assert.equal(this, front); proxy.sendCalls++; cb(); return 'sent'; };
  front.on('message', () => proxy.clients.set('private-address-not-exported', client));
  backend.on('message', packet => { client.queue.push({ packet, at: now }); client.queuedBytes += packet.length; });
  const trace = new ProxyClassTrace(proxy, { clock: () => now, maxEvents: 2 });
  return { front, backend, client, proxy, trace, setNow: n => { now = n; } };
}
test('observer records RTCP behind actual queued RTP without payload or identity and bounds events', () => {
  const f = fixture(); f.trace.beginRun(0); f.front.emit('message');
  const media = channel(Buffer.from([0x80, 111, ...Buffer.from('CREDENTIAL_PAYLOAD')]));
  const rtcp = indication(Buffer.from([0x80, 205, 0, 0]));
  f.backend.emit('message', media); f.setNow(1); f.backend.emit('message', rtcp); f.setNow(25); f.proxy.flush();
  assert.equal(f.proxy.sendCalls, 2);
  const d = f.trace.endRun(); assert.equal(d.events[0].event, 'rtcp_received'); assert.equal(d.events[0].aheadPackets, 1); assert.equal(d.events[0].aheadBytes, media.length); assert.equal(d.events[0].headKind, 'srtp_header_range');
  assert.equal(d.classes.srtcp_header_range.maxObservedQueueAgeMs, 24); assert.equal(d.classes.srtp_header_range.maxObservedQueueAgeMs, 25);
  assert.ok(!JSON.stringify(d).includes('CREDENTIAL_PAYLOAD')); assert.ok(!JSON.stringify(d).includes('private-address'));
  f.trace.beginRun(1); f.backend.emit('message', rtcp); f.proxy.flush(); f.setNow(60); f.backend.emit('message', rtcp); f.proxy.flush();
  const next = f.trace.endRun(); assert.equal(next.events.length, 2); assert.ok(next.eventOverflow > 0); assert.equal(next.classes.srtcp_header_range.sendCalls, 2); assert.equal(next.eventHistoryComplete, false); assert.equal(next.events.at(-1).event, 'flush_return_gap'); assert.equal(f.backend.listenerCount('message'), 1); f.trace.close();
});
test('reset, original errors, diagnostic errors and close do not retain or replace original hooks', () => {
  assert.throws(() => new ProxyClassTrace({}, { maxEvents: 2049 }), RangeError);
  const f = fixture(); const sendWrapper = f.proxy.front.send; f.trace.beginRun(0); f.front.emit('message'); f.trace.close(); assert.notEqual(f.front.send, sendWrapper); assert.equal(f.backend.listenerCount('message'), 1); assert.equal(f.front.listenerCount('message'), 1);
  const original = new Error('send fails'); f.front.send = () => { throw original; };
  const trace = new ProxyClassTrace(f.proxy); trace.beginRun(2); assert.throws(() => f.front.send(Buffer.from('private')), e => e === original); assert.equal(trace.summary().originalSendThrows, 1);
  trace.bucket = () => { throw new Error('not-retained'); }; f.front.emit('message'); f.backend.emit('message', channel(Buffer.from([0x80, 205]))); assert.equal(trace.summary().coverage, 'INVALID_DIAGNOSTIC'); assert.ok(trace.summary().diagnosticErrors > 0); trace.close();
});

function stunEnvelope(method, messageClass, size = 20) {
  const packet = Buffer.alloc(size, 0x61);
  packet.writeUInt16BE((method & 15) | ((method & 0x70) << 1) | ((method & 0xf80) << 2)
    | ((messageClass & 1) << 4) | ((messageClass & 2) << 7), 0);
  packet.writeUInt16BE(size - 20, 2); packet.writeUInt32BE(0x2112a442, 4);
  return packet;
}

test('fixed STUN observation decodes every class/method bit without retaining transaction or attributes', async () => {
  const { inspectStunEnvelope } = await import('./proxy-class-trace.mjs');
  for (const method of [1, 3, 4, 7, 8, 9, 0xabc, 0xfff]) for (const messageClass of [0, 1, 2, 3]) {
    assert.deepEqual(inspectStunEnvelope(stunEnvelope(method, messageClass, 64)),
      { kind: 'stun', datagramBytes: 64, method, messageClass });
  }
  const invalid = stunEnvelope(1, 0); invalid.writeUInt16BE(4, 2);
  assert.equal(inspectStunEnvelope(invalid).kind, 'malformed');
  assert.equal(inspectStunEnvelope(Buffer.alloc(65536)).kind, 'malformed');
  assert.equal(inspectStunEnvelope(channel(Buffer.from([0x80, 111]))).kind, 'channel_data');
  assert.equal(inspectStunEnvelope(Buffer.from([0x80, 1, 0, 0])).kind, 'non_stun');
});

test('first front and backend STUN counters observe after original callbacks with bounded per-run ordinals', () => {
  let now = 100, originalFrontCalls = 0, originalBackendCalls = 0;
  const front = new EventEmitter(), clients = new Map(), backend = new EventEmitter();
  const peer = { address: 'SECRET_PEER_ADDRESS', port: 4000 };
  front.send = () => {};
  const proxy = { front, clients, counters: { droppedPackets: 0 }, flush() {} };
  front.on('message', () => { originalFrontCalls++; clients.set(`${peer.address}:${peer.port}`, { backend, queue: [], queuedBytes: 0 }); });
  backend.on('message', () => { originalBackendCalls++; });
  const trace = new ProxyClassTrace(proxy, { clock: () => now }); trace.beginRun(4);
  now = 105; front.emit('message', stunEnvelope(1, 0, 20), peer);
  assert.equal(originalFrontCalls, 1); now = 110;
  backend.emit('message', stunEnvelope(1, 2, 64)); backend.emit('message', stunEnvelope(1, 2, 64));
  assert.equal(originalBackendCalls, 2);
  const result = trace.endRun(), record = result.clientObservation.records[0];
  assert.equal(result.clientObservation.coverage, 'OBSERVED_AFTER_ORIGINAL_LISTENERS');
  assert.equal(record.ordinal, 0); assert.equal(record.firstObservedSinceRunStartMs, 5);
  assert.deepEqual(record.firstFrontDatagram, { kind: 'stun', datagramBytes: 20, method: 1, messageClass: 0 });
  assert.deepEqual(record.backendResponses, [{ kind: 'stun', datagramBytes: 64, method: 1, messageClass: 2, count: 2 }]);
  assert.equal(record.backendDatagramsObserved, 2); assert.equal(record.existingAtRunStart, false);
  assert.ok(!JSON.stringify(result).includes('SECRET_PEER_ADDRESS'));
  assert.ok(!JSON.stringify(result).includes('aaaaaaaa')); // Filled transaction/attributes never copied.
  trace.beginRun(5); const next = trace.endRun();
  assert.equal(next.clientObservation.records[0].ordinal, 0);
  assert.equal(next.clientObservation.records[0].backendDatagramsObserved, 0);
  assert.equal(next.clientObservation.coverage, 'INCOMPLETE_FIRST_FRONT_OR_CREATION');
  assert.equal(backend.listenerCount('message'), 1); trace.close();
});

test('client and STUN-kind overflow are explicit incomplete coverage without changing original callbacks', () => {
  const front = new EventEmitter(), clients = new Map(); let calls = 0;
  front.send = () => {};
  const proxy = { front, clients, counters: { droppedPackets: 0 }, flush() {} };
  front.on('message', (_packet, peer) => { calls++; if (!clients.has(`${peer.address}:${peer.port}`)) {
    const backend = new EventEmitter(); backend.on('message', () => {});
    clients.set(`${peer.address}:${peer.port}`, { backend, queue: [], queuedBytes: 0 });
  } });
  const trace = new ProxyClassTrace(proxy, { maxClients: 1, maxResponseKinds: 1 }); trace.beginRun(0);
  const peer = { address: 'PRIVATE', port: 1 }; front.emit('message', stunEnvelope(3, 0), peer);
  const backend = [...clients.values()][0].backend;
  backend.emit('message', stunEnvelope(3, 2)); backend.emit('message', stunEnvelope(3, 3));
  assert.equal(trace.summary().clientObservation.coverage, 'INCOMPLETE_RESPONSE_KINDS');
  const record = trace.summary().clientObservation.records[0];
  assert.equal(record.backendResponses.length, 1); assert.equal(record.responseKindsOverflow, 1);
  front.emit('message', stunEnvelope(1, 0), { address: 'PRIVATE_TWO', port: 2 });
  const result = trace.endRun(); assert.equal(calls, 2); assert.equal(result.clientObservation.records.length, 1);
  assert.equal(result.clientObservation.coverage, 'INCOMPLETE_CLIENT_IDENTITY');
  assert.equal(result.clientObservation.clientAttachmentOverflowAttempts, 1);
  assert.equal(result.clientObservation.frontDatagramsWithoutIdentity, 1);
  assert.equal([...clients.values()][1].backend.listenerCount('message'), 1);
  trace.close();
});
