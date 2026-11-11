// Opt-in observations only. Never retain packet bytes, addresses, channel/SSRC IDs or credentials.
import { performance } from 'node:perf_hooks';

function parseTurnHeader(packet) {
  if (!Buffer.isBuffer(packet) || packet.length < 4 || packet.length > 65535) return 'malformed';
  let offset, end;
  if ((packet[0] & 0xc0) === 0x40) {
    const length = packet.readUInt16BE(2);
    end = 4 + length;
    if (end > packet.length || packet.length - end > 3) return 'malformed';
    offset = 4;
  } else if ((packet[0] & 0xc0) === 0 && packet.length >= 20 && packet.readUInt32BE(4) === 0x2112a442) {
    const length = packet.readUInt16BE(2);
    end = 20 + length;
    if (length % 4 || end !== packet.length) return 'malformed';
    if (packet.readUInt16BE(0) !== 0x0017) return 'turn_control';
    let at = 20, attributes = 0, dataEnd;
    while (at < end && attributes++ < 128) {
      if (at + 4 > end) return 'malformed';
      const kind = packet.readUInt16BE(at), size = packet.readUInt16BE(at + 2);
      const next = at + 4 + Math.ceil(size / 4) * 4;
      if (next > end) return 'malformed';
      if (kind === 0x0013) { if (offset !== undefined) return 'malformed'; offset = at + 4; dataEnd = offset + size; }
      at = next;
    }
    if (offset === undefined || at !== end) return 'malformed';
    end = dataEnd;
  } else return 'unknown';
  if (end - offset < 2 || (packet[offset] & 0xc0) !== 0x80) return 'unknown';
  // Header-range inference, NOT authentication/decryption. RTP PT64..95 would overlap.
  return packet[offset + 1] >= 192 && packet[offset + 1] <= 223
    ? { kind: 'srtcp_header_range', controlPt: packet[offset + 1], countOrFmt: packet[offset] & 31 }
    : { kind: 'srtp_header_range' };
}
export function inspectTurnHeader(packet) {
  const parsed = parseTurnHeader(packet);
  return typeof parsed === 'string' ? { kind: parsed } : parsed;
}
export function classifyTurnPacket(packet) { return inspectTurnHeader(packet).kind; }

// Only the fixed STUN header is inspected. Attribute content, transaction IDs and
// TURN channel numbers are deliberately neither parsed nor retained.
export function inspectStunEnvelope(packet) {
  const datagramBytes = Buffer.isBuffer(packet) ? packet.length : undefined;
  if (!Buffer.isBuffer(packet) || packet.length < 4 || packet.length > 65535) return { kind: 'malformed', datagramBytes };
  if ((packet[0] & 0xc0) === 0x40) return { kind: 'channel_data', datagramBytes };
  if ((packet[0] & 0xc0) !== 0 || packet.length < 20 || packet.readUInt32BE(4) !== 0x2112a442) return { kind: 'non_stun', datagramBytes };
  const length = packet.readUInt16BE(2);
  if (length % 4 || 20 + length !== packet.length) return { kind: 'malformed', datagramBytes };
  const type = packet.readUInt16BE(0);
  return { kind: 'stun', datagramBytes,
    method: (type & 0x000f) | ((type & 0x00e0) >> 1) | ((type & 0x3e00) >> 2),
    messageClass: ((type >> 4) & 1) | ((type >> 7) & 2) };
}


// Execute original first and exactly once; preserve receiver, argument identities,
// return value, callback behaviour and original thrown error. Diagnostic errors cannot replace them.
export function observeAfterCall(original, observe, diagnosticFailure = () => {}) {
  const note = event => { try { observe(event); } catch { try { diagnosticFailure(); } catch {} } };
  return function (...args) {
    let result;
    try { result = Reflect.apply(original, this, args); }
    catch (error) { note({ args, threw: true }); throw error; }
    note({ args, threw: false });
    return result;
  };
}

export class ProxyClassTrace {
  constructor(proxy, { clock = () => performance.now(), maxEvents = 2048, maxClients = 64, maxResponseKinds = 16 } = {}) {
    if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 2048) throw new RangeError('diagnostic event bound must be between1and2048');
    if (!Number.isSafeInteger(maxClients) || maxClients < 1 || maxClients > 64) throw new RangeError('diagnostic client bound must be between1and64');
    if (!Number.isSafeInteger(maxResponseKinds) || maxResponseKinds < 1 || maxResponseKinds > 16) throw new RangeError('response kind bound must be between1and16');
    this.proxy = proxy; this.clock = clock; this.maxEvents = maxEvents;
    this.maxClients = maxClients; this.maxResponseKinds = maxResponseKinds;
    this.clientRecords = new WeakMap();
    this.listeners = new Map(); this.run = undefined; this.metadata = new WeakMap();
    this.originalSend = proxy.front.send; this.originalFlush = proxy.flush;
    const failure = () => { if (this.state) { this.state.diagnosticErrors++; this.state.coverage = 'INVALID_DIAGNOSTIC'; } };
    this.failure = failure;
    this.frontListener = (packet, peer) => this.observe(() => { this.attachBackends(); this.frontDatagram(packet, peer); });
    proxy.front.on('message', this.frontListener); // Installed after the proxy's own client-creation listener.
    this.sendWrapper = observeAfterCall(this.originalSend, e => this.observe(() => this.sent(e)), failure);
    proxy.front.send = this.sendWrapper;
    this.flushWrapper = observeAfterCall(this.originalFlush, e => this.observe(() => this.flushed(e)), failure);
    proxy.flush = this.flushWrapper;
  }
  observe(action) {
    if (this.run === undefined) return;
    let start;
    try { start = this.clock(); action(); }
    catch { this.failure(); }
    finally { if (start !== undefined) { try { this.state.selfTimeMs += Math.max(0, this.clock() - start); } catch { this.failure(); } } }
  }
  detachBackends() { for (const [socket, listener] of this.listeners) socket.removeListener('message', listener); this.listeners.clear(); }
  beginRun(run) {
    this.detachBackends(); this.metadata = new WeakMap(); this.run = run; this.lastFlushAt = undefined; this.clientRecords = new WeakMap();
    this.state = { run, coverage: 'OBSERVED_HEADER_INFERENCE', events: [], eventCursor: 0, eventOverflow: 0, diagnosticErrors: 0, originalSendThrows: 0, originalFlushThrows: 0, selfTimeMs: 0, classes: {}, proxyDropAtStart: this.proxy.counters.droppedPackets,
      runStartedAtMs: this.clock(), clients: [], clientCoverage: 'OBSERVED_AFTER_ORIGINAL_LISTENERS',
      clientAttachmentOverflowAttempts: 0, frontDatagramsWithoutIdentity: 0 };
    this.observe(() => this.attachBackends(true));
  }
  bucket(kind) {
    return this.state.classes[kind] ??= { receivedPackets: 0, receivedDatagramBytes: 0, enqueuedPackets: 0, rejectedBeforeQueue: 0, sendCalls: 0, sentDatagramBytes: 0, maxObservedQueueAgeMs: 0, queueAgeHistogram: [0, 0, 0, 0, 0, 0] };
  }
  event(event) {
    if (this.state.events.length < this.maxEvents) this.state.events.push(event);
    else { this.state.events[this.state.eventCursor] = event; this.state.eventCursor = (this.state.eventCursor + 1) % this.maxEvents; this.state.eventOverflow++; }
  }
  frontDatagram(packet, peer) {
    const client = peer && this.proxy.clients.get(`${peer.address}:${peer.port}`);
    const record = client && this.clientRecords.get(client);
    if (!record) { this.state.frontDatagramsWithoutIdentity++; this.state.clientCoverage = 'INCOMPLETE_CLIENT_IDENTITY'; return; }
    record.frontDatagramsObserved++;
    if (record.firstFrontDatagram === null) {
      record.firstFrontDatagram = inspectStunEnvelope(packet);
      if (record.coverage === 'FIRST_FRONT_NOT_YET_OBSERVED') record.coverage = 'OBSERVED_FIXED_HEADERS';
      record.firstFrontObservedAtMs = this.clock();
      record.firstFrontSinceRunStartMs = record.firstFrontObservedAtMs - this.state.runStartedAtMs;
    }
  }
  backendEnvelope(record, packet) {
    record.backendDatagramsObserved++;
    record.lastBackendObservedAtMs = this.clock();
    // Media dominates the hot path: no per-packet header object or response-kind
    // search unless this is structurally a complete fixed STUN envelope.
    if (!Buffer.isBuffer(packet) || packet.length < 20 || packet.length > 65535
      || (packet[0] & 0xc0) !== 0 || packet.readUInt32BE(4) !== 0x2112a442
      || packet.readUInt16BE(2) % 4 || packet.readUInt16BE(2) + 20 !== packet.length) {
      record.backendNonStunDatagrams++; return;
    }
    const header = inspectStunEnvelope(packet);
    const existing = record.backendResponses.find(x => x.kind === header.kind && x.method === header.method
      && x.messageClass === header.messageClass && x.datagramBytes === header.datagramBytes);
    if (existing) { existing.count++; return; }
    if (record.backendResponses.length >= this.maxResponseKinds) {
      record.responseKindsOverflow++; record.coverage = 'INCOMPLETE_RESPONSE_KINDS'; this.state.clientCoverage = 'INCOMPLETE_RESPONSE_KINDS'; return;
    }
    record.backendResponses.push({ ...header, count: 1 });
  }
  attachBackends(existingAtRunStart = false) {
    for (const client of this.proxy.clients.values()) if (!this.listeners.has(client.backend)) {
      if (this.state.clients.length >= this.maxClients) {
        this.state.clientAttachmentOverflowAttempts++; this.state.clientCoverage = 'INCOMPLETE_CLIENT_LIMIT'; continue;
      }
      const firstObservedAtMs = this.clock();
      const record = { ordinal: this.state.clients.length, existingAtRunStart, firstObservedAtMs,
        firstObservedSinceRunStartMs: firstObservedAtMs - this.state.runStartedAtMs,
        creationTimeScope: 'First observation after original listener; not an independently observed socket creation timestamp.',
        firstFrontDatagram: null, frontDatagramsObserved: 0, backendDatagramsObserved: 0, backendNonStunDatagrams: 0,
        backendResponses: [], responseKindsOverflow: 0, coverage: 'FIRST_FRONT_NOT_YET_OBSERVED' };
      this.state.clients.push(record); this.clientRecords.set(client, record);
      const listener = packet => this.observe(() => {
        this.backendEnvelope(record, packet);
        const atMs = this.clock(), header = inspectTurnHeader(packet), kind = header.kind, size = packet.length;
        const count = this.bucket(kind); count.receivedPackets++; count.receivedDatagramBytes += size;
        const enqueued = client.queue.at(-1)?.packet === packet;
        if (!enqueued) { count.rejectedBeforeQueue++; return; }
        count.enqueuedPackets++;
        const head = client.queue[0];
        const meta = { kind, atMs, size, controlPt: header.controlPt, countOrFmt: header.countOrFmt, aheadPackets: client.queue.length - 1, aheadBytes: client.queuedBytes - size,
          headKind: head?.packet === packet ? kind : this.metadata.get(head?.packet)?.kind ?? 'unobserved',
          headAgeMs: Math.max(0, atMs - (head?.at ?? atMs)) };
        this.metadata.set(packet, meta);
        if (kind === 'srtcp_header_range') this.event({ event: 'rtcp_received', atMs, controlPt: meta.controlPt, countOrFmt: meta.countOrFmt, datagramBytes: size, aheadPackets: meta.aheadPackets, aheadBytes: meta.aheadBytes, headKind: meta.headKind, headAgeMs: meta.headAgeMs });
      });
      client.backend.on('message', listener); // Original queue/limit handling runs first.
      this.listeners.set(client.backend, listener);
    }
  }
  sent({ args, threw }) {
    if (threw) { this.state.originalSendThrows++; return; }
    const packet = args[0], meta = this.metadata.get(packet);
    if (!meta) { this.state.unmatchedSendCalls = (this.state.unmatchedSendCalls || 0) + 1; this.state.coverage = 'INCOMPLETE_UNMATCHED_SENDS'; return; }
    const atMs = this.clock(), age = Math.max(0, atMs - meta.atMs), count = this.bucket(meta.kind);
    count.sendCalls++; count.sentDatagramBytes += meta.size;
    if (age > count.maxObservedQueueAgeMs) {
      count.maxObservedQueueAgeMs = age;
      count.maximumQueueObservation = { atMs, queueAgeMs: age, datagramBytes: meta.size, controlPt: meta.controlPt, countOrFmt: meta.countOrFmt, aheadPacketsAtReceive: meta.aheadPackets, aheadBytesAtReceive: meta.aheadBytes, headKindAtReceive: meta.headKind };
    }
    const bin = [5, 10, 25, 50, 100].findIndex(limit => age <= limit); count.queueAgeHistogram[bin < 0 ? 5 : bin]++;
    if (meta.kind === 'srtcp_header_range') this.event({ event: 'rtcp_send_call_returned', atMs, controlPt: meta.controlPt, countOrFmt: meta.countOrFmt, datagramBytes: meta.size, queueAgeMs: age, aheadPacketsAtReceive: meta.aheadPackets, aheadBytesAtReceive: meta.aheadBytes, headKindAtReceive: meta.headKind });
    this.metadata.delete(packet);
  }
  flushed({ threw }) {
    if (threw) { this.state.originalFlushThrows++; return; }
    const atMs = this.clock(), intervalMs = this.lastFlushAt === undefined ? undefined : atMs - this.lastFlushAt;
    this.lastFlushAt = atMs;
    if (intervalMs > 10) this.event({ event: 'flush_return_gap', atMs, intervalMs, queuedHeads: [...this.proxy.clients.values()].map(c => ({ queuedPackets: c.queue.length, queuedBytes: c.queuedBytes, headKind: this.metadata.get(c.queue[0]?.packet)?.kind ?? 'none_or_unobserved', headAgeMs: c.queue[0] ? Math.max(0, atMs - c.queue[0].at) : 0 })) });
  }
  summary() {
    if (!this.state) return undefined;
    return { run: this.run ?? this.state.run, coverage: this.state.coverage, diagnosticErrors: this.state.diagnosticErrors, eventOverflow: this.state.eventOverflow,
      selfTimeMs: this.state.selfTimeMs, recordedEvents: this.state.events.length, eventHistoryComplete: this.state.eventOverflow === 0, classes: structuredClone(this.state.classes),
      originalNetworkDrops: this.proxy.counters.droppedPackets - this.state.proxyDropAtStart,
      originalSendThrows: this.state.originalSendThrows, originalFlushThrows: this.state.originalFlushThrows,
      unmatchedSendCalls: this.state.unmatchedSendCalls || 0,
      clientObservation: { runStartedAtMs: this.state.runStartedAtMs, coverage: this.state.clientCoverage === 'OBSERVED_AFTER_ORIGINAL_LISTENERS' && this.state.clients.some(c => c.firstFrontDatagram === null || c.existingAtRunStart) ? 'INCOMPLETE_FIRST_FRONT_OR_CREATION' : this.state.clientCoverage,
        clientAttachmentOverflowAttempts: this.state.clientAttachmentOverflowAttempts, frontDatagramsWithoutIdentity: this.state.frontDatagramsWithoutIdentity,
        records: structuredClone(this.state.clients) } };
  }
  endRun() {
    if (this.run === undefined) return undefined;
    const result = { ...this.summary(), events: [...this.state.events.slice(this.state.eventCursor), ...this.state.events.slice(0, this.state.eventCursor)], limits: { maxEvents: this.maxEvents, maximumStunAttributes: 128, maxClients: this.maxClients, maxResponseKindsPerClient: this.maxResponseKinds },
      scope: 'Opt-in callbacks observed after original handlers/calls; header inference only, no packet content retained. Send-call return is not send completion/browser receipt. Queue delay starts after the original receive handler. Proxy TTL drops remain original aggregate, not per-class attribution. No timers added. Self time excludes snapshots and callback dispatch overhead; observation is not zero cost. Per-run client ordinals are observer-local, not nominated path identity. First front fixed STUN header and bounded backend STUN header/length counts do not establish allocation ownership. Existing clients at beginRun have incomplete creation/first-front coverage. No STUN attribute/transaction/address retained. First clear control PT/count-or-FMT only; PT205/FMT15 suggests TWCC but authentication/compound contents are opaque. Ring keeps latest bounded events; overwritten history cannot prove absence of head-of-line delay.' };
    this.run = undefined; this.detachBackends(); this.metadata = new WeakMap(); this.clientRecords = new WeakMap(); return result;
  }
  close() {
    this.detachBackends(); this.metadata = new WeakMap(); this.clientRecords = new WeakMap(); this.run = undefined;
    this.proxy.front.removeListener('message', this.frontListener);
    if (this.proxy.front.send === this.sendWrapper) this.proxy.front.send = this.originalSend;
    if (this.proxy.flush === this.flushWrapper) this.proxy.flush = this.originalFlush;
  }
}
