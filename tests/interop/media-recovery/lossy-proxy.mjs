// Test-only loopback UDP impairment proxy in front of the server's built-in TURN/UDP listener.
// Each direction (client→TURN "up", TURN→client "down") can drop a deterministic fraction of
// packets, add jitter, or black out completely. The RNG is seeded so a profile is reproducible.
// Forwarded TURN ChannelData carrying SRTP is observed at header level only (the RTP header is
// cleartext in SRTP): ssrc, sequence, timestamp, marker and payload type. Payloads stay opaque.
import dgram from 'node:dgram';

function rng(seed) { // mulberry32
  let a = seed >>> 0;
  return () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
}

const CLEAN = { loss: 0, jitterMs: 0, blackout: false };

export class LossyUdpProxy {
  constructor({ upstreamPort, seed = 1 }) {
    this.upstreamPort = upstreamPort; this.random = rng(seed);
    this.profile = { up: { ...CLEAN }, down: { ...CLEAN } };
    this.clients = new Map(); this.front = dgram.createSocket('udp4');
    this.counters = { up: { received: 0, dropped: 0, forwarded: 0 }, down: { received: 0, dropped: 0, forwarded: 0 } };
    this.rtp = { up: [], down: [] };
    this.ingressRtp = { up: [], down: [] };
    this.droppedRtp = { up: [], down: [] };
    this.forwardedRtcp = { up: [], down: [] };
    this.absSendTimeId = 2; // both servers negotiate abs-send-time as id 2 here
  }
  // TURN ChannelData (0x4000-0x7FFF) whose data is RTP (version 2, not an RTCP type).
  observe(direction, bytes, receivedAt, stage = 'forward') {
    if (bytes.length < 16 || bytes[0] < 0x40 || bytes[0] > 0x7f) return;
    const d = bytes.subarray(4);
    if (d[0] >> 6 !== 2) return;
    if (d[1] >= 192 && d[1] <= 223) {
      // SRTCP exposes the first header only. Encrypted compound members and
      // REMB/TWCC payload values cannot be recovered from this header.
      if (stage === 'forward') {
        const log = this.forwardedRtcp[direction];
        log.push({ t: performance.now(), pt: d[1], fmt: d[0] & 0x1f, rtcpBytes: 4 * (d.readUInt16BE(2) + 1) });
        if (log.length > 200000) log.splice(0, 50000);
      }
      return;
    }
    const log = (stage === 'ingress' ? this.ingressRtp : stage === 'drop' ? this.droppedRtp : this.rtp)[direction];
    if (stage !== 'forward') {
      const header = { t: performance.now(), receivedAt, pt: d[1] & 0x7f, m: d[1] >> 7, seq: d.readUInt16BE(2), ts: d.readUInt32BE(4), ssrc: d.readUInt32BE(8) };
      log.push(header);
      if (log.length > 200000) log.splice(0, 50000);
      return header;
    }
    // One-byte (0xBEDE) or two-byte (0x100x) header extension element ids.
    let ext = '', ast = null;
    const cc = d[0] & 0x0f, x = (d[0] >> 4) & 1, off = 12 + cc * 4;
    if (x && d.length >= off + 4) {
      const profile = d.readUInt16BE(off), words = d.readUInt16BE(off + 2), end = Math.min(d.length, off + 4 + words * 4);
      const ids = [];
      for (let i = off + 4; i < end;) {
        if (profile === 0xbede) { const b0 = d[i]; if (b0 === 0) { i++; continue; } const id = b0 >> 4, len = (b0 & 0xf) + 1; if (id === 15) break; ids.push(id); if (id === this.absSendTimeId && len === 3) ast = ((d[i + 1] << 16) | (d[i + 2] << 8) | d[i + 3]) / (1 << 18); i += 1 + len; }
        else if ((profile & 0xfff0) === 0x1000) { const id = d[i]; if (id === 0) { i++; continue; } const len = d[i + 1]; ids.push(id); if (id === this.absSendTimeId && len === 3) ast = ((d[i + 2] << 16) | (d[i + 3] << 8) | d[i + 4]) / (1 << 18); i += 2 + len; }
        else break;
      }
      ext = (profile === 0xbede ? '' : '2B:') + ids.sort((p, q) => p - q).join(',');
    }
    log.push({ t: performance.now(), receivedAt, pt: d[1] & 0x7f, m: d[1] >> 7, seq: d.readUInt16BE(2), ts: d.readUInt32BE(4), ssrc: d.readUInt32BE(8), len: d.length, ext, ast });
    if (log.length > 200000) log.splice(0, 50000);
  }
  // RTP headers seen before impairment and handed to UDP after impairment.
  // Offsets use the proxy clock; these rows do not assert browser delivery.
  streamRtp(direction, streams, from, to, origin) {
    const keys = new Set(streams.map(({ssrc, pt}) => `${ssrc}:${pt}`));
    const select = rows => rows.filter(p => p.t >= from && p.t <= to && keys.has(`${p.ssrc}:${p.pt}`))
      .map(({t, seq, ts, ssrc, pt}) => ({ms: +(t-origin).toFixed(3), seq, ts, ssrc, pt}));
    return {ingress: select(this.ingressRtp[direction]), forwarded: select(this.rtp[direction])};
  }
  // Match Chrome frame timestamps to the packet cohort on this leg, including
  // RTX SSRCs. Match within the phase to avoid reusing a wrapped timestamp.
  frameRtp(direction, timestamps, from, to, origin) {
    const wanted = new Set(timestamps);
    const select = packets => packets.filter(p => p.t >= from && p.t <= to && wanted.has(p.ts))
      .map(({t, pt, m, seq, ts, ssrc}) => ({ms: +(t-origin).toFixed(3), pt, marker: m, seq, ts, ssrc}));
    return {ingress: select(this.ingressRtp[direction]), forwarded: select(this.rtp[direction])};
  }
  lossRtp(direction, streams, from, to) {
    // Forwarded entries retain their actual departure time, but are selected
    // by the same arrival window as dropped/ingress packets.
    const inCohort = p => p.receivedAt >= from && p.receivedAt < to;
    const ingress = this.ingressRtp[direction].filter(inCohort);
    const forwarded = this.rtp[direction].filter(inCohort);
    const dropped = this.droppedRtp[direction].filter(inCohort);
    const onStream = (p, s) => p.ssrc === s.ssrc && p.pt === s.pt;
    return streams.map(s => ({
      rid: s.rid ?? null, ssrc: s.ssrc, pt: s.pt, codec: s.codec,
      ingress: ingress.filter(p => onStream(p, s)).length,
      forwarded: forwarded.filter(p => onStream(p, s)).length,
      dropped: dropped.filter(p => onStream(p, s))
        .map(({t, seq, ts}) => ({ms: +(t-from).toFixed(3), seq, ts})),
    }));
  }
  rtcpTimeline(direction, from, to, origin) {
    return this.forwardedRtcp[direction].filter(p => p.t >= from && p.t <= to)
      .map(({t, ...header}) => ({ms: +(t-origin).toFixed(3), ...header}));
  }
  // Per-SSRC header facts for packets the client actually received in [from, to] (proxy clock).
  rtpSummary(direction, from, to = Infinity) {
    const by = new Map();
    for (const p of this.rtp[direction]) {
      if (p.t < from || p.t > to) continue;
      let s = by.get(p.ssrc);
      if (!s) { s = { ssrc: p.ssrc, pt: p.pt, packets: 0, markers: 0, missing: 0, reordered: 0, maxSeqJump: 0, maxTsJump: 0, extIds: {}, last: null }; by.set(p.ssrc, s); }
      s.packets++; s.markers += p.m; s.extIds[p.ext] = (s.extIds[p.ext] || 0) + 1;
      // abs-send-time against actual departure (proxy arrival on the same host):
      // |Δarrival - Δabs-send-time| per consecutive packet, ignoring 64 s wraps.
      if (p.ast !== null && s.lastAst) {
        let dAst = p.ast - s.lastAst.ast; if (dAst < -32) dAst += 64;
        const dArr = (p.t - s.lastAst.t) / 1000;
        const err = Math.abs(dArr - dAst) * 1000;
        s.astErrMsSum = (s.astErrMsSum || 0) + err; s.astN = (s.astN || 0) + 1; s.astErrMsMax = Math.max(s.astErrMsMax || 0, err);
      }
      if (p.ast !== null) {
        // One-way delay up to a constant: arrival minus abs-send-time, unwrapped per SSRC.
        if (s.astBase === undefined) { s.astBase = p.ast; s.astWraps = 0; s.prevAstRaw = p.ast; }
        if (p.ast < s.prevAstRaw - 32) s.astWraps++; s.prevAstRaw = p.ast;
        const owd = p.t / 1000 - (p.ast + 64 * s.astWraps);
        if (s.owdMin === undefined) s.owdMin = s.owdMax = owd;
        s.owdMin = Math.min(s.owdMin, owd); s.owdMax = Math.max(s.owdMax, owd);
        const bucket = Math.floor((p.t - from) / 500); (s.owdBuckets ??= {})[bucket] = Math.max(s.owdBuckets[bucket] ?? -Infinity, owd);
        s.lastAst = p;
      }
      if (s.last) {
        const ds = (p.seq - s.last.seq + 65536) % 65536;
        if (ds === 0 || ds >= 32768) s.reordered++;
        else { s.missing += ds - 1; s.maxSeqJump = Math.max(s.maxSeqJump, ds); s.maxTsJump = Math.max(s.maxTsJump, (p.ts - s.last.ts + 2 ** 32) % 2 ** 32); }
      }
      if (!s.last || ((p.seq - s.last.seq + 65536) % 65536) < 32768) s.last = p;
    }
    // Send pattern: per RTP timestamp (frame) span from first to last packet, and
    // inter-arrival quantiles within the window, per SSRC.
    const pattern = new Map();
    for (const p of this.rtp[direction]) {
      if (p.t < from || p.t > to) continue;
      let q = pattern.get(p.ssrc);
      if (!q) { q = { frames: new Map(), gaps: [], prev: null, bytes: 0 }; pattern.set(p.ssrc, q); }
      const f = q.frames.get(p.ts) || { a: p.t, b: p.t, n: 0 }; f.b = p.t; f.n++; q.frames.set(p.ts, f);
      if (q.prev !== null) q.gaps.push(p.t - q.prev); q.prev = p.t; q.bytes += p.len;
    }
    const quant = (xs, k) => { if (!xs.length) return null; const v = [...xs].sort((x, y) => x - y); return +v[Math.min(v.length - 1, Math.floor(k * v.length))].toFixed(3); };
    return [...by.values()].map(({ last, lastAst, astErrMsSum, astN, astBase, astWraps, prevAstRaw, owdMin, owdMax, ...s }) => {
      const q = pattern.get(s.ssrc); const spans = q ? [...q.frames.values()].filter(f => f.n > 1).map(f => f.b - f.a) : [];
      const dur = q && q.gaps.length ? q.gaps.reduce((x, y) => x + y, 0) / 1000 : 0;
      const { owdBuckets, ...rest } = s; s = rest; s.owdBuckets = owdBuckets;
      return { ...s, astErrMsMean: astN ? +(astErrMsSum / astN).toFixed(3) : null,
        owdRangeMs: owdMin === undefined ? null : +((owdMax - owdMin) * 1000).toFixed(2),
        owdPer500ms: s.owdBuckets ? Object.entries(s.owdBuckets).sort((x, y) => x[0] - y[0]).map(([, v]) => +((v - owdMin) * 1000).toFixed(1)) : null,
        bitrateKbps: dur ? Math.round(q.bytes * 8 / dur / 1000) : null,
        frameSpanMsP50: quant(spans, 0.5), frameSpanMsP90: quant(spans, 0.9),
        gapMsP10: quant(q?.gaps || [], 0.1), gapMsP50: quant(q?.gaps || [], 0.5), gapMsP90: quant(q?.gaps || [], 0.9) };
    }).sort((a, b) => b.packets - a.packets);
  }
  now() { return performance.now(); }
  set(direction, profile) { this.profile[direction] = { ...CLEAN, ...profile }; }
  clean() { this.profile = { up: { ...CLEAN }, down: { ...CLEAN } }; }
  pass(direction, bytes, send) {
    const c = this.counters[direction]; const p = this.profile[direction]; c.received++;
    const receivedAt = performance.now();
    const rtp = this.observe(direction, bytes, receivedAt, 'ingress');
    if (p.blackout || (rtp && p.dropRtp?.(rtp)) || (p.loss > 0 && this.random() < p.loss)) {
      c.dropped++;
      if (!p.blackout) this.observe(direction, bytes, receivedAt, 'drop');
      return;
    }
    c.forwarded++;
    if (p.jitterMs > 0) setTimeout(() => send(receivedAt), Math.floor(this.random() * p.jitterMs)); else send(receivedAt);
  }
  async start() {
    this.front.on('message', (bytes, peer) => {
      if (this.closed) return;
      const key = `${peer.address}:${peer.port}`;
      let backend = this.clients.get(key);
      if (!backend) {
        backend = dgram.createSocket('udp4');
        backend.on('error', () => {});
        backend.on('message', (packet, source) => {
          if (source.port !== this.upstreamPort || this.closed) return;
          this.pass('down', packet, receivedAt => { if (!this.closed) { this.observe('down', packet, receivedAt); this.front.send(packet, peer.port, peer.address); } });
        });
        this.clients.set(key, backend);
      }
      this.pass('up', bytes, receivedAt => { if (!this.closed) { this.observe('up', bytes, receivedAt); backend.send(bytes, this.upstreamPort, '127.0.0.1'); } });
    });
    await new Promise((resolve, reject) => { this.front.once('error', reject); this.front.bind(0, '127.0.0.1', resolve); });
    this.port = this.front.address().port;
    return this;
  }
  stats() { return JSON.parse(JSON.stringify({ counters: this.counters, clients: this.clients.size })); }
  async close() {
    this.closed = true;
    await Promise.all([this.front, ...this.clients.values()].map(s => new Promise(r => { try { s.close(r); } catch { r(); } })));
  }
}
