// Test-only, loopback TURN/UDP bottleneck. No credentials are inspected or logged.
import dgram from 'node:dgram';
import { performance } from 'node:perf_hooks';

export class TokenBucket {
  constructor(bps, now = 0, burst = 4500) {
    this.bps = bps; this.now = now; this.burst = burst; this.tokens = burst;
    // Includes normal idle refill discard. Only the proxy's separate
    // saturatedDiscardedRefillBytes counter implies a queued service deficit.
    this.discardedRefillBytes = 0;
  }
  setRate(bps, now) {
    this.refill(now); this.bps = bps;
  }
  refill(now) {
    const available = this.tokens + Math.max(0, now - this.now) * this.bps / 8000;
    this.discardedRefillBytes += Math.max(0, available - this.burst);
    this.tokens = Math.min(this.burst, available);
    this.now = now;
  }
  take(bytes, now) {
    this.refill(now);
    if (bytes > this.tokens) return false;
    this.tokens -= bytes; return true;
  }
}

export class TurnUdpProxy {
  constructor({ upstreamPort = 3488, bps = 4000000, maxQueueMs = 200 } = {}) {
    this.upstreamPort = upstreamPort; this.bps = bps; this.maxQueueMs = maxQueueMs;
    this.clients = new Map(); this.flushTiming = { calls: 0, maxIntervalMs: 0, intervalsOver10Ms: 0, intervalsOver25Ms: 0 }; this.lastFlushAt = undefined; this.front = dgram.createSocket('udp4');
    this.counters = { receivedPackets: 0, forwardedPackets: 0, forwardedBytes: 0, forwardedIpv4Bytes: 0, droppedPackets: 0 };
  }
  async start() {
    this.front.on('message', (bytes, peer) => {
      if (this.resetting) return;
      const key = `${peer.address}:${peer.port}`;
      let client = this.clients.get(key);
      if (!client) {
        if (this.clients.size >= 64) { this.counters.droppedPackets++; return; }
        const backend = dgram.createSocket('udp4');
        client = { backend, peer, queue: [], queuedBytes: 0, bucket: new TokenBucket(this.bps, performance.now()) };
        client.diagnostics = { receivedPackets: 0, forwardedPackets: 0, forwardedIpv4Bytes: 0,
          expiredPackets: 0, queueLimitDrops: 0, oversizedDrops: 0, maxQueuedBytes: 0, maxQueueAgeMs: 0, tokenBlockedFlushes: 0, saturatedDiscardedRefillBytes: 0, backloggedFlushIntervals: 0 };
        this.clients.set(key, client);
        backend.on('error', () => { this.counters.droppedPackets++; });
        backend.on('message', (packet, source) => {
          if (source.address !== '127.0.0.1' || source.port !== this.upstreamPort) return;
          this.counters.receivedPackets++; client.diagnostics.receivedPackets++;
          if (client.queuedBytes + packet.length > 262144 || packet.length + 28 > client.bucket.burst) {
            this.counters.droppedPackets++;
            if (packet.length + 28 > client.bucket.burst) client.diagnostics.oversizedDrops++;
            else client.diagnostics.queueLimitDrops++;
            return;
          }
          client.queue.push({ packet, at: performance.now() }); client.queuedBytes += packet.length;
          client.diagnostics.maxQueuedBytes = Math.max(client.diagnostics.maxQueuedBytes, client.queuedBytes);
        });
      }
      client.backend.send(bytes, this.upstreamPort, '127.0.0.1');
    });
    await new Promise((resolve, reject) => {
      this.front.once('error', reject); this.front.bind(0, '127.0.0.1', resolve);
    });
    this.port = this.front.address().port;
    this.timer = setInterval(() => this.flush(), 5); this.timer.unref();
    return this;
  }
  flush() {
    const now = performance.now();
    if (this.lastFlushAt !== undefined) {
      const interval = now - this.lastFlushAt;
      this.flushTiming.maxIntervalMs = Math.max(this.flushTiming.maxIntervalMs, interval);
      if (interval > 10) this.flushTiming.intervalsOver10Ms++;
      if (interval > 25) this.flushTiming.intervalsOver25Ms++;
    }
    this.lastFlushAt = now; this.flushTiming.calls++;
    for (const client of this.clients.values()) {
      const continuouslyBacklogged = client.lastFlushHadBacklog && client.queue.length > 0;
      const discardedBefore = client.bucket.discardedRefillBytes;
      while (client.queue.length) {
        const next = client.queue[0];
        client.diagnostics.maxQueueAgeMs = Math.max(client.diagnostics.maxQueueAgeMs, now - next.at);
        if (now - next.at > this.maxQueueMs) {
          client.queue.shift(); client.queuedBytes -= next.packet.length; this.counters.droppedPackets++; client.diagnostics.expiredPackets++; continue;
        }
        if (!client.bucket.take(next.packet.length + 28, now)) { client.diagnostics.tokenBlockedFlushes++; break; }
        client.queue.shift(); client.queuedBytes -= next.packet.length;
        this.front.send(next.packet, client.peer.port, client.peer.address);
        client.diagnostics.forwardedPackets++; client.diagnostics.forwardedIpv4Bytes += next.packet.length + 28;
        this.counters.forwardedPackets++; this.counters.forwardedBytes += next.packet.length; this.counters.forwardedIpv4Bytes += next.packet.length + 28;
      }
      if (continuouslyBacklogged) {
        client.diagnostics.backloggedFlushIntervals++;
        client.diagnostics.saturatedDiscardedRefillBytes += client.bucket.discardedRefillBytes - discardedBefore;
      }
      client.lastFlushHadBacklog = client.queue.length > 0;
    }
  }
  setBandwidth(bps) {
    if (!Number.isFinite(bps) || bps <= 0) throw new Error('bandwidth must be positive bits/second');
    this.bps = bps;
    for (const client of this.clients.values()) client.bucket.setRate(bps, performance.now());
  }
  stats() {
    return { ...this.counters, atMonotonicMs: performance.now(), flushTiming: { ...this.flushTiming }, perClient: [...this.clients.values()].map((c, index) => ({ index, ...c.diagnostics, queuedBytes: c.queuedBytes, discardedRefillBytes: c.bucket.discardedRefillBytes })), bpsPerClient: this.bps, clients: this.clients.size,
      queuedBytes: [...this.clients.values()].reduce((n, c) => n + c.queuedBytes, 0) };
  }
  async resetClients() {
    this.resetting = true;
    const sockets = [...this.clients.values()].map(c => c.backend); this.clients.clear();
    await Promise.all(sockets.map(socket => new Promise(resolve => {
      try { socket.close(resolve); } catch { resolve(); }
    })));
    for (const key of Object.keys(this.counters)) this.counters[key] = 0;
    this.flushTiming = { calls: 0, maxIntervalMs: 0, intervalsOver10Ms: 0, intervalsOver25Ms: 0 }; this.lastFlushAt = undefined;
    this.resetting = false;
  }
  async close() {
    clearInterval(this.timer);
    this.resetting = true;
    const sockets = [this.front, ...[...this.clients.values()].map(c => c.backend)];
    await Promise.all(sockets.map(socket => new Promise(resolve => {
      try { socket.close(resolve); } catch { resolve(); }
    })));
    this.clients.clear();
  }
}
