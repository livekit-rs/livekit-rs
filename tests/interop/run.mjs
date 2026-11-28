import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname, join, isAbsolute } from 'node:path';
import { chromium } from 'playwright';
import { AccessToken, RoomServiceClient } from 'livekit-server-sdk';
import { SignalRequest, SignalResponse, WrappedJoinRequest, JoinRequest } from '@livekit/protocol';
import { gunzipSync } from 'node:zlib';
import { normalizer } from './normalize.mjs';
import { TurnUdpProxy } from './network-proxy.mjs';
import { ACCURACY_SETTLE_US, accuracyVerdict, evaluateRecoveryProgress, evaluateSeedResponse } from './bandwidth-timeline-gate.mjs';

const root = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const baseUrl = process.env.LIVEKIT_URL || 'ws://127.0.0.1:7880';
const peerUrls = process.env.LIVEKIT_PEER_URLS ? JSON.parse(process.env.LIVEKIT_PEER_URLS) : [baseUrl, baseUrl];
const url = process.env.LIVEKIT_URL || peerUrls[0];
assert.ok(Array.isArray(peerUrls) && peerUrls.length === 2 && peerUrls.every(value => typeof value === 'string' && ['ws:', 'wss:'].includes(new URL(value).protocol)), 'LIVEKIT_PEER_URLS requires two explicit WS/WSS entry URLs');
const clusterMediaProbe = process.env.CLUSTER_MEDIA_PROBE === 'true';
if (clusterMediaProbe) assert.notEqual(new URL(peerUrls[0]).origin, new URL(peerUrls[1]).origin, 'cluster media probe requires distinct entries');
const secureSignaling = peerUrls.every(value => new URL(value).protocol === 'wss:');
const apiKey = process.env.LIVEKIT_API_KEY || 'devkey';
const apiSecret = process.env.LIVEKIT_API_SECRET || 'secret';
const single = process.env.SINGLE_PC !== 'false';
const runs = Number(process.env.RUNS || 1);
const duration = Number(process.env.DURATION_MS || 30000);
const video = process.env.VIDEO === 'true';
const videoCodec = process.env.VIDEO_CODEC || 'vp8';
const scalabilityMode = process.env.SCALABILITY_MODE || undefined;
const svcSwitch = process.env.SVC_SWITCH === 'true';
const svcTemporal = process.env.SVC_TEMPORAL === 'true';
const complexVideo = process.env.COMPLEX_VIDEO === 'true';
const turnTls = process.env.TURN_TLS === 'true';
const spki = process.env.TLS_SPKI || undefined;
const networkProfile = process.env.NETWORK_PROFILE || undefined;
if (networkProfile) assert.equal(networkProfile, 'congest-recover');
if (networkProfile) assert.ok(process.env.FORCE_RELAY === 'true', 'network bottleneck requires forced TURN relay');
const bandwidthAccuracy = process.env.BANDWIDTH_ACCURACY === 'true';
if (bandwidthAccuracy) {
  assert.ok(networkProfile, 'bandwidth accuracy requires the unchanged congestion profile');
  assert.ok(process.env.METRICS_URL, 'bandwidth accuracy requires the original server metrics endpoint');
}
const evaluateBudgetTimeline = bandwidthAccuracy ? (await import('./bandwidth-timeline-gate.mjs')).evaluateBudgetTimeline : undefined;
const proxyClassTraceEnabled = process.env.OPTIN_PROXY_CLASS_TRACE === 'true';
const ProxyClassTrace = proxyClassTraceEnabled ? (await import('./proxy-class-trace.mjs')).ProxyClassTrace : undefined;
const relay = process.env.FORCE_RELAY === 'true';
const extended = process.env.EXTENDED === 'true';
const label = process.env.SERVER_LABEL || 'rust';
const output = process.env.INTEROP_OUTPUT_DIR || join(root, 'output');
if (process.env.INTEROP_OUTPUT_DIR) assert.ok(isAbsolute(output), 'INTEROP_OUTPUT_DIR must be absolute');
await mkdir(output, { recursive: !process.env.INTEROP_OUTPUT_DIR });
const sdk = await readFile(require.resolve('livekit-client'));
const html = await readFile(join(root, 'client.html'));
const host = createServer((req, res) => {
  res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'text/javascript' : 'text/html' });
  res.end(req.url === '/sdk.js' ? sdk : html);
});
await new Promise(resolve => host.listen(0, '127.0.0.1', resolve));
const pageUrl = `http://127.0.0.1:${host.address().port}`;
let networkProxy, proxyClassTrace, browser, networkMonitor, networkMonitoring;
let networkPhase = 'startup';
let networkRun = 0;
const networkTimeline = [];
const bandwidthRateTrace = [];
const api = new RoomServiceClient(url.replace(/^ws/, 'http'), apiKey, apiSecret);
const evidence = { label, secureSignaling, sdk: '2.23.0', serverSdk: '2.19.2', browser: 'NOT_LAUNCHED', single, video, videoCodec, scalabilityMode, svcSwitch, svcTemporal, complexVideo, turnTls, networkProfile, relay, extended,
  peerEntries: peerUrls.map(value => { const endpoint = new URL(value); return { origin: endpoint.origin, path: endpoint.pathname }; }), clusterMediaProbe, duration, startedAt: new Date().toISOString(), runs: [], status: 'RUNNING' };
if (bandwidthAccuracy) evidence.bandwidthAccuracy = { status: 'RUNNING', settleUs: 5000000, allocationHorizonUs: 1000000, interpretation: 'Hypothetical allocation horizon, not observed ACK throughput', runs: [], rateTrace: bandwidthRateTrace };
let currentRoom, activePages = [], activeSignals = [], activeProfiles = [];
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function metricsSnapshot() {
  if (!process.env.METRICS_URL) return undefined;
  const response = await fetch(process.env.METRICS_URL, { signal: AbortSignal.timeout(2000) });
  assert.ok(response.ok, 'metrics endpoint unavailable');
  const values = {};
  for (const line of (await response.text()).split('\n')) {
    const match = /^(livekit_(?:estimated_capacity_bps|allocated_bps|allocation_overshoot_bps|paused_video_tracks|probe_attempts|probe_successes|probe_failures|congestion_events))(?:\{[^}]*\})? (\d+)$/.exec(line);
    if (match) values[match[1]] = (values[match[1]] || 0) + Number(match[2]);
  }
  // An empty object is truthy, and returning one made every gauge arm evaluate `undefined < undefined`
  // on a server that publishes no allocator gauges (the pinned Go exposes none of these names).
  // Absent gauges must read as absent so the caller can use the server's own records instead.
  return Object.keys(values).length ? values : undefined;
}

// The 18s congestion window kept audio flowing on both implementations (measured deltas of 180
// and 184 samples), so a window that delivers far fewer events means the real-time audio path
// stalled. The floor sits well below the measurement and far above silence.
const AUDIO_CONTINUITY_FLOOR = 100;

// A congestion response has to cut the allocation, not merely change a label. The filed receipts of
// this profile separate cleanly: an allocator that responds drops to 0.10 (2,720,253 -> 270,892) and
// 0.49 (2,203,442 -> 1,071,466) of its own baseline, while an allocator that ignores the bottleneck
// never goes below 1.07 of it (frozen 200Mbps pre-fix run: 3,219,418 -> 3,442,914; congestion control
// switched off: 3,663,759 -> 3,901,836). The ratio sits between the two populations.
const ALLOCATOR_CUT_RATIO = 0.7;

// The proxy rates this profile applies, named once so the wall, the rate trace, the recovery
// assertion and the gauge gate cannot drift apart.
const CONGESTED_BOTTLENECK_BPS = 120000;
const OPEN_BOTTLENECK_BPS = 4000000;

async function metricsSeries(durationMs, intervalMs) {
  const samples = [];
  const until = Date.now() + durationMs;
  while (Date.now() < until) {
    const metrics = await metricsSnapshot();
    if (metrics) samples.push({ atMs: Date.now(), ...metrics });
    const wait = until - Date.now();
    if (wait > 0) await sleep(Math.min(intervalMs, wait));
  }
  return samples;
}

const allocatorTargetRank = layer => ((layer?.Spatial ?? -1) * 100) + (layer?.Temporal ?? -1);

// The allocator response is a server-side fact, and the pinned Go oracle reports it only in its
// stream-allocator log records (`stream allocation: <mode>` carrying the `allocation` object; the
// requested-bandwidth key is spelled `BandwidthRquested` upstream). INTEROP_SERVER_LOG points at
// the log of the server this leg is bound to so the same gate can be evaluated on the Go oracle.
// `alloc cost sample` (crates/rtc-media/src/forward.rs) is the same decision on this side: one
// record per allocation pass per transport, with `caps` (granted spatial layer, -1 = paused) and
// `costs` holding each track's `([observed], [declared], [resolved])` bitrate triplets. Normalise it
// onto the Go key names so one arm reads either engine. Without this the arm the reference
// implementation satisfies was blind here: the filed Rust receipt has 314 records, 63 of them
// deficient at needed 2,346,972 > requested 64,000 with caps [-1], and the Go-only parser returned
// `[]` for all of them.
function rustAllocationRows(line, at) {
  const payload = /\{"transport": .*\}$/.exec(line)?.[0];
  if (!payload) return [];
  let record;
  try {
    record = JSON.parse(payload);
  } catch {
    return [];
  }
  const caps = ((record.caps ?? '').match(/-?\d+/g) ?? []).map(Number);
  // `capacity_bps` is the committed channel capacity for this transport at the moment of the pass
  // (crates/rtc-feedback/src/feedback.rs `committed`), filed as evidence next to the request series.
  // The pinned Go has no equivalent field in `stream allocation:`, so it stays undefined there and
  // only the request/rank series is claimed across engines.
  const capacity = Number(/Some\((\d+)\)/.exec(record.capacity_bps ?? '')?.[1]);
  const capacityBps = Number.isSafeInteger(capacity) ? capacity : undefined;
  const triplets = [...String(record.costs ?? '').matchAll(/\[([\d,\s]+)\],\s*\[([\d,\s]+)\],\s*\[([\d,\s]+)\]/g)]
    .map(m => [m[1], m[2], m[3]].map(list => list.split(',').map(value => Number(value.trim()))));
  return triplets.map((triplet, index) => {
    const resolved = triplet[2];
    const cap = caps[index] ?? -1;
    return {
      atMs: at, mode: 'alloc cost sample', participant: `transport-${record.transport}`,
      track: `layer-${index}`, IsDeficient: record.deficient === true, capacityBps,
      BandwidthNeeded: Math.max(...resolved),
      BandwidthRquested: cap >= 0 && resolved[cap] !== undefined ? resolved[cap] : 0,
      TargetLayer: { Spatial: cap, Temporal: 0 },
    };
  }).filter(row => row.BandwidthNeeded > 0);
}

function allocatorLogRecords(logPath) {
  if (!logPath) return undefined;
  let text;
  try {
    text = readFileSync(logPath, 'utf8');
  } catch (error) {
    return { error: String(error) };
  }
  const rows = [];
  for (const line of text.split('\n')) {
    const isGo = line.includes('stream allocation:');
    if (!isGo && !line.includes('alloc cost sample')) continue;
    const at = Date.parse(/^(\S+)/.exec(line)?.[1] ?? '');
    const record = /"allocation": (\{.*?\}), "lastAllocation"/.exec(line);
    if (!Number.isFinite(at)) continue;
    if (!isGo) {
      rows.push(...rustAllocationRows(line, at));
      continue;
    }
    if (!record) continue;
    try {
      const participant = /"participant": "([^"]+)"/.exec(line)?.[1];
      const track = /"trackID": "([^"]+)"/.exec(line)?.[1];
      rows.push({ atMs: at, mode: /stream allocation: (\S+)/.exec(line)[1], participant, track, ...JSON.parse(record[1]) });
    } catch {
      /* a truncated debug line is not evidence */
    }
  }
  return rows;
}

// `remote feedback sample` (crates/rtc-media/src/feedback.rs) is one record per REMB per subscriber
// transport: the receiver estimate the browser reported, the capacity committed for that transport,
// the channel-observer verdict and the forwarded stream SSRCs. Those SSRCs are the subscriber's own
// inbound-rtp SSRCs, which the timeline snapshots already carry, so a record binds to the browser it
// describes with no server-side identity plumbing (the Rust media logs contain no participant name).
// The pinned Go emits no `remote feedback sample` line at all (measured on the filed C108 go legs: 0
// rows), so this arm is Rust-side: on a Go leg it yields no records and reports INSUFFICIENT instead
// of borrowing the aggregate gauge.
function receiverFeedbackRecords(logPath, timelineRows) {
  if (!logPath) return { records: [], ssrcBindings: 0, error: 'no server log' };
  let text;
  try {
    text = readFileSync(logPath, 'utf8');
  } catch (error) {
    return { records: [], ssrcBindings: 0, error: String(error) };
  }
  const ssrcToClient = new Map();
  for (const sample of timelineRows) {
    for (const [client, row] of (sample.clients ?? []).entries()) {
      for (const peer of row.peers ?? []) {
        for (const stat of peer.stats ?? []) {
          if (stat.type === 'inbound-rtp' && typeof stat.ssrc === 'number') ssrcToClient.set(stat.ssrc, client);
        }
      }
    }
  }
  const groups = new Map();
  for (const line of text.split('\n')) {
    if (!line.includes('remote feedback sample')) continue;
    const payload = line.slice(line.indexOf('{"mode"'));
    if (!payload.startsWith('{"mode"')) continue;
    let record;
    try {
      record = JSON.parse(payload);
    } catch {
      continue;
    }
    const atMs = Date.parse(/^(\S+)/.exec(line)?.[1] ?? '');
    if (!Number.isFinite(atMs)) continue;
    const ssrcs = [...String(record.streams ?? '').matchAll(/\("(\d+)", (\d+)/g)].map(match => Number(match[2]));
    const signature = [...ssrcs].sort((a, b) => a - b).join(',');
    if (!groups.has(signature)) {
      const clients = [...new Set(ssrcs.map(ssrc => ssrcToClient.get(ssrc)).filter(v => v !== undefined))];
      groups.set(signature, { key: clients.length === 1 ? `client-${clients[0]}` : undefined,
        pathBound: clients.length === 1, ssrcs, rows: [] });
    }
    groups.get(signature).rows.push({ atMs, estimateBps: record.receiver_estimate_bps,
      capacityBps: record.committed_capacity_bps, observation: record.observation });
  }
  const records = [...groups.values()].flatMap(group => group.rows.map(row =>
    ({ key: group.key ?? `ssrc:${group.ssrcs.join(',')}`, pathBound: group.pathBound, ...row })));
  return { records, ssrcBindings: ssrcToClient.size };
}

function videoBitrateHints(sdp) {
  const hints = new Map();
  for (const section of sdp.split(/(?=^m=)/m).filter(s => s.startsWith('m=video '))) {
    const mid = /^a=mid:(.+)$/m.exec(section)?.[1].trim();
    for (const match of section.matchAll(/^a=fmtp:(\d+) (.+)$/gm)) {
      for (const param of match[2].trim().split(';')) {
        const hint = /^(x-google-(?:start|min|max)-bitrate)=(\d+)$/.exec(param.trim());
        if (hint) hints.set(`${mid}/${match[1]}/${hint[1]}`, hint[2]);
      }
    }
  }
  return hints;
}

try {
  if (networkProfile) {
    networkProxy = new TurnUdpProxy({ upstreamPort: Number(process.env.TURN_UPSTREAM_PORT || 3488) });
    await networkProxy.start();
    if (proxyClassTraceEnabled) {
      proxyClassTrace = new ProxyClassTrace(networkProxy);
      evidence.proxyClassTrace = { enabled: true, runs: [] };
    }
    networkMonitor = setInterval(() => {
      if (!activePages.length || networkMonitoring) return;
      networkMonitoring = (async () => {
        const sample = { run: networkRun, atMonotonicMs: performance.now(), phase: networkPhase, proxy: networkProxy.stats() };
        sample.clients = await Promise.all(activePages.map(page => page.evaluate(async () => {
          const report = interop.report();
          const peers = await Promise.all((window.interopPeerConnections || []).map(async pc => ({
            connection: pc.connectionState,
            // All entries are public transport/encoder/decoder counters; no SDP,
            // bearer token, TURN credential or SDK implementation is copied.
            stats: [...(await pc.getStats()).values()].filter(entry =>
              ['inbound-rtp', 'outbound-rtp', 'remote-inbound-rtp', 'candidate-pair', 'transport'].includes(entry.type))
          })));
          return { videoFrames: report.videoFrames, lastDimensions: report.videoDimensions.at(-1),
            audibleSamples: report.audibleSamples, connection: report.connection,
            streamState: report.events.filter(event => event[0] === 'stream-state').at(-1), peers };
        }).catch(error => ({ error: String(error) }))));
        if (bandwidthAccuracy) sample.metricsReadStartedUs = Math.floor(performance.now() * 1000);
        sample.metrics = await metricsSnapshot().catch(error => ({ error: String(error) }));
        if (bandwidthAccuracy) sample.metricsReadCompletedUs = Math.ceil(performance.now() * 1000);
        if (proxyClassTrace) sample.proxyClassTrace = proxyClassTrace.summary();
        networkTimeline.push(sample);
      })().finally(() => { networkMonitoring = undefined; });
    }, 1000);
    networkMonitor.unref();
    evidence.networkTimeline = networkTimeline;
  }
browser = await chromium.launch({ channel: process.env.BROWSER_CHANNEL || 'chrome', headless: true,
  args: ['--autoplay-policy=no-user-gesture-required', ...(spki ? [`--ignore-certificate-errors-spki-list=${spki}`] : [])] });
  evidence.browser = browser.version();
  for (let run = 0; run < runs; run++) {
    networkRun = run;
    proxyClassTrace?.beginRun(run);
    networkPhase = 'startup';
    if (bandwidthAccuracy) {
      const atUs = Math.floor(performance.now() * 1000);
      bandwidthRateTrace.push({ run, atUs, settledByUs: atUs, bpsPerClient: networkProxy.bps });
    }
    currentRoom = `interop-${Date.now()}-${run}`;
    await api.createRoom({ name: currentRoom, emptyTimeout: 10, departureTimeout: 2 });
    const contexts = await Promise.all([browser.newContext(), browser.newContext()]);
    const pages = await Promise.all(contexts.map(c => c.newPage()));
    activePages = pages;
    const normalize = normalizer(), signals = [], profiles = [];
    activeSignals = signals; activeProfiles = profiles;
    for (const [i, page] of pages.entries()) {
      page.on('websocket', ws => {
        const signalAddress = new URL(ws.url());
        const signalingTransport = { protocol: signalAddress.protocol, origin: signalAddress.origin, path: signalAddress.pathname };
        const query = signalAddress.searchParams;
        const join = query.get('join_request');
        if (join) {
          const wrapped = WrappedJoinRequest.fromBinary(Buffer.from(join, 'base64url'));
          const request = JoinRequest.fromBinary(wrapped.compression === 1 ? gunzipSync(wrapped.joinRequest) : wrapped.joinRequest);
          profiles.push({ client: i, transport: signalingTransport, info: normalize(request.clientInfo?.toJson()), settings: request.connectionSettings?.toJson() });
        } else {
          profiles.push({ client: i, transport: signalingTransport, info: { sdk: query.get('sdk'), version: query.get('version'),
            protocol: query.has('protocol') ? Number(query.get('protocol')) : undefined,
            clientProtocol: query.has('client_protocol') ? Number(query.get('client_protocol')) : undefined },
            settings: { autoSubscribe: query.get('auto_subscribe') } });
        }
        const capture = (direction, MessageType) => event => {
          try {
            const message = typeof event.payload === 'string' ? MessageType.fromJsonString(event.payload) : MessageType.fromBinary(event.payload);
            signals.push({ client: i, direction, message: normalize(message.toJson()) });
          } catch (error) { signals.push({ client: i, direction, decodeError: String(error) }); }
        };
        ws.on('framesent', capture('client-to-server', SignalRequest));
        ws.on('framereceived', capture('server-to-client', SignalResponse));
      });
      await page.addInitScript(({ udpProxyPort, upstreamPort }) => {
        window.interopPeerConnections = [];
        window.interopNegotiationErrors = [];
        window.interopSignalSockets = [];
        const NativeSocket = window.WebSocket;
        window.WebSocket = class extends NativeSocket {
          constructor(...args) { super(...args); window.interopSignalSockets.push(this); }
        };
        const Native = window.RTCPeerConnection;
        const routedConfiguration = config => udpProxyPort && config?.iceServers ? { ...config, iceServers: config.iceServers.map(server => ({
          ...server, urls: (Array.isArray(server.urls) ? server.urls : [server.urls]).map(url =>
            url === `turn:127.0.0.1:${upstreamPort}?transport=udp` ? `turn:127.0.0.1:${udpProxyPort}?transport=udp` : url)
        })) } : config;
        window.RTCPeerConnection = class extends Native {
          setConfiguration(config) { return super.setConfiguration(routedConfiguration(config)); }
          async createOffer(options) {
            try { return await super.createOffer(options); }
            catch (error) { window.interopNegotiationErrors.push('createOffer: ' + String(error)); throw error; }
          }
          async createAnswer(options) {
            try { return await super.createAnswer(options); }
            catch (error) { window.interopNegotiationErrors.push('createAnswer: ' + String(error)); throw error; }
          }
          async setRemoteDescription(description) {
            try { return await super.setRemoteDescription(description); }
            catch (error) { window.interopNegotiationErrors.push(String(error)); throw error; }
          }
          async setLocalDescription(description) {
            try { return await super.setLocalDescription(description); }
            catch (error) { window.interopNegotiationErrors.push(String(error)); throw error; }
          }
          constructor(...args) {
            args[0] = routedConfiguration(args[0]);
            super(...args); window.interopPeerConnections.push(this); this.interopSnapshot = {};
            const timer = setInterval(async () => {
              if (this.connectionState === 'closed') { clearInterval(timer); return; }
              this.interopSnapshot = { connection: this.connectionState, ice: this.iceConnectionState,
                local: this.localDescription?.toJSON(), remote: this.remoteDescription?.toJSON(),
                stats: [...(await this.getStats()).values()] };
            }, 1000);
          }
        };
      }, { udpProxyPort: networkProxy?.port, upstreamPort: Number(process.env.TURN_UPSTREAM_PORT || 3488) });
      page.on('pageerror', e => console.error(`client ${i}:`, e.message));
      page.on('console', m => { if (m.type() === 'error') console.error(`client ${i}:`, m.text()); });
      await page.goto(pageUrl);
    }
    const connections = await Promise.all(pages.map(async (page, i) => {
      const token = new AccessToken(apiKey, apiSecret, { identity: `client-${i}`, ttl: '10m' });
      token.addGrant({ roomJoin: true, room: currentRoom, canPublish: true, canSubscribe: true, canPublishData: true });
      return page.evaluate(opts => interop.connect(opts), { url: peerUrls[i], token: await token.toJwt(), single, frequency: 440 + 440 * i, relay, adaptiveStream: !(svcSwitch || networkProfile) });
    }));
    await Promise.all(pages.map(page => page.waitForFunction(() => interop.report().audibleSamples >= 10, null, { timeout: 25000 })));
    await Promise.all(pages.map(page => page.evaluate(() => interop.data())));
    if (video) await Promise.all(pages.map(page => page.evaluate(options => interop.video(options), { codec: videoCodec, scalabilityMode, width: svcSwitch || complexVideo ? 960 : 640, height: svcSwitch || complexVideo ? 544 : 360, complex: complexVideo })));
    if (networkProfile) await Promise.all(pages.map(page => page.waitForFunction(() => interop.report().videoFrames >= 30, null, { timeout: 30000 })));
    await sleep(duration);
    const reports = await Promise.all(pages.map(page => page.evaluate(async () => {
      const transports = [], outboundVideo = [];
      for (const pc of window.interopPeerConnections || []) {
        if (pc.connectionState === 'closed') continue;
        const stats = await pc.getStats();
        for (const stat of stats.values()) {
          if (stat.type === 'outbound-rtp' && stat.kind === 'video') outboundVideo.push({
            mimeType: stats.get(stat.codecId)?.mimeType, scalabilityMode: stat.scalabilityMode,
            frameWidth: stat.frameWidth, frameHeight: stat.frameHeight, framesEncoded: stat.framesEncoded,
            bytesSent: stat.bytesSent, targetBitrate: stat.targetBitrate, qualityLimitationReason: stat.qualityLimitationReason
          });
        }
        for (const transport of stats.values()) {
          if (transport.type !== 'transport' || !transport.selectedCandidatePairId) continue;
          const pair = stats.get(transport.selectedCandidatePairId);
          transports.push({ dtlsState: transport.dtlsState,
            localCandidateType: stats.get(pair.localCandidateId)?.candidateType,
            remoteCandidateType: stats.get(pair.remoteCandidateId)?.candidateType,
            remoteAddress: stats.get(pair.remoteCandidateId)?.address,
            remotePort: stats.get(pair.remoteCandidateId)?.port,
            protocol: stats.get(pair.localCandidateId)?.protocol,
            relayProtocol: stats.get(pair.localCandidateId)?.relayProtocol,
            turnUrl: stats.get(pair.localCandidateId)?.url,
            bytesSent: pair.bytesSent, bytesReceived: pair.bytesReceived,
            roundTripTime: pair.currentRoundTripTime });
        }
      }
      return { ...interop.report(), transports, outboundVideo, inbound: (await interop.stats()).map(s => ({
        kind: s.kind, packetsReceived: s.packetsReceived, bytesReceived: s.bytesReceived,
        totalAudioEnergy: s.totalAudioEnergy, totalSamplesReceived: s.totalSamplesReceived, concealedSamples: s.concealedSamples
      })) };
    })));
    evidence.pendingReports = reports;
    for (const [i, report] of reports.entries()) {
      assert.equal(report.connection, 'connected', `client ${i} disconnected`);
      assert.ok(report.audibleSamples >= duration / 100 * 0.8, `client ${i} decoded insufficient audio`);
      assert.deepEqual(report.received.filter(x => x.topic === 'interop').map(x => x.seq), Array.from({ length: 100 }, (_, n) => n));
      assert.equal(report.errors.length, 0);
      assert.ok(report.transports.length > 0 && report.transports.every(t => t.dtlsState === 'connected'));
      if (relay) assert.ok(report.transports.every(t => t.localCandidateType === 'relay'), 'forced relay did not select a relay candidate');
      if (turnTls) assert.ok(report.transports.every(t => t.localCandidateType === 'relay' && t.relayProtocol === 'tls'), 'TURN TLS was not selected');
      if (scalabilityMode) assert.ok(report.outboundVideo.some(t => t.scalabilityMode === scalabilityMode && t.mimeType?.toLowerCase() === `video/${videoCodec}`), 'requested SVC encoder mode was not active');
      if (video) assert.ok(report.videoFrames >= 30, `client ${i} decoded insufficient video`);
    }
    const clusterResults = {};
    if (clusterMediaProbe) {
      assert.ok(video && videoCodec === 'vp8', 'cluster media acceptance requires actual VP8');
      for (const [i, report] of reports.entries()) {
        const expected = i === 0 ? 880 : 440;
        assert.ok(report.received.filter(row => row.topic === 'interop').every(row => row.from === `client-${1-i}`), `client ${i} received data without the actual remote participant identity`);
        const tones = report.toneMeasurements.filter(sample => sample.identity === `client-${1-i}` && sample.rms > 0.01);
        assert.ok(tones.length >= 10, `client ${i} lacks remote tone measurements`);
        assert.ok(tones.filter(sample => Math.abs(sample.frequencyHz-expected) < 35).length >= tones.length * 0.8, `client ${i} decoded wrong remote tone`);
        assert.ok(profiles.some(profile => profile.client === i && profile.transport.origin === new URL(peerUrls[i]).origin), `client ${i} never used its assigned entry`);
      }
      clusterResults.initialToneAudioDataVp8 = 'PASS';
      clusterResults.reconnect = [];
      for (const i of [0, 1]) {
        const before = await pages[i].evaluate(() => interop.report());
        await pages[i].evaluate(() => interop.reconnect());
        await pages[i].waitForFunction(count => interop.report().events.slice(count).some(event => event[0] === 'state' && event[1] === 'connected'), before.events.length, { timeout: 30000 });
        const topic = `cluster-resume-${i}`;
        await pages[1-i].evaluate(topic => interop.data({ count: 20, topic }), topic);
        await pages[i].waitForFunction(({ topic, audio, video }) => {
          const current = interop.report();
          return current.received.filter(row => row.topic === topic).length === 20 && current.audibleSamples >= audio + 10 && current.videoFrames >= video + 10;
        }, { topic, audio: before.audibleSamples, video: before.videoFrames }, { timeout: 30000 });
        const after = await pages[i].evaluate(() => interop.report());
        assert.deepEqual(after.received.filter(row => row.topic === topic).map(row => row.seq), Array.from({ length: 20 }, (_, seq) => seq));
        assert.ok(after.received.filter(row => row.topic === topic).every(row => row.from === `client-${1-i}`), 'resumed data lacks the real other participant identity');
        const freshTones = after.toneMeasurements.filter(sample => sample.sampleIndex > before.audioSamples && sample.identity === `client-${1-i}` && sample.rms > 0.01);
        assert.ok(freshTones.length >= 5 && freshTones.filter(sample => Math.abs(sample.frequencyHz-(i === 0 ? 880 : 440)) < 35).length >= freshTones.length*0.8, `client ${i} resumed without correct decoded remote tone`);
        assert.equal(after.errors.length, 0);assert.equal(after.connection, 'connected');
        clusterResults.reconnect.push({ client: i, audioGrowth: after.audibleSamples-before.audibleSamples, videoGrowth: after.videoFrames-before.videoFrames, orderedData: 20, freshToneSamples: freshTones.length, status: 'PASS' });
      }
    }
    const participants = await api.listParticipants(currentRoom);
    assert.equal(participants.length, 2);
    const networkResults = [];
    // Filed under two names: the pending alias is dropped when the run completes, so the phase rows
    // (client signals, gauge windows, recovery metrics) survive into the evidence of a green leg too,
    // while a leg that throws keeps them under the pending name for the failure dump.
    evidence.pendingNetworkResults = evidence.networkResults = networkResults;
    if (networkProxy) {
      const bandwidthPathsVerifiedAtUs = bandwidthAccuracy ? Math.ceil(performance.now() * 1000) : undefined;
      assert.ok(networkProxy.stats().clients >= 2, 'TURN traffic bypassed the bottleneck proxy');
      assert.ok(reports.every(report => report.transports.every(t => t.turnUrl === `turn:127.0.0.1:${networkProxy.port}?transport=udp`)), 'selected TURN route bypassed proxy');
      // Startup BWE can already have paused video. Establish a genuinely
      // active decoded baseline before attributing a new pause to congestion.
      const baselineDeadline = Date.now() + 45000;
      let before;
      while (Date.now() < baselineDeadline) {
        const first = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
        await sleep(3000);
        const next = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
        const active = next.every((report, i) => report.connection === 'connected'
          && report.errors.length === 0 && report.videoFrames >= first[i].videoFrames + 20
          && (!complexVideo || (report.videoDimensions.slice(-8).length === 8
            && report.videoDimensions.slice(-8).every(frame => frame.width >= 800)))
          && report.audibleSamples >= first[i].audibleSamples + 20
          && report.events.filter(e => e[0] === 'stream-state').at(-1)?.[2] !== 'paused'
          && !report.events.slice(first[i].events.length).some(e => e[0] === 'stream-state' && e[2] === 'paused'));
        if (active) { before = next; break; }
      }
      assert.ok(before, 'BWE startup did not establish three seconds of active decoded baseline (high resolution for complex video) before congestion');
      const phaseStartStats = networkProxy.stats();
      const initialMetrics = await metricsSnapshot();
      networkResults.push({ phase: 'baseline', reports: before, metrics: initialMetrics, proxy: phaseStartStats, status: 'PASS' });
      networkPhase = 'constrained';
      const constraintRequestedUs = bandwidthAccuracy ? Math.floor(performance.now() * 1000) : undefined;
      const constraintWallMs = Date.now();
      networkProxy.setBandwidth(CONGESTED_BOTTLENECK_BPS);
      if (bandwidthAccuracy) {
        const settledByUs = Math.ceil(performance.now() * 1000);
        // Conservative rate history: lower capacity takes effect no later than
        // completion; retain the old higher bound through this call interval.
        bandwidthRateTrace.push({ run, requestedAtUs: constraintRequestedUs, atUs: settledByUs, settledByUs, bpsPerClient: CONGESTED_BOTTLENECK_BPS });
      }
      // Sample the allocator gauges across the whole window instead of reading them once at its end:
      // in the filed post-fix run the allocation cratered to 270,892 bps and then a successful probe
      // re-expanded the commit to 2,475,607 bps before the window closed, so an end-of-window reading
      // can miss a response that actually happened.
      const congestionWindowMs = 18000;
      const windowSamples = await metricsSeries(congestionWindowMs, 500);
      const constrained = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      const congestedAt = Date.now();
      const congestedStats = networkProxy.stats();
      const constrainedMetrics = await metricsSnapshot();
      // File the congested window before asserting: a failing leg has to show
      // which arm (pause event vs layer reduction) each client actually saw.
      const congestedRow = { phase: 'congested', metrics: constrainedMetrics, initialMetrics, proxy: congestedStats, reports: constrained, status: 'PRE-ASSERT' };
      networkResults.push(congestedRow);
      congestedRow.signals = constrained.map((report, i) => ({
        client: i,
        paused: report.events.slice(before[i].events.length).some(e => e[0] === 'stream-state' && e[2] === 'paused'),
        lower: report.videoDimensions.slice(-8).some(f => f.width < (before[i].videoDimensions.at(-1)?.width || 0)),
        streamStateEvents: report.events.slice(before[i].events.length).filter(e => e[0] === 'stream-state'),
        baselineLastWidth: before[i].videoDimensions.at(-1)?.width ?? null,
        recentWidths: report.videoDimensions.slice(-8).map(f => f.width),
        framesGrowth: report.videoFrames - before[i].videoFrames,
        audioGrowth: report.audibleSamples - before[i].audibleSamples,
      }));
      assert.ok(congestedStats.droppedPackets > phaseStartStats.droppedPackets, 'bottleneck did not actually drop queued datagrams');
      const armFailures = congestedRow.signals.filter(signal => !signal.paused && !signal.lower).map(signal => signal.client);
      const audioGaps = congestedRow.signals.filter(signal => signal.audioGrowth < AUDIO_CONTINUITY_FLOOR).map(signal => signal.client);
      // The drop counter above proves the bottleneck was real, so the SFU reaction may be read
      // from three sources and only some of them exist per server build:
      //  - clientArm: Chrome reports a stream pause or a narrower frame;
      //  - gaugeArm: the Rust allocator published a cut (allocated down against its own baseline,
      //    capacity down, congestion events counted);
      //  - allocatorLogArm: the pinned Go oracle exports none of those gauge names and records the
      //    same decision only as `stream allocation` log lines.
      // The client arm cannot be the only gate: the unmodified official v1.13.10 binary leaves both
      // subscribers at 960x544 with no stream-state event under this same profile, which is filed
      // in livekit-docs as server/evidence/2026-10-23/official-congestion-baseline.json. A leg that
      // only read the client arm would be asserting behaviour the reference implementation lacks.
      const allocatorLog = allocatorLogRecords(process.env.INTEROP_SERVER_LOG);
      const peakRequested = new Map();
      const peakTarget = new Map();
      const allocatorWindow = [];
      for (const record of Array.isArray(allocatorLog) ? allocatorLog : []) {
        const key = `${record.participant}/${record.track}`;
        if (record.atMs < constraintWallMs) {
          peakRequested.set(key, Math.max(peakRequested.get(key) ?? 0, record.BandwidthRquested ?? 0));
          peakTarget.set(key, Math.max(peakTarget.get(key) ?? -1, allocatorTargetRank(record.TargetLayer)));
        } else if (record.atMs <= congestedAt) {
          allocatorWindow.push({ key, atMs: record.atMs, mode: record.mode, deficient: record.IsDeficient === true,
            requested: record.BandwidthRquested ?? 0, needed: record.BandwidthNeeded ?? 0, target: record.TargetLayer ?? null,
            peakRequestedBefore: peakRequested.get(key) ?? 0, peakTargetBefore: peakTarget.get(key) ?? -1 });
        }
      }
      // `IsDeficient` with `BandwidthNeeded` above `BandwidthRquested` is the allocator's own record of
      // holding back below what the stream wants; every pre-constraint record of the paired Go run is
      // deficient=false, so an uncongested allocator does not produce it. The withholding has to be
      // visible as a smaller request or a lower target layer than that stream had before the wall.
      const allocatorLogArm = allocatorWindow.some(row => row.deficient && row.needed > row.requested
        && ((row.peakRequestedBefore > 0 && row.requested < row.peakRequestedBefore)
          || (row.peakTargetBefore >= 0 && allocatorTargetRank(row.target) < row.peakTargetBefore)));
      const allocatedWindow = windowSamples.map(sample => sample.livekit_allocated_bps).filter(Number.isFinite);
      const capacityWindow = windowSamples.map(sample => sample.livekit_estimated_capacity_bps).filter(Number.isFinite);
      // The estimate claim cannot be relative to the pre-wall snapshot alone: the estimator is seeded
      // optimistically (100,000,000 per subscriber, so 200,000,000 aggregate in every filed run of
      // this profile), and `min < baseline` is then true for a server that never saw the wall - the
      // filed pre-fix receipt reads 200,000,000 -> 100,392,978 (and 392,978 at its lowest) while the
      // allocation never cut. It cannot be absolute against the rate the wall removed either: the
      // gauge sums each subscriber connection's committed capacity
      // (crates/rtc-runtime/src/manager.rs), so a profile where one of two connections collapses
      // still reports the other connection's untouched 100,000,000 seed - the responding Rust run
      // reads 200,000,000 -> 100,000,000 with 0 of 26 qualified samples ever at or below the 8,000,000
      // open-path aggregate, a condition no server with an uncongested second subscriber can pass.
      // The claim is therefore a conjunction of three independent server-side signals inside the same
      // predeclared qualified window: the committed capacity fell below its own baseline, the
      // allocator actually paused a managed video track, and the congestion counter advanced. A
      // teardown moves the sum but produces neither of the latter two, and the congestion-control-off
      // control reports 0 on every sample, so the positivity test still disqualifies it.
      const proxyPaths = reports.reduce((count, report) => count + report.transports.length, 0);
      const capacityUnimpairedBps = OPEN_BOTTLENECK_BPS * proxyPaths;
      const capacityQualified = windowSamples
        .filter(sample => sample.atMs >= constraintWallMs + ACCURACY_SETTLE_US / 1000)
        .map(sample => sample.livekit_estimated_capacity_bps).filter(Number.isFinite);
      const gaugeWindow = allocatedWindow.length >= 4 && initialMetrics ? {
        windowMs: congestionWindowMs, samples: windowSamples.length,
        allocatedBaseline: initialMetrics.livekit_allocated_bps, allocatedMin: Math.min(...allocatedWindow),
        allocatedAtWindowEnd: constrainedMetrics?.livekit_allocated_bps ?? null,
        capacityBaseline: initialMetrics.livekit_estimated_capacity_bps,
        capacityMin: capacityQualified.length ? Math.min(...capacityQualified) : null,
        capacityWindowSeries: capacityWindow, capacityQualifiedSeries: capacityQualified,
        capacityQualifiedSamples: capacityQualified.length,
        capacityPositiveSamples: capacityQualified.filter(value => value > 0).length,
        capacityAtOrBelowUnimpairedSamples: capacityQualified.filter(value => value <= capacityUnimpairedBps).length,
        proxyPaths, capacityUnimpairedBps,
        pausedVideoTracksMax: Math.max(...windowSamples.map(sample => sample.livekit_paused_video_tracks ?? 0)),
        congestionEvents: constrainedMetrics?.livekit_congestion_events ?? 0,
      } : undefined;
      const gaugeArm = !!gaugeWindow
        && gaugeWindow.allocatedMin > 0
        && gaugeWindow.allocatedMin <= gaugeWindow.allocatedBaseline * ALLOCATOR_CUT_RATIO
        && gaugeWindow.capacityQualifiedSamples >= 4
        && gaugeWindow.capacityPositiveSamples === gaugeWindow.capacityQualifiedSamples
        && gaugeWindow.capacityMin !== null && gaugeWindow.capacityMin < gaugeWindow.capacityBaseline
        && gaugeWindow.pausedVideoTracksMax >= 1
        && gaugeWindow.congestionEvents > (initialMetrics.livekit_congestion_events ?? 0);
      const clientArm = armFailures.length === 0;
      congestedRow.response = { clientArm, clientsWithoutSignal: armFailures, audioArm: audioGaps.length === 0,
        clientsWithAudioGap: audioGaps, audioContinuityFloor: AUDIO_CONTINUITY_FLOOR, gaugeArm, allocatorLogArm,
        allocatorCutRatio: ALLOCATOR_CUT_RATIO, allocatorLogSource: process.env.INTEROP_SERVER_LOG ?? null,
        allocatorLogError: allocatorLog?.error ?? null, allocatorWindow, constraintWallMs, congestedAt,
        gaugeWindow: gaugeWindow ?? { note: `fewer than four in-window allocator gauge samples (${windowSamples.length})` },
        gauges: { baseline: initialMetrics, constrained: constrainedMetrics } };
      assert.ok(audioGaps.length === 0, `audio was not continuous through the congestion window for client(s) ${audioGaps.join(',')}: ${JSON.stringify(congestedRow.signals)}`);
      // INTEROP_RECORD_ONLY keeps the response gate as data instead of an assertion so the run can
      // reach the recovery phase; the CI leg still asserts.
      if (!process.env.INTEROP_RECORD_ONLY) {
        assert.ok(clientArm || gaugeArm || allocatorLogArm,
          `SFU showed no congestion response under the ${CONGESTED_BOTTLENECK_BPS}bps bottleneck: no client pause/layer reduction (clients ${armFailures.join(',')}), `
          + `no allocator gauge cut across the window (${JSON.stringify(congestedRow.response.gaugeWindow)}), `
          + `no deficient in-window allocator record withholding bandwidth or layers (${JSON.stringify(allocatorWindow)})`);
      }
      congestedRow.status = clientArm ? 'PASS' : (gaugeArm || allocatorLogArm ? 'PASS-SERVER-SIGNAL-ONLY' : `NO-SERVER-RESPONSE:${armFailures.join(',')}`);
      networkPhase = 'recovering';
      const restoreRequestedUs = bandwidthAccuracy ? Math.floor(performance.now() * 1000) : undefined;
      const restoreAtMs = Date.now();
      networkProxy.setBandwidth(OPEN_BOTTLENECK_BPS);
      if (bandwidthAccuracy) bandwidthRateTrace.push({ run, requestedAtUs: restoreRequestedUs, atUs: restoreRequestedUs,
        settledByUs: Math.ceil(performance.now() * 1000), bpsPerClient: OPEN_BOTTLENECK_BPS });
      const recoveryWant = pages.map((page, i) => ({ videoFrames: constrained[i].videoFrames,
        audioSamples: constrained[i].audibleSamples,
        minWidth: Math.min(...before[i].videoDimensions.slice(-8).map(f => f.width)) }));
      // The recovery arm measures the estimate staircase, so its window has to be as long as the
      // mechanism the pinned Go implements. After the wall clears, the committed capacity can only
      // grow at a probe finalize, and a finalize commits the highest receiver estimate seen during
      // that probe (references/livekit-stable pkg/sfu/bwe/remotebwe/remote_bwe.go:331-338) - the
      // ordinary estimate path is decrease-only (remote_bwe.go:205-212), so a rising REMB alone never
      // moves capacity. Each successful round resets the interval to the 3s base and multiplies the
      // probe duration by 1.5 from 200ms up to 20s (pkg/sfu/ccutils/probe_regulator.go:80-98), so the
      // commit points land ~3.2/6.5/10/14/18/22/27/34/42/53/67s after the wall clears and each commit
      // is capped by the REMB the client had reached by then. Measured on this profile (server log of
      // the 2026-11-27 C108 dbg6 leg): the wall cleared at t=37s, seven finalizes in 28s took one
      // subscriber 250->1118kbps with the client REMB at 1464kbps and still climbing ~7%/s, while the
      // 960x544 layer costs 1.9-2.2Mbps measured, so the upgrade needs the 8th-10th commit - 45-60s
      // after restore. The window is therefore the staircase envelope, not a frame budget, and the
      // progress assertion below is what keeps it from passing on a stalled estimate.
      const RECOVERY_WINDOW_MS = 75000;
      // The client arms may consume the whole window before the allocator series is read, so the
      // per-path claim is scored over the window plus the observation phase that follows it instead
      // of a hard cut where the client arms stop waiting.
      const RECOVERY_OBSERVE_SLACK_MS = 20000;
      const recoveryStartedAt = Math.floor(performance.now());
      const recoveryArms = await Promise.all(pages.map(async (page, i) => {
        try {
          await page.waitForFunction(({ videoFrames, audioSamples, minWidth }) => {
            const report = interop.report();
            return report.videoFrames >= videoFrames + 20 && report.audibleSamples >= audioSamples + 5
              && report.videoDimensions.slice(-8).every(f => f.width >= minWidth);
          }, recoveryWant[i], { timeout: RECOVERY_WINDOW_MS, polling: 500 });
          return { client: i, recovered: true, waitedMs: Math.floor(performance.now()) - recoveryStartedAt };
        } catch (error) {
          // A bare Playwright timeout does not say which of the three conditions the client missed,
          // and the recovery claim is about frame flow versus delivered resolution. The counters are
          // re-read once so the failure names the arm that did not recover.
          const report = await page.evaluate(() => interop.report());
          return { client: i, recovered: false, connection: report.connection,
            videoFrames: report.videoFrames, needVideoFrames: recoveryWant[i].videoFrames + 20,
            audibleSamples: report.audibleSamples, needAudibleSamples: recoveryWant[i].audioSamples + 5,
            last8Widths: report.videoDimensions.slice(-8).map(frame => frame.width),
            needWidthAtLeast: recoveryWant[i].minWidth, error: String(error).split('\n')[0] };
        }
      }));
      const recoveryRows = networkTimeline.filter(sample => sample.run === run && sample.phase === 'recovering');
      const recoveryCapacity = recoveryRows.map(sample => sample.metrics?.livekit_estimated_capacity_bps).filter(Number.isFinite);
      const recoverySteps = recoveryCapacity.filter((value, idx) => idx > 0 && value > recoveryCapacity[idx - 1] * 1.05).length;
      // The same dual-source problem gate 1 has: the pinned Go binary exports none of the livekit_*
      // gauge names (its /metrics has 0 rows of these names in every filed leg), so a gauge-only
      // staircase cannot be satisfied by the reference implementation at all. Allocation records are
      // the pairable source - both engines emit one per allocation pass - and the claim has to be made
      // per nominated path, because the capacity gauge sums one committed capacity per subscriber
      // connection (crates/rtc-runtime/src/manager.rs): on the 2026-11-27 C108 leg transport-1 went
      // 100,000,000 -> 39,434 and then climbed 68,525/83,694/129,481/163,244/205,713/362,104 while
      // transport-2 never congested and held its seed, so the summed gauge moved 0.03-0.10% per step
      // and the old 5%-staircase-on-the-sum arm saw 0 up-steps in 26 qualified samples on a server
      // that demonstrably responded.
      const recoveryLog = allocatorLogRecords(process.env.INTEROP_SERVER_LOG);
      const recoveryRecords = (Array.isArray(recoveryLog) ? recoveryLog : []).map(record => ({
        key: record.participant, atMs: record.atMs, requestedBps: record.BandwidthRquested ?? 0,
        targetRank: allocatorTargetRank(record.TargetLayer), capacityBps: record.capacityBps,
        deficient: record.IsDeficient === true,
      }));
      // The window is the client-side recovery wait plus the observation slack after it; the dip is
      // scored across the whole impaired-plus-recovery period, not up to the restore: in the filed Go
      // leg the wall lands at t=0 and the deepest cut (Spatial 2/T 3 -> Spatial 1/T 1, 312,592bps
      // requested) is recorded at t=20.7s, ~2.7s after the wall had already lifted at t=18.0s.
      const recoveryProgress = evaluateRecoveryProgress({ records: recoveryRecords,
        wallAtMs: constraintWallMs, restoreAtMs, windowMs: RECOVERY_WINDOW_MS + RECOVERY_OBSERVE_SLACK_MS,
        cutRatio: ALLOCATOR_CUT_RATIO });
      const gaugeStaircase = recoveryCapacity.length >= 8 && recoverySteps >= 3;
      const recovery = { windowMs: RECOVERY_WINDOW_MS, arms: recoveryArms, capacitySamples: recoveryCapacity.length,
        capacityFirst: recoveryCapacity[0] ?? null, capacityLast: recoveryCapacity.at(-1) ?? null,
        capacityMax: recoveryCapacity.length ? Math.max(...recoveryCapacity) : null, capacityUpSteps: recoverySteps,
        restoreAtMs, gaugeStaircase, progress: recoveryProgress,
        certifiedBy: recoveryProgress.status === 'PASS' ? 'per-path-allocator-records'
          : gaugeStaircase ? 'aggregate-gauge-staircase' : recoveryProgress.status,
        allocatorLogSource: process.env.INTEROP_SERVER_LOG ?? null,
        allocatorLogError: recoveryLog?.error ?? null,
        widths: recoveryRows.map(sample => ({ waitedMs: Math.floor(sample.atMonotonicMs - recoveryStartedAt),
          widths: sample.clients.map(client => client.lastDimensions?.width ?? null) })) };
      for (const arm of recoveryArms) {
        assert.ok(arm.recovered, `recovery did not restore the pre-congestion layer for client ${arm.client}: ${JSON.stringify(arm)}`);
      }
      // The window is long, so the leg has to prove the estimate actually moved inside it: a server
      // whose committed capacity is pinned fails here rather than after 75s of waiting for pixels.
      assert.ok(recoveryProgress.status === 'PASS' || gaugeStaircase,
        `recovery did not progress on any nominated path: ${JSON.stringify(recoveryProgress)}, `
        + `aggregate gauge ${JSON.stringify({ capacitySamples: recoveryCapacity.length, upSteps: recoverySteps,
          first: recoveryCapacity[0] ?? null, last: recoveryCapacity.at(-1) ?? null })}`);
      networkPhase = 'recovered-observation';
      const stableBefore = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      await sleep(5000);
      const stableAfter = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      for (let i = 0; i < 2; i++) {
        assert.equal(stableAfter[i].connection, 'connected'); assert.equal(stableAfter[i].errors.length, 0);
        if (stableAfter[i].videoFrames < stableBefore[i].videoFrames + 20
          || stableAfter[i].audibleSamples < stableBefore[i].audibleSamples + 20) {
          const widths = stableAfter[i].videoDimensions.slice(-8).map(frame => frame.width);
          assert.fail(`recovery did not sustain media: client=${i} videoFrames=${stableBefore[i].videoFrames}->`
            + `${stableAfter[i].videoFrames} need>=${stableBefore[i].videoFrames + 20} `
            + `audibleSamples=${stableBefore[i].audibleSamples}->${stableAfter[i].audibleSamples} `
            + `need>=${stableBefore[i].audibleSamples + 20} last8Widths=${JSON.stringify(widths)}`);
        }
      }
      const recoveredMetrics = await metricsSnapshot();
      if (initialMetrics && constrainedMetrics && recoveredMetrics) {
        // Judge the wall response against this leg's own baseline, not against the rate the wall
        // removed: the gauge sums each subscriber connection's committed capacity
        // (crates/rtc-runtime/src/manager.rs), so an untouched 100,000,000 seed on the second
        // connection keeps the aggregate above OPEN_BOTTLENECK_BPS x paths no matter how hard the
        // first one collapses. The 0.7 factor is the same allocator cut ratio gate 1 uses.
        assert.ok(constrainedMetrics.livekit_estimated_capacity_bps > 0
          && constrainedMetrics.livekit_estimated_capacity_bps
            <= initialMetrics.livekit_estimated_capacity_bps * ALLOCATOR_CUT_RATIO,
          `BWE did not fall under the wall: ${constrainedMetrics.livekit_estimated_capacity_bps}bps is not at or below 70% of this leg's baseline ${initialMetrics.livekit_estimated_capacity_bps}bps (${JSON.stringify(congestedRow.response.gaugeWindow)})`);
        assert.ok(recoveredMetrics.livekit_estimated_capacity_bps > constrainedMetrics.livekit_estimated_capacity_bps, 'BWE did not recover');
      }
      networkResults.push({ phase: 'recovered', metrics: recoveredMetrics, proxy: networkProxy.stats(), reports: stableAfter, recovery, status: 'PASS' });
      if (bandwidthAccuracy) {
        await networkMonitoring;
        const runSamples = networkTimeline.filter(sample => sample.run === run);
        const result = evaluateBudgetTimeline({ samples: runSamples,
          rateChanges: bandwidthRateTrace.filter(change => change.run === run),
          expectedClients: proxyPaths,
          pathIdentityVerifiedAtUs: bandwidthPathsVerifiedAtUs });
        // The aggregate gauge is a sum, so it cannot tell "the SFU ignored its receiver" from "this
        // receiver never reported congestion". The per-path records can, and they are the only form
        // of this claim a two-path leg with one unresponsive receiver can satisfy at all: with one
        // path holding its 100,000,000 seed the sum is >= 100,000,000 forever and every aggregate
        // sample is PRIOR (measured 2026-11-28: constrained gauge 100,031,685 = 100,000,000 + 31,685).
        const receiver = evaluateSeedResponse({
          records: receiverFeedbackRecords(process.env.INTEROP_SERVER_LOG, runSamples).records,
          wallAtMs: constraintWallMs, restoreAtMs });
        const runStatus = result.status === 'PASS' ? 'PASS'
          : result.status === 'INSUFFICIENT' && receiver.status === 'PASS' ? 'PASS_PER_PATH' : result.status;
        evidence.bandwidthAccuracy.runs.push({ run, ...result, runStatus, receiverResponse: receiver });
        evidence.bandwidthAccuracy.status = accuracyVerdict(evidence.bandwidthAccuracy.runs);
        assert.ok(result.status === 'PASS' || receiver.status === 'PASS',
          `stable original budget gauge violated the allocation-horizon invariant, or required measurement `
          + `metadata was missing, and no subscriber path was certified against its own receiver estimate: `
          + `aggregate ${result.status} (${JSON.stringify(result.epochs ?? result.reason)}), `
          + `per-path ${receiver.status} (${JSON.stringify(receiver.paths)})`);
      }
    }
    const svcResults = [];
    if (svcSwitch) {
      for (const quality of [0, 2]) {
        await Promise.all(pages.map(page => page.evaluate(q => interop.videoQuality(q), quality)));
        await Promise.all(pages.map(page => page.waitForFunction(({ low }) => {
          const frames = interop.report().videoDimensions.slice(-8);
          return frames.length >= 8 && frames.every(f => low ? f.width <= 320 : f.width >= 800);
        }, { low: quality === 0 }, { timeout: 20000 })));
        const dimensions = await Promise.all(pages.map(page => page.evaluate(() => interop.report().videoDimensions.slice(-8))));
        svcResults.push({ quality, dimensions, status: 'PASS' });
      }
    }
    const temporalResults = [];
    if (svcTemporal) {
      assert.ok(svcSwitch, 'temporal layer test requires manual SVC controls');
      // The desired FPS only selects a temporal layer inside the spatial layer the
      // subscriber asked for: the server reads the frame-rate table of that spatial
      // layer, and an unpublished or never-measured one leaves the request without a
      // ceiling. Quality 1 is the layer both publishers reliably report.
      await Promise.all(pages.map(page => page.evaluate(() => interop.videoQuality(1))));
      await sleep(1500);
      evidence.temporalQuality = 1;
      // The measurement is the subscriber's inbound video frame counter over equal
      // windows, with an unrestricted window first as the budget. Neither the publisher's
      // framesEncoded (one frame per spatial layer of a picture) nor rendered-frame
      // callbacks (display cadence) measure what the server forwarded.
      const counters = () => Promise.all(pages.map(page => page.evaluate(async () => {
        const outbound = [], inbound = [];
        for (const pc of new Set(window.interopPeerConnections || [])) {
          if (pc.connectionState === 'closed') continue;
          const stats = await pc.getStats();
          for (const stat of stats.values()) {
            if (stat.kind !== 'video') continue;
            if (stat.type === 'outbound-rtp' && stat.frameWidth) {
              outbound.push([stats.get(stat.codecId)?.mimeType, stat.frameWidth, stat.framesEncoded]);
            } else if (stat.type === 'inbound-rtp') {
              inbound.push([stat.frameWidth, stat.framesDecoded ?? stat.framesReceived ?? 0, stat.rid ?? null]);
            }
          }
        }
        return { rendered: interop.report().videoFrames, outbound, inbound };
      })));
      const grown = (field, index, before, after) => {
        assert.equal(after[field].length, before[field].length, `${field} row set changed between samples`);
        return after[field].reduce((total, row, slot) => total + row[index] - before[field][slot][index], 0);
      };
      for (const fps of [0, 2, 15]) {
        await Promise.all(pages.map(page => page.evaluate(fps => interop.videoFPS(fps), fps)));
        await sleep(2500);
        const before = await counters();
        await sleep(6000);
        const after = await counters();
        const rendered = after.map((value, i) => value.rendered - before[i].rendered);
        const forwarded = after.map((value, i) => grown('inbound', 1, before[i], value));
        const encoded = after.map((value, i) => grown('outbound', 2, before[i], value));
        assert.ok(forwarded.every(value => value > 0), `forwarding stalled at ${fps} FPS request: ${forwarded}`);
        assert.ok(encoded.every(value => value > 0), `publisher stalled during the ${fps} FPS window: ${encoded}`);
        temporalResults.push({
          fps, intervalMs: 6000, rendered, forwarded, encoded,
          inbound: after.map(value => value.inbound),
          encodedEntries: after.map((value, i) => value.outbound.map((entry, slot) =>
            [...entry, entry[2] - before[i].outbound[slot][2]])),
        });
      }
      evidence.temporalResults = temporalResults;
      const [unrestricted, low, high] = temporalResults;
      for (let i = 0; i < pages.length; i++) {
        assert.ok(low.forwarded[i] < unrestricted.forwarded[i] * 0.6,
          `${low.fps} FPS request forwarded ${low.forwarded[i]} frames, not below 60% of the ${unrestricted.forwarded[i]} forwarded with no request, so no temporal layer was dropped`);
        assert.ok(high.forwarded[i] > low.forwarded[i] * 1.5,
          `raising the request from ${low.fps} to ${high.fps} FPS forwarded ${high.forwarded[i]} frames against ${low.forwarded[i]}, so the request did not select a higher temporal layer`);
        assert.ok(high.forwarded[i] >= unrestricted.forwarded[i] * 0.7,
          `${high.fps} FPS request forwarded ${high.forwarded[i]} frames, below 70% of the ${unrestricted.forwarded[i]} forwarded with no request`);
      }
    }
    const extendedResults = {};
    if (extended) {
      const admin = new AccessToken(apiKey, apiSecret, { ttl: '10m' });
      admin.addGrant({ roomAdmin: true, room: currentRoom });
      const response = await fetch(url.replace(/^ws/, 'http') + '/twirp/livekit.RoomService/PerformRpc', {
        method: 'POST', headers: { authorization: 'Bearer ' + await admin.toJwt(), 'content-type': 'application/json' },
        body: JSON.stringify({ room: currentRoom, destination_identity: 'client-1', method: 'interop.echo', payload: 'server', response_timeout_ms: 10000 })
      });
      const result = await response.json();
      assert.equal(response.status, 200, JSON.stringify(result));
      assert.equal(result.payload, 'echo:server'); extendedResults.rpc = 'PASS';
      await api.updateParticipant(currentRoom, 'client-0', { metadata: 'interop-updated', attributes: { stage: 'verified' } });
      await pages[1].waitForFunction(() => interop.metadata().some(p => p.identity === 'client-0' && p.metadata === 'interop-updated' && p.attributes.stage === 'verified'));
      extendedResults.metadata = 'PASS';
      const before = await pages[0].evaluate(() => interop.report().events.length);
      await pages[0].evaluate(() => interop.reconnect());
      await pages[0].waitForFunction(before => interop.report().events.slice(before).some(e => e[0] === 'state' && e[1] === 'connected'), before, { timeout: 30000 });
      await pages[1].evaluate(() => interop.data({ count: 10, topic: 'after-resume' }));
      await pages[0].waitForFunction(() => interop.report().received.filter(x => x.topic === 'after-resume').length === 10, null, { timeout: 10000 });
      assert.deepEqual((await pages[0].evaluate(() => interop.report().received)).filter(x => x.topic === 'after-resume').map(x => x.seq), Array.from({ length: 10 }, (_, i) => i));
      extendedResults.resume = 'PASS';
      const subscribed = await pages[1].evaluate(() => interop.report().events.filter(e => e[0] === 'subscribed' && e[1] === 'audio').length);
      await pages[0].evaluate(() => interop.republish());
      await pages[1].waitForFunction(before => interop.report().events.filter(e => e[0] === 'subscribed' && e[1] === 'audio').length > before, subscribed, { timeout: 20000 });
      extendedResults.republish = 'PASS';
      const publisher = await api.getParticipant(currentRoom, 'client-0');
      const audioTrack = publisher.tracks.find(track => track.type === 0);
      assert.ok(audioTrack, 'republished audio track missing');
      await api.mutePublishedTrack(currentRoom, 'client-0', audioTrack.sid, true);
      await pages[1].waitForFunction(sid => interop.remoteTracks().some(t => t.sid === sid && t.muted), audioTrack.sid);
      await assert.rejects(api.mutePublishedTrack(currentRoom, 'client-0', audioTrack.sid, false));
      await pages[0].evaluate(() => interop.unmute());
      await pages[1].waitForFunction(sid => interop.remoteTracks().some(t => t.sid === sid && !t.muted), audioTrack.sid);
      extendedResults.muteAndClientUnmute = 'PASS';
      const progress = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      await sleep(2000);
      const after = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      for (let i = 0; i < 2; i++) {
        assert.ok(after[i].audibleSamples >= progress[i].audibleSamples + 5, 'audio stopped after extended operations');
        if (video) assert.ok(after[i].videoFrames >= progress[i].videoFrames + 2, 'video stopped after extended operations');
      }
      extendedResults.mediaContinuity = 'PASS';
    }
    let videoHintsEchoed = 0;
    // VP9 official answers do not echo every browser bitrate hint.
    if (video && videoCodec === 'vp8') {
      for (const signal of signals.filter(s => s.direction === 'client-to-server' && s.message?.offer)) {
        const offer = signal.message.offer;
        const hints = videoBitrateHints(offer.sdp);
        if (!hints.size) continue;
        const answer = signals.find(s => s.client === signal.client && s.direction === 'server-to-client' && s.message?.answer?.id === offer.id)?.message.answer;
        assert.ok(answer, 'missing video answer');
        const echoed = videoBitrateHints(answer.sdp);
        for (const [key, value] of hints) {
          assert.equal(echoed.get(key), value, `video answer dropped bitrate hint ${key}`);
          videoHintsEchoed++;
        }
      }
      assert.ok(videoCodec !== 'vp8' || videoHintsEchoed >= 2, 'video scenario did not exercise both publisher bitrate hints');
    }
    if (secureSignaling) {
      assert.ok(profiles.length >= 2 && profiles.every(p => p.transport.protocol === 'wss:'), 'signaling did not use native WSS');
    }
    let av1DependencyDescriptorNegotiated = false;
    if (videoCodec === 'av1' && scalabilityMode) {
      const dependencyDescriptorUri = 'https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension';
      for (let client = 0; client < 2; client++) {
        assert.ok(signals.some(s => s.client === client && s.direction === 'client-to-server' && s.message?.offer?.sdp?.includes(dependencyDescriptorUri)), 'AV1 offer omitted dependency descriptor');
        assert.ok(signals.some(s => s.client === client && s.direction === 'server-to-client' && s.message?.answer?.sdp?.includes(dependencyDescriptorUri)), 'AV1 answer omitted dependency descriptor');
      }
      av1DependencyDescriptorNegotiated = true;
    }
    const negotiationErrors = await Promise.all(pages.map(page => page.evaluate(() => window.interopNegotiationErrors)));
    assert.deepEqual(negotiationErrors, [[], []], 'browser rejected SDP during the scenario');
    evidence.runs.push({ negotiationErrors, av1DependencyDescriptorNegotiated, videoHintsEchoed, networkResults, svcResults, temporalResults, room: currentRoom, connections, reports, profiles, signals, extendedResults, clusterResults, status: 'PASS' });
    if (proxyClassTrace) evidence.proxyClassTrace.runs.push(proxyClassTrace.endRun());
    await Promise.all(pages.map(page => page.evaluate(() => interop.close())));
    if (clusterMediaProbe) {
      const closed = await Promise.all(pages.map(page => page.evaluate(() => ({ peers: (window.interopPeerConnections || []).map(pc => pc.connectionState), report: interop.report() }))));
      for (const row of closed) { assert.ok(row.peers.length > 0 && row.peers.every(state => state === 'closed'), 'SDK peer connection leak'); assert.equal(row.report.audioContext, 'closed'); assert.equal(row.report.activeSamplingTimers, 0); }
      const apis = peerUrls.map(entry => new RoomServiceClient(entry.replace(/^ws/, 'http'), apiKey, apiSecret));
      const deadline = Date.now() + 10000;let remaining;
      do { remaining = await Promise.all(apis.map(peerApi => peerApi.listParticipants(currentRoom))); if (remaining.every(rows => rows.length === 0)) break; await sleep(100); } while (Date.now() < deadline);
      assert.ok(remaining.every(rows => rows.length === 0), 'presence not released through both entries');
      clusterResults.resources = { closedPeerConnections: closed.map(row => row.peers.length), audioContextsClosed: true, samplingTimersReleased: true, bothEntriesEmpty: true };
    }
    await Promise.all(contexts.map(c => c.close()));
    if (networkProxy) { await networkProxy.resetClients(); networkProxy.setBandwidth(OPEN_BOTTLENECK_BPS); }
    try {
      await api.deleteRoom(currentRoom);
      if (clusterMediaProbe) clusterResults.resources.finalDelete = { status: 200 };
    } catch (error) {
      // Fixed Go roomservice.go:143-148 returns not_found when the empty room
      // naturally retires before this cleanup request. Browser/context teardown
      // can exceed departure_timeout; verify actual absence through BOTH entries
      // instead of interpreting that successful natural cleanup as a media fail.
      if (!clusterMediaProbe || error.status !== 404 || error.code !== 'not_found') throw error;
      const apis = peerUrls.map(entry => new RoomServiceClient(entry.replace(/^ws/, 'http'), apiKey, apiSecret));
      const inventories = await Promise.all(apis.map(peerApi => peerApi.listRooms([currentRoom])));
      assert.ok(inventories.every(rooms => !rooms.some(room => room.name === currentRoom)), 'delete not_found cannot conceal a still-present room');
      clusterResults.resources.finalDelete = { status: 404, code: 'not_found', bothEntriesAlreadyRetired: true };
    }
    assert.ok(!(await api.listRooms([currentRoom])).some(room => room.name === currentRoom));
    currentRoom = undefined;
    console.log(`PASS ${run + 1}/${runs}: ${single ? 'single' : 'dual'} PC, audio/data${video ? '/video' : ''}`);
  }
  delete evidence.pendingReports;
  delete evidence.pendingNetworkResults;
  evidence.status = 'PASS';
  if (bandwidthAccuracy) evidence.bandwidthAccuracy.status = accuracyVerdict(evidence.bandwidthAccuracy.runs);
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = String(error.stack || error);
  delete evidence.pendingNetworkResults;
  if (proxyClassTrace?.run !== undefined) evidence.proxyClassTrace.runs.push(proxyClassTrace.endRun());
  if (bandwidthAccuracy && ['RUNNING', 'PASS', 'PASS_PER_PATH'].includes(evidence.bandwidthAccuracy.status)) evidence.bandwidthAccuracy.status = 'INCOMPLETE';
  evidence.failedSignals = activeSignals; evidence.failedProfiles = activeProfiles;
  if (networkProxy) evidence.failedNetwork = networkProxy.stats();
  evidence.failedMetrics = await metricsSnapshot().catch(error => ({ error: String(error) }));
  evidence.diagnostics = normalizer()(await Promise.all(activePages.map(page => page.evaluate(async () => ({
    ...interop.report(), negotiationErrors: window.interopNegotiationErrors, stats: await interop.stats(), peers: await Promise.all((window.interopPeerConnections || []).map(async pc => ({
      connection: pc.connectionState, ice: pc.iceConnectionState, signaling: pc.signalingState,
      local: pc.localDescription, remote: pc.remoteDescription, stats: [...(await pc.getStats()).values()]
      , beforeClose: pc.interopSnapshot
    })))
  })).catch(e => ({ error: String(e) })))));
  console.error(error); process.exitCode = 1;
} finally {
  clearInterval(networkMonitor);
  await networkMonitoring;
  proxyClassTrace?.close();
  if (currentRoom) await api.deleteRoom(currentRoom).catch(() => {});
  evidence.finishedAt = new Date().toISOString();
  try {
    await writeFile(join(output, `${label}-${single ? 'single' : 'dual'}${video ? '-video' + (videoCodec === 'vp8' ? '' : '-' + videoCodec) : ''}${relay ? '-relay' : ''}${extended ? '-extended' : ''}.json`), JSON.stringify(evidence, null, 2));
  } finally {
    host.closeAllConnections();
    await Promise.allSettled([browser?.close(), networkProxy?.close(), new Promise(resolve => host.close(resolve))]);
  }
}
