import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname, join, isAbsolute } from 'node:path';
import { chromium } from 'playwright';
import { AccessToken, RoomServiceClient } from 'livekit-server-sdk';
import { SignalRequest, SignalResponse, WrappedJoinRequest, JoinRequest } from '@livekit/protocol';
import { gunzipSync } from 'node:zlib';
import { normalizer } from './normalize.mjs';
import { TurnUdpProxy } from './network-proxy.mjs';

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
  return values;
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
    evidence.pendingNetworkResults = networkResults;
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
      networkProxy.setBandwidth(120000);
      if (bandwidthAccuracy) {
        const settledByUs = Math.ceil(performance.now() * 1000);
        // Conservative rate history: lower capacity takes effect no later than
        // completion; retain the old higher bound through this call interval.
        bandwidthRateTrace.push({ run, requestedAtUs: constraintRequestedUs, atUs: settledByUs, settledByUs, bpsPerClient: 120000 });
      }
      await sleep(18000);
      const constrained = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      const congestedStats = networkProxy.stats();
      const constrainedMetrics = await metricsSnapshot();
      assert.ok(congestedStats.droppedPackets > phaseStartStats.droppedPackets, 'bottleneck did not actually drop queued datagrams');
      for (let i = 0; i < 2; i++) {
        const paused = constrained[i].events.slice(before[i].events.length).some(e => e[0] === 'stream-state' && e[2] === 'paused');
        const lower = constrained[i].videoDimensions.slice(-8).some(f => f.width < (before[i].videoDimensions.at(-1)?.width || 0));
        assert.ok(paused || lower, `client ${i} had no SFU pause/layer reduction under congestion`);
      }
      networkResults.push({ phase: 'congested', metrics: constrainedMetrics, initialMetrics, proxy: congestedStats, reports: constrained, status: 'PASS' });
      networkPhase = 'recovering';
      const restoreRequestedUs = bandwidthAccuracy ? Math.floor(performance.now() * 1000) : undefined;
      networkProxy.setBandwidth(4000000);
      if (bandwidthAccuracy) bandwidthRateTrace.push({ run, requestedAtUs: restoreRequestedUs, atUs: restoreRequestedUs,
        settledByUs: Math.ceil(performance.now() * 1000), bpsPerClient: 4000000 });
      await Promise.all(pages.map((page, i) => page.waitForFunction(({ videoFrames, audioSamples, minWidth }) => {
        const report = interop.report();
        return report.videoFrames >= videoFrames + 20 && report.audibleSamples >= audioSamples + 5
          && report.videoDimensions.slice(-8).every(f => f.width >= minWidth);
      }, { videoFrames: constrained[i].videoFrames, audioSamples: constrained[i].audibleSamples,
        minWidth: Math.min(...before[i].videoDimensions.slice(-8).map(f => f.width)) }, { timeout: 30000 })));
      networkPhase = 'recovered-observation';
      const stableBefore = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      await sleep(5000);
      const stableAfter = await Promise.all(pages.map(page => page.evaluate(() => interop.report())));
      for (let i = 0; i < 2; i++) {
        assert.equal(stableAfter[i].connection, 'connected'); assert.equal(stableAfter[i].errors.length, 0);
        assert.ok(stableAfter[i].videoFrames >= stableBefore[i].videoFrames + 20, 'recovery did not sustain video');
        assert.ok(stableAfter[i].audibleSamples >= stableBefore[i].audibleSamples + 20, 'recovery did not sustain audio');
      }
      const recoveredMetrics = await metricsSnapshot();
      if (initialMetrics && constrainedMetrics && recoveredMetrics) {
        assert.ok(constrainedMetrics.livekit_estimated_capacity_bps < initialMetrics.livekit_estimated_capacity_bps, 'BWE did not fall');
        assert.ok(recoveredMetrics.livekit_estimated_capacity_bps > constrainedMetrics.livekit_estimated_capacity_bps, 'BWE did not recover');
      }
      networkResults.push({ phase: 'recovered', metrics: recoveredMetrics, proxy: networkProxy.stats(), reports: stableAfter, status: 'PASS' });
      if (bandwidthAccuracy) {
        await networkMonitoring;
        const result = evaluateBudgetTimeline({ samples: networkTimeline.filter(sample => sample.run === run),
          rateChanges: bandwidthRateTrace.filter(change => change.run === run),
          expectedClients: reports.reduce((count, report) => count + report.transports.length, 0),
          pathIdentityVerifiedAtUs: bandwidthPathsVerifiedAtUs });
        evidence.bandwidthAccuracy.runs.push({ run, ...result });
        evidence.bandwidthAccuracy.status = result.status;
        assert.equal(result.status, 'PASS', 'stable original budget gauge violated the allocation-horizon invariant, or required measurement metadata was missing');
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
      for (const fps of [5, 15]) {
        await Promise.all(pages.map(page => page.evaluate(fps => interop.videoFPS(fps), fps)));
        await sleep(2500);
        const before = await Promise.all(pages.map(page => page.evaluate(() => interop.report().videoFrames)));
        await sleep(6000);
        const after = await Promise.all(pages.map(page => page.evaluate(() => interop.report().videoFrames)));
        const decoded = after.map((value, i) => value - before[i]);
        assert.ok(decoded.every(value => value > 0), 'temporal layer switch stalled decoding');
        temporalResults.push({ fps, intervalMs: 6000, decodedFrames: decoded });
      }
      for (let i = 0; i < 2; i++) {
        assert.ok(temporalResults[1].decodedFrames[i] > temporalResults[0].decodedFrames[i] * 1.5, 'temporal request did not change actual decoded FPS');
        assert.ok(temporalResults[1].decodedFrames[i] >= 66, '15 FPS request did not recover at least 11 decoded FPS');
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
    if (networkProxy) { await networkProxy.resetClients(); networkProxy.setBandwidth(4000000); }
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
  if (bandwidthAccuracy) evidence.bandwidthAccuracy.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL'; evidence.error = String(error.stack || error);
  if (proxyClassTrace?.run !== undefined) evidence.proxyClassTrace.runs.push(proxyClassTrace.endRun());
  if (bandwidthAccuracy && ['RUNNING', 'PASS'].includes(evidence.bandwidthAccuracy.status)) evidence.bandwidthAccuracy.status = 'INCOMPLETE';
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
