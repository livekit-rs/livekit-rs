import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/** End-to-end force_tcp candidate witness.
 *
 * Connects the locked official browser SDK to the launched MAIN server, publishes
 * a synthetic oscillator audio track (headless-safe, no microphone), then reads the
 * *selected* ICE candidate pair from RTCPeerConnection.getStats(). The selected
 * local candidate `protocol` (udp/tcp) and the remote candidate `port` are the
 * actual negotiated media transport — not a parse of offered candidates. Under
 * rtc.force_tcp the server binds no UDP media socket, so the only selectable
 * server candidate is passive ICE-TCP on rtc.tcp_port; with force_tcp off and no
 * tcp_port the only server candidate is UDP on rtc.udp_port. EXPECT_PROTOCOL and
 * EXPECT_REMOTE_PORT are supplied by the paired launcher per vector.
 */
const env = process.env;
const require = createRequire(resolve(env.INTEROP_DIR, 'package.json'));
const { chromium } = require('playwright');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const { closeOwnedBrowser, observeChildStderr } = await import(pathToFileURL(resolve(env.INTEROP_DIR, 'owned-browser-exit.mjs')));
const sdkPath = require.resolve('livekit-client');
const sdk = await readFile(sdkPath);
const sdkSha = createHash('sha256').update(sdk).digest('hex');
const sdkPackage = JSON.parse(await readFile(resolve(env.INTEROP_DIR, 'node_modules/livekit-client/package.json'), 'utf8'));
assert.equal(sdkPackage.version, '2.23.0');
const expectProtocol = env.EXPECT_PROTOCOL;
const expectRemotePort = Number(env.EXPECT_REMOTE_PORT);
assert.ok(expectProtocol === 'tcp' || expectProtocol === 'udp', 'EXPECT_PROTOCOL must be tcp or udp');
assert.ok(Number.isInteger(expectRemotePort) && expectRemotePort > 0, 'EXPECT_REMOTE_PORT must be a positive integer');
const api = new RoomServiceClient(env.RTC_URL, env.LIVEKIT_API_KEY, env.LIVEKIT_API_SECRET);
const result = { status: 'RUNNING', sdk_version: sdkPackage.version, sdk_sha256: sdkSha,
  expect: { protocol: expectProtocol, remote_port: expectRemotePort },
  transports: [], errors: [], cleanup_errors: [] };
const redact = str => String(str).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<fixture-token>');
const room = 'owned-force-tcp-' + createHash('sha256').update(env.RTC_URL + expectProtocol).digest('hex').slice(0, 10);
const identity = 'owned-force-tcp-probe';
const html = '<!doctype html><meta charset="utf-8"><script src="/sdk.js"></script>';

let host, browserServer, browser, context, finishStderr, created = false;
try {
  host = createServer((req, res) => {
    res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'application/javascript' : 'text/html' });
    res.end(req.url === '/sdk.js' ? sdk : html);
  });
  await new Promise(done => host.listen(0, '127.0.0.1', done));
  browserServer = await chromium.launchServer({ headless: true, executablePath: env.OWNED_CHROMIUM,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--use-fake-ui-for-media-stream', '--use-fake-device-for-media-stream'] });
  finishStderr = observeChildStderr(browserServer.process());
  browser = await chromium.connect(browserServer.wsEndpoint());
  context = await browser.newContext();
  // Record every RTCPeerConnection the SDK creates so getStats can be read after
  // negotiation. Installed before page scripts so the SDK uses the subclass.
  await context.addInitScript(() => {
    window.__pcs = [];
    const Native = window.RTCPeerConnection;
    window.RTCPeerConnection = class extends Native {
      constructor(...args) { super(...args); window.__pcs.push(this); }
    };
  });
  await api.createRoom({ name: room, emptyTimeout: 90, departureTimeout: 90 });
  created = true;
  const grant = new AccessToken(env.LIVEKIT_API_KEY, env.LIVEKIT_API_SECRET, { identity });
  grant.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  const jwt = await grant.toJwt();
  const page = await context.newPage();
  page.on('pageerror', e => result.errors.push('pageerror: ' + redact(e.message)));
  await page.goto('http://127.0.0.1:' + host.address().port);
  const observed = await page.evaluate(async ([url, jwt]) => {
    const ac = new AudioContext();
    await ac.resume();
    const rtcRoom = new LivekitClient.Room({ singlePeerConnection: true, dynacast: true });
    let connectError = null;
    try {
      await rtcRoom.connect(url, jwt, { maxRetries: 0, websocketTimeout: 15000 });
    } catch (e) { connectError = String(e && e.message || e); }
    let published = false;
    if (!connectError) {
      const osc = ac.createOscillator(); osc.frequency.value = 440;
      const gain = ac.createGain(); gain.gain.value = 0.2;
      const dest = ac.createMediaStreamDestination();
      osc.connect(gain); gain.connect(dest); osc.start();
      const track = dest.stream.getAudioTracks()[0];
      try {
        await rtcRoom.localParticipant.publishTrack(track, { source: LivekitClient.Track.Source.Microphone, name: 'force-tcp-tone' });
        published = true;
      } catch (e) { connectError = 'publish: ' + String(e && e.message || e); }
    }
    // Poll the selected candidate pair until the media transport is connected.
    const deadline = Date.now() + 25000;
    let transports = [];
    while (Date.now() < deadline) {
      transports = [];
      for (const pc of window.__pcs) {
        if (pc.connectionState === 'closed') continue;
        const stats = await pc.getStats();
        for (const s of stats.values()) {
          if (s.type !== 'transport' || !s.selectedCandidatePairId) continue;
          const pair = stats.get(s.selectedCandidatePairId);
          if (!pair) continue;
          const local = stats.get(pair.localCandidateId);
          const remote = stats.get(pair.remoteCandidateId);
          transports.push({ dtlsState: s.dtlsState, protocol: local?.protocol ?? null,
            localCandidateType: local?.candidateType ?? null, remoteCandidateType: remote?.candidateType ?? null,
            remoteAddress: remote?.address ?? null, remotePort: remote?.port ?? null,
            bytesSent: pair.bytesSent, bytesReceived: pair.bytesReceived });
        }
      }
      if (transports.length && transports.every(t => t.dtlsState === 'connected' && t.protocol)) break;
      await new Promise(r => setTimeout(r, 250));
    }
    const sid = rtcRoom.localParticipant.sid;
    const state = rtcRoom.state;
    try { await rtcRoom.disconnect(); } catch {}
    try { await ac.close(); } catch {}
    return { connectError, published, sid, state, transports };
  }, [env.RTC_URL.replace(/^http/, 'ws'), jwt]);
  result.connect_error = observed.connectError ? redact(observed.connectError) : null;
  result.published = observed.published;
  result.participant_sid = observed.sid;
  result.room_state = observed.state;
  result.transports = observed.transports;
  assert.equal(observed.connectError, null, 'SDK connect failed: ' + redact(observed.connectError || ''));
  assert.equal(observed.state, 'connected', 'room not connected');
  assert.match(observed.sid, /^PA_/);
  assert.ok(observed.published, 'audio track not published');
  assert.ok(observed.transports.length >= 1, 'no selected ICE candidate pair observed');
  for (const t of observed.transports) {
    assert.equal(t.dtlsState, 'connected', 'media transport DTLS not connected');
    assert.equal(t.protocol, expectProtocol, `selected protocol ${t.protocol} != expected ${expectProtocol}`);
    assert.equal(t.remotePort, expectRemotePort, `selected remote port ${t.remotePort} != expected ${expectRemotePort}`);
    assert.equal(t.remoteCandidateType, 'host', 'server media candidate should be a host candidate');
  }
  result.match = true;
} catch (error) {
  result.errors.push(redact(error.stack || error.message).slice(0, 1500));
} finally {
  if (context) try { await context.close(); } catch (error) { result.cleanup_errors.push('context: ' + error.message); }
  if (browserServer) {
    result.browser = await closeOwnedBrowser(browser, browserServer);
    if (result.browser.exit?.code !== 0 || result.browser.contextsAfter !== 0) result.cleanup_errors.push('missing real browser child exit or live context');
    if (result.browser.failures.length) result.cleanup_errors.push(...result.browser.failures);
    if (finishStderr) {
      const stderr = finishStderr();
      result.browser_stderr = stderr.metadata;
      await writeFile(resolve(env.OUTPUT, '..', 'browser.stderr'), stderr.bytes);
    }
  }
  if (host) await new Promise(done => host.close(done));
  if (created) try { await api.deleteRoom(room); } catch (error) { result.cleanup_errors.push('owned room: ' + redact(error.message)); }
  assert.equal(createHash('sha256').update(await readFile(sdkPath)).digest('hex'), sdkSha);
  result.status = !result.errors.length && !result.cleanup_errors.length && result.match ? 'PASS' : 'FAIL';
  await writeFile(env.OUTPUT, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  process.exitCode = result.status === 'PASS' ? 0 : 1;
}