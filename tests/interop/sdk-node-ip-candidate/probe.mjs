#!/usr/bin/env node
/**
 * SPEC-0077 C99 SDK lane probe.
 *
 * Drives the PINNED official `livekit-client` UMD build (served byte-for-byte from
 * `tests/interop/node_modules`, never patched) in headless Chromium against ONE
 * already-launched daemon, and writes the ICE/stats evidence the SDK actually
 * negotiated. The only page-level tap is the `RTCPeerConnection` constructor, which
 * records the object so `getStats()` and `remoteDescription` can be read; no SDK
 * method is replaced.
 *
 * Usage:
 *   node probe.mjs --server ws://127.0.0.1:PORT --out FILE \
 *        --key KEY --secret SECRET [--room R] [--identity I] [--budget-ms N]
 *        [--media audio|video]
 *
 * The URL scheme is part of the measured leg, not a detail. Measured on
 * `livekit-client@2.23.0`: the SDK builds its own join URL from the origin it is handed,
 * so `ws://127.0.0.1:P` and `http://127.0.0.1:P` BOTH end up as
 * `signal connecting to ws://127.0.0.1:P/rtc/v1` and both join successfully. An earlier
 * reading of this row — that an `http://` origin reaches the Twirp `Join` handler and is
 * answered `400 invalid Websocket upgrade request` — described the daemon's handler
 * surface, not what this client ever sends, and is refuted by the receipts (the http leg
 * shows peer_connection_count 1 and room_state connected). The scheme the SDK actually
 * dials is therefore read back out of its own console lines rather than inferred from the
 * argument. The page catches `connect()` rejection so a failed leg still yields the peer
 * connection, SDP candidates and stats that the row is about; exit is non-zero only when
 * the probe itself produced no result.
 *
 * `--media` selects which fake device is captured. The published kind and the getUserMedia
 * outcome are recorded per leg so a capture failure on a given host cannot be misread as an
 * ICE failure; the gate declares `gum_result_class` per side and leaves `media_kind`
 * undeclared, because either kind is legitimate input to this lane.
 */
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { AccessToken } from 'livekit-server-sdk';
import { chromium } from 'playwright';

const dir = path.dirname(fileURLToPath(import.meta.url));
const arg = (name, def) => {
  const i = process.argv.indexOf('--' + name);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : def;
};
const sha = (buf) => createHash('sha256').update(buf).digest('hex');

const server = arg('server', '');
const out = arg('out', '');
const apiKey = arg('key', '');
const apiSecret = arg('secret', '');
const roomName = arg('room', 'c99-sdk-node-ip');
const identity = arg('identity', 'c99-viewer');
const budgetMs = Number(arg('budget-ms', '60000'));
const mediaKind = arg('media', 'audio') === 'video' ? 'video' : 'audio';
const sdkPath = path.resolve(arg('sdk',
  path.join(dir, '..', 'node_modules', 'livekit-client', 'dist', 'livekit-client.umd.js')));
const pagePath = path.join(dir, 'page.html');

if (!server || !out || !apiKey || !apiSecret) {
  throw new Error('probe.mjs needs --server, --out, --key and --secret');
}

const sdkBytes = readFileSync(sdkPath);
const pageBytes = readFileSync(pagePath);
const fakeDeviceArgs = [
  '--use-fake-device-for-media-stream',
  '--use-fake-ui-for-media-stream',
  // Without these the fake capture path dies on this host with
  // "The AudioContext encountered an error from the audio device or the WebAudio
  // renderer", `setMicrophoneEnabled()` never resolves, and the leg looks like an ICE
  // failure while the daemon has already reported the participant active.
  '--mute-audio',
  '--autoplay-policy=no-user-gesture-required',
  '--disable-features=AudioServiceOutOfProcess,AudioServiceSandbox',
];

const at = new AccessToken(apiKey, apiSecret, { identity, ttl: '5m' });
// `livekit-server-sdk@2.19.2` does NOT derive the join permission from `room`: the
// server's `EnsureJoinPermission` (pkg/service/auth.go:172-183) reads
// `claims.Video.RoomJoin`, and `grants.d.ts` lists `roomJoin`/`roomCreate` as separate
// optional flags. A token without them is refused with `permissions denied` before any
// peer connection is negotiated, which reads exactly like a transport failure.
at.addGrant({
  room: roomName, roomJoin: true, roomCreate: true,
  canPublish: true, canSubscribe: true,
});
const token = await at.toJwt();

const files = {
  '/index.html': { body: pageBytes, type: 'text/html; charset=utf-8' },
  '/livekit-client.umd.js': { body: sdkBytes, type: 'text/javascript; charset=utf-8' },
};
const staticServer = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  const file = files[url.pathname];
  if (!file) {
    res.writeHead(404, { 'content-length': 0 });
    res.end();
    return;
  }
  res.writeHead(200, { 'content-type': file.type, 'content-length': file.body.length });
  res.end(file.body);
});
await new Promise((resolve) => staticServer.listen(0, '127.0.0.1', resolve));
const staticPort = staticServer.address().port;

const receipt = {
  server,
  room: roomName,
  identity,
  media_kind: mediaKind,
  token_sha256: sha(Buffer.from(token)),
  client: {
    package: 'livekit-client',
    version: JSON.parse(readFileSync(path.join(path.dirname(sdkPath), '..', 'package.json'))).version,
    path: sdkPath,
    sha256: sha(sdkBytes),
    served_byte_for_byte: true,
    patched: false,
  },
  page: { path: pagePath, sha256: sha(pageBytes) },
  server_sdk: { package: 'livekit-server-sdk',
                 version: JSON.parse(readFileSync(
                   path.join(path.dirname(sdkPath), '..', '..',
                     'livekit-server-sdk', 'package.json'))).version },
  browser: {},
  console_lines: [],
  page_errors: [],
  result: null,
  error: null,
  timed_out: false,
};

const persist = () => writeFileSync(out, JSON.stringify(receipt, null, 2) + '\n');
let browser = null;
try {
  browser = await chromium.launch({ args: fakeDeviceArgs });
  const context = await browser.newContext();
  // The microphone grant (not just the fake-device flags) is what makes
  // `getUserMedia()` resolve in this headless build. Autoplay is handled by the
  // `--autoplay-policy` launch argument: playwright 1.64.1 rejects an 'autoplay'
  // permission name outright, so it is never requested here.
  try {
    await context.grantPermissions(['microphone']);
  } catch (error) {
    receipt.permission_grant = 'failed: ' + String(error && error.message || error);
  }
  receipt.permission_grant = receipt.permission_grant || 'microphone';
  const page = await context.newPage();
  page.on('console', (message) => receipt.console_lines.push(message.type() + ': ' + message.text()));
  page.on('pageerror', (error) => receipt.page_errors.push(String(error && error.stack || error)));
  const url = `http://127.0.0.1:${staticPort}/index.html?server=`
    + encodeURIComponent(server) + '&token=' + encodeURIComponent(token)
    + '&media=' + mediaKind;
  await page.goto(url, { waitUntil: 'load', timeout: 30000 });
  receipt.browser = {
    name: browser.browserType().name(),
    version: browser.version(),
    executable: chromium.executablePath(),
    launch_args: fakeDeviceArgs,
    static_origin: `http://127.0.0.1:${staticPort}`,
  };
  const persist = () => writeFileSync(out, JSON.stringify(receipt, null, 2) + '\n');
  let timer = null;
  const budget = new Promise((resolve) => {
    timer = setTimeout(() => resolve('__timeout__'), budgetMs);
  });
  const run = page.evaluate(() => window.__run());
  run.catch(() => {});
  const value = await Promise.race([run, budget]);
  if (value === '__timeout__') {
    receipt.timed_out = true;
    receipt.error = `probe exceeded the ${budgetMs} ms budget`;
  } else {
    receipt.result = value;
  }
  if (timer) clearTimeout(timer);
  // The evidence is on disk before teardown: a headless renderer holding an open
  // peer connection can stall `context.close()` past the caller's process timeout,
  // and a lost receipt is worse than a lingering browser (the caller still SIGTERMs
  // the daemon and this pid).
  persist();
  const cap = (promise, ms) => Promise.race([
    promise, new Promise((resolve) => setTimeout(resolve, ms)),
  ]);
  await cap(context.close(), 8000);
  await cap(browser.close(), 8000);
  browser = null;
} catch (error) {
  receipt.error = String(error && error.stack || error);
  if (browser) {
    try {
      await cap(browser.close(), 8000);
    } catch {
      /* the browser is already gone; the receipt still carries the failure */
    }
    browser = null;
  }
  persist();
}
process.exit(receipt.result === null || receipt.timed_out ? 1 : 0);
