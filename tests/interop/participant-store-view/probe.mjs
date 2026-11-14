import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/** Launched-MAIN cached-vs-live participant video-size differential probe (SPEC-0077 C15).
 *
 * Closes API:RoomService.GetListParticipants.CachedVsLiveVideoSize. Drives the public
 * livekit.RoomService ListParticipants/GetParticipant surface of ONE launched MAIN
 * (the paired launcher runs the locked official Go v1.13.10 oracle and the Rust
 * candidate separately, on identical config bytes) under BOTH settings of
 * api.enable_psrpc_for_get_list_participants:
 *
 *   cached     (flag=false): reads the owner OSS participant-store snapshot. Upstream
 *              MediaTrackReceiver.UpdateVideoSize (mediatrackreceiver.go:979) updates the
 *              live receiver TrackInfo + telemetry but does NOT dirty/broadcast/store the
 *              ParticipantInfo, so a private source video-size change must NOT refresh the
 *              cached API view; only a real participant change (metadata update) refreshes it.
 *   live-psrpc (flag=true):  roomservice.go:178 routes ListParticipants/GetParticipant through
 *              PSRPC to the room owner, whose live snapshot reflects the current receiver size.
 *
 * A real native WHIP VP8 publisher (canvas captureStream 640x360 -> resize 960x540) and an
 * unchanged official browser SDK observer witness the actual decoded size change. No SDK code
 * is modified. Each side must independently satisfy the structural contract below; the launcher
 * then requires the two normalized contracts to be deeply equal. Equal-but-wrong does not pass.
 *
 * The cached snapshot's ABSOLUTE initial dimensions are timing-dependent (the store snapshot is
 * captured at the last participant-change boundary, which may precede the first RTP), so they are
 * mapped to a placeholder for cross-side comparison exactly like C14's client-random ssrc; the
 * real invariants (cached view stays stale across the private resize, is NOT the resized value,
 * and refreshes to the live 960x540 on a genuine participant change) are asserted on the RAW
 * values per side, so a genuine parity gap cannot hide behind normalization.
 */
const env = process.env;
const require = createRequire(resolve(env.INTEROP_DIR, 'package.json'));
const { chromium } = require('playwright');
const { AccessToken } = require('livekit-server-sdk');
const { closeOwnedBrowser, observeChildStderr } = await import(pathToFileURL(resolve(env.INTEROP_DIR, 'owned-browser-exit.mjs')));
const sdkPath = require.resolve('livekit-client');
const sdk = await readFile(sdkPath);
const sdkSha = createHash('sha256').update(sdk).digest('hex');
const sdkPackage = JSON.parse(await readFile(resolve(env.INTEROP_DIR, 'node_modules/livekit-client/package.json'), 'utf8'));
assert.equal(sdkPackage.version, '2.23.0');
const httpUrl = env.RTC_URL.replace(/\/$/, '');
const wsUrl = httpUrl.replace(/^http/, 'ws');
const key = env.LIVEKIT_API_KEY, secret = env.LIVEKIT_API_SECRET;
const side = env.SIDE || 'unknown';
const mode = env.API_PARTICIPANT_VIEW;
assert.ok(['cached', 'live-psrpc'].includes(mode), 'explicit API_PARTICIPANT_VIEW=cached|live-psrpc required');
const redact = s => String(s).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<fixture-token>');
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, msg, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await fn()) return; await sleep(100); }
  throw new Error(msg);
}

const SID = /^(RM_|PA_|TR_|ND_|EG_|AD_)[A-Za-z0-9_-]+$/;
const TS_KEYS = new Set(['creation_time', 'creation_time_ms', 'joined_at', 'joined_at_ms', 'started_at', 'ended_at', 'updated_at', 'last_seen', 'created_at', 'deleted_at', 'unix_micro']);
const SECRET_KEYS = new Set(['turn_password']);
const ID_KEYS = new Set(['cid']);
const SSRC_KEYS = new Set(['ssrc', 'repair_ssrc']);
function norm(v) {
  if (Array.isArray(v)) return v.map(norm);
  if (v && typeof v === 'object') {
    const o = {};
    for (const [k, val] of Object.entries(v)) {
      if (TS_KEYS.has(k)) o[k] = '<TS>';
      else if (SECRET_KEYS.has(k)) o[k] = '<SECRET>';
      else if (ID_KEYS.has(k)) o[k] = '<CID>';
      else if (SSRC_KEYS.has(k)) o[k] = '<SSRC>';
      else if (k === 'version' && typeof val === 'number') o[k] = '<VERSION>';
      else o[k] = norm(val);
    }
    if (Array.isArray(o.tracks)) o.tracks.sort(byMid);
    if (Array.isArray(o.data_tracks)) o.data_tracks.sort(byMid);
    if (Array.isArray(o.codecs)) o.codecs.sort(byMid);
    if (Array.isArray(o.layers)) o.layers.sort(bySpatial);
    return o;
  }
  if (typeof v === 'string' && SID.test(v)) return '<SID>';
  return v;
}
const byIdentity = (a, b) => String(a.identity).localeCompare(String(b.identity));
const byMid = (a, b) => (Number(a.mid) - Number(b.mid))
  || String(a.type).localeCompare(String(b.type)) || String(a.name).localeCompare(String(b.name));
const bySpatial = (a, b) => (Number(a.spatial_layer) - Number(b.spatial_layer))
  || String(a.quality).localeCompare(String(b.quality));

// Video-size metadata kept for exact comparison; dimensions are the contract signal.
// Field names are the protojson snake_case actually emitted by the Twirp envelope
// (mime_type / video_layer_mode), so codec identity is compared, not silently dropped.
const metadata = t => ({
  width: t.width, height: t.height, mime_type: t.mime_type,
  layers: (t.layers || []).map(l => ({ quality: l.quality, width: l.width, height: l.height })),
  codecs: (t.codecs || []).map(c => ({ mime_type: c.mime_type, video_layer_mode: c.video_layer_mode, layers: (c.layers || []).map(l => ({ quality: l.quality, width: l.width, height: l.height })) })),
});
// In cached mode the absolute initial/after-resize dimensions are timing-dependent; map them
// (and only them) to a placeholder. The staleness / not-resized / refresh-to-960 invariants are
// asserted on raw values per side below. Live mode and the post-participant-change refresh keep
// exact dimensions on both sides.
const cacheSize = m => ({ ...m, width: '<CACHED_SIZE>', height: '<CACHED_SIZE>',
  layers: m.layers.map(l => ({ ...l, width: '<CACHED_SIZE>', height: '<CACHED_SIZE>' })),
  codecs: m.codecs.map(c => ({ ...c, layers: c.layers.map(l => ({ ...l, width: '<CACHED_SIZE>', height: '<CACHED_SIZE>' })) })) });
const viewMeta = (m) => mode === 'cached' ? norm(cacheSize(m)) : norm(m);

const result = { status: 'RUNNING', side, mode, sdk_version: sdkPackage.version, sdk_sha256: sdkSha, contract: [], errors: [], cleanup_errors: [], diagnostics: {} };
const record = (caseName, obs) => result.contract.push({ case: caseName, ...obs });
async function twirp(method, body, token) {
  const res = await fetch(`${httpUrl}/twirp/livekit.RoomService/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify(body), signal: AbortSignal.timeout(15000),
  });
  const text = await res.text();
  let parsed; try { parsed = JSON.parse(text); } catch { parsed = { unparsed: text }; }
  return { status: res.status, body: parsed };
}
async function adminToken(room) {
  const t = new AccessToken(key, secret, { ttl: '10m' });
  t.addGrant({ room, roomAdmin: true, roomCreate: true, roomList: true });
  return t.toJwt();
}
async function peerToken(room, identity) {
  const t = new AccessToken(key, secret, { identity, ttl: '10m' });
  t.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  return t.toJwt();
}

const ROOM = 'c15-store-view', PUB = 'native-source', OBS = 'unchanged-sdk-observer';
const CHANGE = 'c15-genuine-participant-change';
const ownedRooms = new Set();
let host, browserServer, browser, context, finishStderr;
const pages = [];
const snap = p => p.evaluate(() => videoFixture.snapshot());

async function views(tok) {
  const lp = await twirp('ListParticipants', { room: ROOM }, tok);
  assert.equal(lp.status, 200);
  const list = lp.body.participants.find(p => p.identity === PUB);
  const get = await twirp('GetParticipant', { room: ROOM, identity: PUB }, tok);
  assert.equal(get.status, 200);
  assert.ok(list, 'List must include the actual WHIP publisher');
  assert.equal(list.sid, get.body.sid);
  assert.deepEqual(norm(list.tracks), norm(get.body.tracks));
  const video = get.body.tracks.find(t => t.type === 'VIDEO');
  assert.ok(video, 'publisher has no video track');
  return { participant: get.body, video, meta: metadata(video) };
}

try {
  const clientHtml = await readFile(resolve(env.CLIENT_HTML));
  host = createServer((req, res) => {
    res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'application/javascript' : 'text/html' });
    res.end(req.url === '/sdk.js' ? sdk : clientHtml);
  });
  await new Promise(done => host.listen(0, '127.0.0.1', done));
  browserServer = await chromium.launchServer({ headless: true, executablePath: env.OWNED_CHROMIUM,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--autoplay-policy=no-user-gesture-required', '--use-fake-ui-for-media-stream', '--use-fake-device-for-media-stream'] });
  finishStderr = observeChildStderr(browserServer.process());
  browser = await chromium.connect(browserServer.wsEndpoint());
  context = await browser.newContext();
  const assetUrl = 'http://127.0.0.1:' + host.address().port;
  const tok = await adminToken(ROOM);

  let r = await twirp('CreateRoom', { name: ROOM, empty_timeout: 120, departure_timeout: 60 }, tok); ownedRooms.add(ROOM);
  assert.equal(r.status, 200);

  async function newPage() {
    const page = await context.newPage(); pages.push(page);
    page.on('pageerror', e => result.errors.push('pageerror: ' + redact(e.message)));
    await page.goto(assetUrl);
    return page;
  }
  const observer = await newPage(), publisher = await newPage();
  await observer.evaluate(a => videoFixture.observe(a), { url: wsUrl, token: await peerToken(ROOM, OBS), identity: PUB });
  const jwt = await peerToken(ROOM, PUB);
  await publisher.evaluate(a => videoFixture.publish(a), { url: wsUrl, token: jwt, codec: 'VP8' });

  // Actual initial decoded 640x360 + publication present in the API.
  await until(async () => {
    const s = await snap(observer);
    const lp = await twirp('ListParticipants', { room: ROOM }, tok);
    const p = lp.body.participants.find(p => p.identity === PUB);
    return s.frames >= 40 && s.lastWidth === 640 && s.lastHeight === 360 && p?.tracks.some(t => t.type === 'VIDEO');
  }, 'actual initial decoded 640x360 and publication absent');
  if (mode === 'live-psrpc') await until(async () => { const v = await views(tok); return v.meta.width === 640 && v.meta.height === 360; }, 'live API did not expose actual initial dimensions');
  const initial = await views(tok);
  const sid = initial.participant.sid, trackSid = initial.video.sid;
  result.diagnostics.initialRaw = { width: initial.meta.width, height: initial.meta.height };
  record('initial_view', { mode, video: viewMeta(initial.meta), listGetConsistent: true });

  // Private source resize on the SAME RTC track: observer must decode 960x540.
  const beforeFrames = (await snap(observer)).frames;
  await publisher.evaluate(() => videoFixture.resize());
  await until(async () => { const s = await snap(observer); return s.frames >= beforeFrames + 30 && s.lastWidth === 960 && s.lastHeight === 540; }, 'same real RTC track did not decode resized 960x540');
  if (mode === 'live-psrpc') await until(async () => { const v = await views(tok); return v.meta.width === 960 && v.meta.height === 540; }, 'live API did not expose private receiver size change');

  // Repeated reads are API observations, not a cache-refresh mechanism.
  let after;
  for (let i = 0; i < 20; i++) {
    after = await views(tok);
    if (mode === 'cached') {
      assert.deepEqual(after.meta, initial.meta, 'private native dimensions must not refresh the stored participant');
      assert.notEqual(after.meta.width, 960, 'cached view wrongly reflects the private resize');
    }
    await sleep(50);
  }
  assert.equal(after.participant.sid, sid); assert.equal(after.video.sid, trackSid);
  result.diagnostics.afterResizeRaw = { width: after.meta.width, height: after.meta.height };
  // Mode-explicit witness of the invariant actually asserted above: cached must stay stale and
  // NOT take the resized value; live must reflect the new receiver size. The field name states
  // what was verified so the persisted contract is not misleading across the two modes.
  const resizeWitness = mode === 'cached'
    ? { cachedStayedStale: JSON.stringify(after.meta) === JSON.stringify(initial.meta), cachedNotResized: after.meta.width !== 960 }
    : { liveReflectedResize: after.meta.width === 960 && after.meta.height === 540 && JSON.stringify(after.meta) !== JSON.stringify(initial.meta) };
  record('after_private_resize', { mode, video: viewMeta(after.meta), ...resizeWitness,
    sameParticipant: true, sameTrack: true });

  // A genuine participant change (metadata) refreshes the full stored TrackInfo snapshot.
  r = await twirp('UpdateParticipant', { room: ROOM, identity: PUB, metadata: CHANGE }, tok);
  assert.equal(r.status, 200);
  await until(async () => { const v = await views(tok); return v.participant.metadata === CHANGE && v.meta.width === 960 && v.meta.height === 540; }, 'real participant metadata callback did not refresh stored full TrackInfo');
  const refreshed = await views(tok);
  assert.equal(refreshed.participant.sid, sid); assert.equal(refreshed.video.sid, trackSid);
  record('after_participant_change', { mode, video: norm(refreshed.meta), metadata: refreshed.participant.metadata,
    refreshedToLiveSize: refreshed.meta.width === 960 && refreshed.meta.height === 540, sameParticipant: true, sameTrack: true });

  // Actual WHIP DELETE removes the publisher from both stored and live views.
  const delStatus = await publisher.evaluate(a => videoFixture.remove(a), { token: jwt });
  assert.equal(delStatus, 200);
  await until(async () => { const lp = await twirp('ListParticipants', { room: ROOM }, tok); return !lp.body.participants.some(p => p.identity === PUB); }, 'removed publisher retained in participant store');
  const getAfter = await twirp('GetParticipant', { room: ROOM, identity: PUB }, tok);
  assert.equal(getAfter.status, 404);
  record('remove_publisher', { whipDeleteStatus: delStatus, listAbsent: true, getRejects: getAfter.status === 404 });

  assert.ok(result.errors.length === 0, 'page errors: ' + result.errors.join('; '));
  result.match = true;
} catch (error) {
  result.errors.push(redact(error.stack || error.message).slice(0, 2000));
} finally {
  for (const page of pages) { try { await page.evaluate(() => videoFixture.close()); } catch { result.cleanup_errors.push('disconnect-owned-peer'); } }
  if (context) try { await context.close(); } catch (e) { result.cleanup_errors.push('context: ' + e.message); }
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
  const cleanupTok = await adminToken(ROOM);
  for (const room of ownedRooms) { try { await twirp('DeleteRoom', { room }, cleanupTok); } catch (e) { result.cleanup_errors.push('owned room ' + room + ': ' + redact(e.message)); } }
  assert.equal(createHash('sha256').update(await readFile(sdkPath)).digest('hex'), sdkSha);
  result.status = !result.errors.length && !result.cleanup_errors.length && result.match ? 'PASS' : 'FAIL';
  await writeFile(env.OUTPUT, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  process.exitCode = result.status === 'PASS' ? 0 : 1;
}