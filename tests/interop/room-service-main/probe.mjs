import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/** Launched-MAIN RoomService API differential probe (SPEC-0077 C14).
 *
 * Drives the public livekit.RoomService Twirp surface of ONE launched MAIN server
 * (the paired launcher runs the locked official Go v1.13.10 oracle and the Rust
 * candidate separately, on identical config bytes). Control-plane semantics and
 * error contracts are captured as the exact decoded HTTP envelope; participant-
 * dependent positive side-effects are witnessed through two real unchanged
 * official browser SDK peers (a publisher of 440 Hz audio + 640x360 VP8 video and
 * a subscriber). No SDK code is modified; the fixture only reads existing handles
 * and events. Nondeterministic resource IDs / timestamps are mapped, list order is
 * canonicalised by name/identity, and every semantic field (codes, messages,
 * metadata, permissions, mute state, disconnect reasons, topics, payloads) is kept
 * for exact Go-vs-Rust comparison. Each side must independently satisfy the
 * structural contract below; equal-but-wrong does not pass.
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
const redact = s => String(s).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<fixture-token>');
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, msg, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await fn()) return; await sleep(100); }
  throw new Error(msg);
}

const SID = /^(RM_|PA_|TR_|ND_|EG_|AD_)[A-Za-z0-9_-]+$/;
// Nondeterministic per-session values mapped to placeholders: wall-clock
// timestamps (protojson renders int64 as strings, so map regardless of type),
// the random per-room TURN secret, the client-chosen track `cid` UUID, the
// client-chosen RTP `ssrc`, and the coalesced participant `version` magnitude.
// The SSRC *presence* and version *monotonicity* contracts are asserted on the
// raw values below, so a real gap (e.g. ssrc=0) cannot hide behind normalization.
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
    // Canonicalise nondeterministic list ORDER only. Upstream serialises
    // ParticipantInfo.tracks by ranging the publishedTracks map
    // (uptrackmanager.go:50/109), so order varies per call even Go-vs-Go;
    // layers/codecs are keyed likewise. Every semantic field is preserved and
    // still compared — only the order is canonicalised on stable SDP identity
    // (mid / spatial_layer), so a real mid- or layer-assignment gap still fails.
    if (Array.isArray(o.tracks)) o.tracks.sort(byMid);
    if (Array.isArray(o.data_tracks)) o.data_tracks.sort(byMid);
    if (Array.isArray(o.codecs)) o.codecs.sort(byMid);
    if (Array.isArray(o.layers)) o.layers.sort(bySpatial);
    return o;
  }
  if (typeof v === 'string' && SID.test(v)) return '<SID>';
  return v;
}
const byName = (a, b) => String(a.name).localeCompare(String(b.name));
const byIdentity = (a, b) => String(a.identity).localeCompare(String(b.identity));
const byMid = (a, b) => (Number(a.mid) - Number(b.mid))
  || String(a.type).localeCompare(String(b.type)) || String(a.name).localeCompare(String(b.name));
const bySpatial = (a, b) => (Number(a.spatial_layer) - Number(b.spatial_layer))
  || String(a.quality).localeCompare(String(b.quality));

const result = { status: 'RUNNING', side, sdk_version: sdkPackage.version, sdk_sha256: sdkSha, contract: [], errors: [], cleanup_errors: [] };
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
async function adminToken(room, destinationRoom) {
  const t = new AccessToken(key, secret, { ttl: '10m' });
  const grant = { room, roomAdmin: true, roomCreate: true, roomList: true };
  if (destinationRoom) grant.destinationRoom = destinationRoom;
  t.addGrant(grant);
  return t.toJwt();
}

const CP_A = 'rs-cp-a', CP_B = 'rs-cp-b', MEDIA = 'rs-media';
const PUB = 'publisher', SUB = 'subscriber';
const ownedRooms = new Set();
let host, browserServer, browser, context, finishStderr;
const pages = [];
const snapshot = p => p.evaluate(() => fixture.snapshot());
const evCount = (s, event, sid) => s.events.filter(e => e.event === event && (sid === undefined || e.sid === sid)).length;

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

  // ---- Control plane: room CRUD, metadata, participant error contracts, Forward/Move/RPC validation ----
  const tokA = await adminToken(CP_A), tokB = await adminToken(CP_B);
  let r = await twirp('ListRooms', {}, tokA);
  assert.equal(r.status, 200); assert.deepEqual(r.body.rooms, []);
  record('list_rooms_empty', { status: r.status, rooms: norm(r.body.rooms) });

  r = await twirp('CreateRoom', { name: CP_A }, tokA); ownedRooms.add(CP_A);
  assert.equal(r.status, 200); assert.equal(r.body.name, CP_A); assert.match(r.body.sid, /^RM_/);
  record('create_room_defaults', { status: r.status, room: norm(r.body) });

  r = await twirp('CreateRoom', { name: CP_B, metadata: 'm2', empty_timeout: 120, departure_timeout: 30, max_participants: 5 }, tokB); ownedRooms.add(CP_B);
  assert.equal(r.status, 200); assert.equal(r.body.metadata, 'm2'); assert.equal(r.body.max_participants, 5);
  assert.equal(r.body.empty_timeout, 120); assert.equal(r.body.departure_timeout, 30);
  record('create_room_full', { status: r.status, room: norm(r.body) });

  r = await twirp('ListRooms', {}, tokA);
  assert.equal(r.status, 200); assert.deepEqual(r.body.rooms.map(x => x.name).sort(), [CP_A, CP_B]);
  record('list_rooms_populated', { status: r.status, rooms: norm([...r.body.rooms].sort(byName)) });

  r = await twirp('ListRooms', { names: [CP_B] }, tokA);
  assert.equal(r.status, 200); assert.deepEqual(r.body.rooms.map(x => x.name), [CP_B]);
  record('list_rooms_filter', { status: r.status, rooms: norm(r.body.rooms) });

  r = await twirp('UpdateRoomMetadata', { room: CP_B, metadata: 'updated-value' }, tokB);
  assert.equal(r.status, 200); assert.equal(r.body.metadata, 'updated-value'); assert.equal(r.body.name, CP_B);
  record('update_room_metadata', { status: r.status, room: norm(r.body) });

  r = await twirp('UpdateRoomMetadata', { room: 'rs-ghost', metadata: 'x' }, await adminToken('rs-ghost'));
  assert.equal(r.status, 404); assert.equal(r.body.code, 'not_found');
  record('update_room_metadata_missing', { status: r.status, error: norm(r.body) });

  r = await twirp('ListParticipants', { room: CP_A }, tokA);
  assert.equal(r.status, 200); assert.deepEqual(r.body.participants, []);
  record('list_participants_empty', { status: r.status, participants: norm(r.body.participants) });

  r = await twirp('GetParticipant', { room: CP_A, identity: 'ghost' }, tokA);
  assert.equal(r.status, 404); assert.equal(r.body.code, 'not_found');
  record('get_participant_missing', { status: r.status, error: norm(r.body) });

  r = await twirp('RemoveParticipant', { room: CP_A, identity: 'ghost' }, tokA);
  assert.equal(r.status, 404); assert.equal(r.body.code, 'not_found');
  record('remove_participant_missing', { status: r.status, error: norm(r.body) });

  r = await twirp('UpdateParticipant', { room: CP_A, identity: 'ghost', metadata: 'x' }, tokA);
  assert.equal(r.status, 404); assert.equal(r.body.code, 'not_found');
  record('update_participant_missing', { status: r.status, error: norm(r.body) });

  r = await twirp('ForwardParticipant', { room: CP_A, identity: 'x', destination_room: CP_A }, await adminToken(CP_A, CP_A));
  assert.equal(r.status, 400); assert.equal(r.body.code, 'invalid_argument');
  record('forward_same_room', { status: r.status, error: norm(r.body) });

  const tokFwd = await adminToken(CP_A, CP_B);
  r = await twirp('ForwardParticipant', { room: CP_A, identity: 'ghost', destination_room: CP_B }, tokFwd);
  assert.equal(r.status, 503); assert.equal(r.body.code, 'unavailable');
  record('forward_missing_participant', { status: r.status, error: norm(r.body) });

  r = await twirp('MoveParticipant', { room: CP_A, identity: 'x', destination_room: CP_A }, await adminToken(CP_A, CP_A));
  assert.equal(r.status, 400); assert.equal(r.body.code, 'invalid_argument');
  record('move_same_room', { status: r.status, error: norm(r.body) });

  r = await twirp('PerformRpc', { room: CP_A, destination_identity: '', method: 'm', payload: '' }, tokA);
  assert.equal(r.status, 400); assert.equal(r.body.code, 'invalid_argument');
  record('perform_rpc_empty_destination', { status: r.status, error: norm(r.body) });

  r = await twirp('DeleteRoom', { room: CP_B }, tokB);
  assert.equal(r.status, 200); ownedRooms.delete(CP_B);
  record('delete_room', { status: r.status, body: norm(r.body) });

  r = await twirp('DeleteRoom', { room: CP_B }, tokB);
  assert.equal(r.status, 404); assert.equal(r.body.code, 'not_found');
  record('delete_room_missing', { status: r.status, error: norm(r.body) });

  r = await twirp('ListRooms', {}, tokA);
  assert.equal(r.status, 200); assert.deepEqual(r.body.rooms.map(x => x.name), [CP_A]);
  record('list_rooms_after_delete', { status: r.status, rooms: norm(r.body.rooms) });

  // ---- Media plane: two real SDK peers witness the participant-dependent side-effects ----
  const tokM = await adminToken(MEDIA);
  r = await twirp('CreateRoom', { name: MEDIA, empty_timeout: 120, departure_timeout: 60 }, tokM); ownedRooms.add(MEDIA);
  assert.equal(r.status, 200);
  async function joinPeer(identity) {
    const page = await context.newPage(); pages.push(page);
    page.on('pageerror', e => result.errors.push('pageerror: ' + redact(e.message)));
    await page.goto(assetUrl);
    const t = new AccessToken(key, secret, { identity });
    t.addGrant({ roomJoin: true, room: MEDIA, canPublish: true, canSubscribe: true, canPublishData: true });
    const conn = await page.evaluate(a => fixture.connect(a), { url: wsUrl, token: await t.toJwt(), single: true });
    return { page, conn };
  }
  const pub = await joinPeer(PUB);
  const sub = await joinPeer(SUB);
  const { videoSid, audioSid } = await pub.page.evaluate(() => fixture.publish());
  await until(async () => {
    const s = await snapshot(sub.page);
    return s.connection === 'connected' && s.frames >= 15 && s.audible >= 5 && s.width === 640 && s.height === 360;
  }, 'subscriber did not decode real publisher audio+video');
  const pubSnap = await snapshot(pub.page);
  assert.equal(pubSnap.connection, 'connected');
  record('media_join', { publisherConnected: pubSnap.connection === 'connected', subscriberConnected: true,
    subscriberVideoDecoded: true, subscriberAudioAudible: true, subscriberWidth: 640, subscriberHeight: 360 });

  r = await twirp('ListParticipants', { room: MEDIA }, tokM);
  assert.equal(r.status, 200); assert.deepEqual(r.body.participants.map(p => p.identity).sort(), [PUB, SUB]);
  record('list_participants_populated', { status: r.status, participants: norm([...r.body.participants].sort(byIdentity)) });

  r = await twirp('GetParticipant', { room: MEDIA, identity: PUB }, tokM);
  assert.equal(r.status, 200); assert.equal(r.body.identity, PUB);
  assert.equal(r.body.tracks.length, 2);
  // SSRC presence is a real contract (Go binds it from incoming RTP). The value
  // is client-random and normalized away, so assert non-zero population on the
  // raw body here — a server reporting ssrc=0 fails this on its own side.
  const videoTrack = r.body.tracks.find(t => t.type === 'VIDEO');
  assert.ok(videoTrack, 'publisher has no video track');
  const layerSsrcs = (videoTrack.layers || []).map(l => l.ssrc);
  const codecLayerSsrcs = (videoTrack.codecs || []).flatMap(c => (c.layers || []).map(l => l.ssrc));
  assert.ok(layerSsrcs.length > 0 && layerSsrcs.every(s => Number.isInteger(s) && s > 0),
    'video TrackInfo.layers[].ssrc not populated from RTP: ' + JSON.stringify(layerSsrcs));
  assert.ok(codecLayerSsrcs.length > 0 && codecLayerSsrcs.every(s => Number.isInteger(s) && s > 0),
    'video codecs[].layers[].ssrc not populated from RTP: ' + JSON.stringify(codecLayerSsrcs));
  const preUpdateVersion = r.body.version;
  record('get_participant', { status: r.status, participant: norm(r.body),
    videoLayerSsrcPopulated: true, videoCodecLayerSsrcPopulated: true });

  r = await twirp('UpdateParticipant', { room: MEDIA, identity: PUB, metadata: 'pmeta', name: 'PName', attributes: { locale: 'zh' } }, tokM);
  assert.equal(r.status, 200); assert.equal(r.body.metadata, 'pmeta'); assert.equal(r.body.name, 'PName');
  // The participant version is a coalesced monotonic counter: its magnitude is
  // implementation-defined, but it MUST strictly increase across an update.
  assert.ok(Number.isInteger(r.body.version) && r.body.version > preUpdateVersion,
    `participant version not monotonic: ${preUpdateVersion} -> ${r.body.version}`);
  const afterUpdate = await twirp('GetParticipant', { room: MEDIA, identity: PUB }, tokM);
  assert.equal(afterUpdate.body.metadata, 'pmeta'); assert.equal(afterUpdate.body.name, 'PName');
  record('update_participant', { status: r.status, participant: norm(r.body),
    versionMonotonic: r.body.version > preUpdateVersion,
    getEchoesMetadata: afterUpdate.body.metadata === 'pmeta', getEchoesName: afterUpdate.body.name === 'PName',
    getEchoesAttributes: norm(afterUpdate.body.attributes) });

  r = await twirp('MutePublishedTrack', { room: MEDIA, identity: PUB, track_sid: audioSid, muted: true }, tokM);
  assert.equal(r.status, 200); assert.equal(r.body.track.muted, true);
  await until(async () => evCount(await snapshot(sub.page), 'trackMuted', audioSid) >= 1, 'subscriber did not observe remote TrackMuted');
  const mutedGet = await twirp('GetParticipant', { room: MEDIA, identity: PUB }, tokM);
  const mutedTrack = mutedGet.body.tracks.find(t => t.sid === audioSid);
  assert.equal(mutedTrack.muted, true);
  record('mute_published_track', { status: r.status, responseMuted: r.body.track.muted,
    getParticipantMuted: mutedTrack.muted, subscriberTrackMutedEvents: evCount(await snapshot(sub.page), 'trackMuted', audioSid) });

  r = await twirp('MutePublishedTrack', { room: MEDIA, identity: PUB, track_sid: audioSid, muted: false }, tokM);
  assert.equal(r.status, 200); assert.equal(r.body.track.muted, false);
  await until(async () => evCount(await snapshot(sub.page), 'trackUnmuted', audioSid) >= 1, 'subscriber did not observe remote TrackUnmuted');
  record('unmute_published_track', { status: r.status, responseMuted: r.body.track.muted,
    subscriberTrackUnmutedEvents: evCount(await snapshot(sub.page), 'trackUnmuted', audioSid) });

  const payload = { seq: 1, topic: 'rs-topic' };
  r = await twirp('SendData', { room: MEDIA, data: Buffer.from(JSON.stringify(payload)).toString('base64'), kind: 0,
    destination_identities: [SUB], topic: 'rs-topic' }, tokM);
  assert.equal(r.status, 200);
  await until(async () => (await snapshot(sub.page)).data.some(d => d.seq === 1), 'subscriber did not receive targeted SendData');
  const subData = (await snapshot(sub.page)).data.find(d => d.seq === 1);
  const pubData = (await snapshot(pub.page)).data;
  assert.equal(subData.topic, 'rs-topic'); assert.equal(subData.payloadTopic, 'rs-topic'); assert.equal(subData.fromServer, true);
  record('send_data', { status: r.status, subscriberReceived: true, topic: subData.topic,
    payloadTopicMatches: subData.payloadTopic === subData.topic, fromServer: subData.fromServer, kind: subData.kind,
    publisherReceived: pubData.some(d => d.seq === 1) });

  r = await twirp('UpdateSubscriptions', { room: MEDIA, identity: SUB, track_sids: [videoSid], subscribe: false }, tokM);
  assert.equal(r.status, 200);
  await until(async () => { const s = await snapshot(sub.page); return evCount(s, 'unsubscribed', videoSid) >= 1 && s.publications.some(p => p.sid === videoSid && !p.isSubscribed && !p.hasTrack); }, 'subscriber did not unsubscribe video');
  record('update_subscriptions_off', { status: r.status, unsubscribedEvents: evCount(await snapshot(sub.page), 'unsubscribed', videoSid),
    isSubscribed: (await snapshot(sub.page)).publications.find(p => p.sid === videoSid).isSubscribed });

  const frameBase = (await snapshot(sub.page)).frames;
  r = await twirp('UpdateSubscriptions', { room: MEDIA, identity: SUB, track_sids: [videoSid], subscribe: true }, tokM);
  assert.equal(r.status, 200);
  await until(async () => { const s = await snapshot(sub.page); return evCount(s, 'subscribed', videoSid) >= 2 && s.publications.some(p => p.sid === videoSid && p.isSubscribed && p.hasTrack) && s.frames >= frameBase + 15; }, 'subscriber did not resubscribe and decode video');
  record('update_subscriptions_on', { status: r.status, subscribedEvents: evCount(await snapshot(sub.page), 'subscribed', videoSid),
    isSubscribed: (await snapshot(sub.page)).publications.find(p => p.sid === videoSid).isSubscribed, framesAdvanced: true });

  r = await twirp('PerformRpc', { room: MEDIA, destination_identity: SUB, method: 'api.echo', payload: 'hello', response_timeout_ms: 10000 }, tokM);
  assert.equal(r.status, 200); assert.equal(r.body.payload, 'echo:hello');
  assert.equal((await snapshot(sub.page)).rpcCalls, 1); assert.equal((await snapshot(pub.page)).rpcCalls, 0);
  record('perform_rpc', { status: r.status, payload: r.body.payload,
    destinationRpcCalls: (await snapshot(sub.page)).rpcCalls, publisherRpcCalls: (await snapshot(pub.page)).rpcCalls });

  r = await twirp('RemoveParticipant', { room: MEDIA, identity: SUB }, tokM);
  assert.equal(r.status, 200);
  await until(async () => (await snapshot(sub.page)).connection === 'disconnected', 'removed subscriber SDK did not disconnect');
  await until(async () => { const lp = await twirp('ListParticipants', { room: MEDIA }, tokM); return lp.body.participants.length === 1; }, 'owner did not drop removed participant');
  // The publisher must observe the removed peer's ParticipantDisconnected; poll
  // for delivery so the count is sampled after the event arrives, not mid-race.
  await until(async () => evCount(await snapshot(pub.page), 'participantDisconnected', undefined) >= 1, 'publisher did not observe removed participant disconnect');
  const subAfterRemove = await snapshot(sub.page);
  record('remove_participant', { status: r.status, subscriberDisconnected: subAfterRemove.connection === 'disconnected',
    subscriberDisconnectReason: subAfterRemove.disconnectedReason,
    publisherParticipantDisconnectedEvents: evCount(await snapshot(pub.page), 'participantDisconnected', undefined),
    remainingParticipants: 1 });

  r = await twirp('DeleteRoom', { room: MEDIA }, tokM); ownedRooms.delete(MEDIA);
  assert.equal(r.status, 200);
  await until(async () => (await snapshot(pub.page)).connection === 'disconnected', 'publisher SDK did not disconnect on DeleteRoom');
  const pubAfterDelete = await snapshot(pub.page);
  record('delete_room_media', { status: r.status, publisherDisconnected: pubAfterDelete.connection === 'disconnected',
    publisherDisconnectReason: pubAfterDelete.disconnectedReason });

  assert.ok(result.errors.length === 0, 'page errors: ' + result.errors.join('; '));
  result.match = true;
} catch (error) {
  result.errors.push(redact(error.stack || error.message).slice(0, 2000));
} finally {
  for (const page of pages) { try { await page.evaluate(() => fixture.close()); } catch { result.cleanup_errors.push('disconnect-owned-peer'); } }
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
  const cleanupTok = await adminToken(CP_A);
  for (const room of ownedRooms) { try { await twirp('DeleteRoom', { room }, cleanupTok); } catch (e) { result.cleanup_errors.push('owned room ' + room + ': ' + redact(e.message)); } }
  assert.equal(createHash('sha256').update(await readFile(sdkPath)).digest('hex'), sdkSha);
  result.status = !result.errors.length && !result.cleanup_errors.length && result.match ? 'PASS' : 'FAIL';
  await writeFile(env.OUTPUT, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  process.exitCode = result.status === 'PASS' ? 0 : 1;
}