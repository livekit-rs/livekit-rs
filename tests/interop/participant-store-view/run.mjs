// Same unchanged SDK/real native WHIP fixture for pinned Go and Rust. NOT_RUN.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(here, '../package.json'));
const { chromium } = require('playwright');
const { RoomServiceClient, AccessToken } = require('livekit-server-sdk');
const url = process.env.LIVEKIT_URL || 'ws://127.0.0.1:7880';
const key = process.env.LIVEKIT_API_KEY || 'devkey', secret = process.env.LIVEKIT_API_SECRET || 'secret';
const mode = process.env.API_PARTICIPANT_VIEW;
assert.ok(['cached', 'live-psrpc'].includes(mode), 'explicit API_PARTICIPANT_VIEW=cached|live-psrpc required on BOTH reference and Rust server configurations');
const api = new RoomServiceClient(url.replace(/^ws/, 'http'), key, secret);
const roomName = `participant-view-${randomUUID()}`, identity = 'native-source', observerIdentity = 'unchanged-sdk-observer';
const report = { status: 'RUNNING', scope: 'Actual native WHIP VP8 dimensions + unchanged SDK decoded frames + public Get/List cached/live selection + genuine metadata callback refresh; no C3 distributed store claim', mode, requiredServerConfiguration: { api: { enable_psrpc_for_get_list_participants: mode === 'live-psrpc' } }, configurationVerifiedByRunner: false, steps: [] };
let server, browser, publisher, observer, created = false;
const metadata = t => ({ width: t.width, height: t.height, mimeType: t.mimeType, layers: t.layers.map(l => ({ quality: l.quality, width: l.width, height: l.height })), codecs: t.codecs.map(c => ({ mimeType: c.mimeType, videoLayerMode: c.videoLayerMode, layers: c.layers.map(l => ({ quality: l.quality, width: l.width, height: l.height })) })) });
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function until(check, message) { const end = Date.now() + 20000; while (Date.now() < end) { if (await check()) return; await sleep(50); } throw Error(message); }
async function token(who) { const t = new AccessToken(key, secret, { identity: who }); t.addGrant({ roomJoin: true, room: roomName, canPublish: true, canSubscribe: true }); return t.toJwt(); }
async function views() {
  const list = (await api.listParticipants(roomName)).find(p => p.identity === identity);
  const get = await api.getParticipant(roomName, identity);
  assert.ok(list, 'List must include actual publisher');
  assert.equal(list.sid, get.sid); assert.deepEqual(list.tracks.map(metadata), get.tracks.map(metadata));
  return { participant: get, metadata: metadata(get.tracks.find(t => t.type === 1)) };
}
try {
  const html = await readFile(join(here, '../native-whip-video/client.html'));
  const sdk = await readFile(require.resolve('livekit-client'));
  server = createServer((req, res) => { res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'text/javascript' : 'text/html' }); res.end(req.url === '/sdk.js' ? sdk : html); });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  await api.createRoom({ name: roomName }); created = true;
  browser = await chromium.launch({ channel: process.env.BROWSER_CHANNEL || 'chrome', headless: true, args: ['--no-sandbox', '--autoplay-policy=no-user-gesture-required'] });
  observer = await browser.newPage(); publisher = await browser.newPage();
  await Promise.all([observer.goto(`http://127.0.0.1:${server.address().port}`), publisher.goto(`http://127.0.0.1:${server.address().port}`)]);
  await observer.evaluate(a => videoFixture.observe(a), { url, token: await token(observerIdentity), identity });
  const jwt = await token(identity);
  await publisher.evaluate(a => videoFixture.publish(a), { url, token: jwt, codec: 'VP8' });
  await until(async () => { const s = await observer.evaluate(() => videoFixture.snapshot()); const p = (await api.listParticipants(roomName)).find(p => p.identity === identity); return s.frames >= 40 && s.lastWidth === 640 && s.lastHeight === 360 && p?.tracks.some(t => t.type === 1); }, 'actual initial decoded640x360 and publication absent');
  if (mode === 'live-psrpc') await until(async () => { const v = await views(); return v.metadata.width === 640 && v.metadata.height === 360; }, 'live API did not expose actual initial dimensions');
  const initial = await views(), sid = initial.participant.sid, trackSid = initial.participant.tracks.find(t => t.type === 1).sid;
  report.initial = initial.metadata;
  const beforeFrames = (await observer.evaluate(() => videoFixture.snapshot())).frames;
  await publisher.evaluate(() => videoFixture.resize());
  await until(async () => { const s = await observer.evaluate(() => videoFixture.snapshot()); return s.frames >= beforeFrames + 30 && s.lastWidth === 960 && s.lastHeight === 540; }, 'same real RTC track did not decode resized960x540');
  if (mode === 'live-psrpc') await until(async () => { const v = await views(); return v.metadata.width === 960 && v.metadata.height === 540; }, 'live API did not expose private receiver size change');
  let after;
  // These are API observations, not a cache refresh mechanism. Repeated reads must leave cached view unchanged.
  for (let i = 0; i < 20; i++) { after = await views(); if (mode === 'cached') assert.deepEqual(after.metadata, initial.metadata, 'private native dimensions must not refresh stored participant'); await sleep(50); }
  assert.equal(after.participant.sid, sid); assert.equal(after.participant.tracks.find(t => t.type === 1).sid, trackSid);
  report.afterPrivateResize = after.metadata;
  report.steps.push('actual-decoded-resize-without-participant-change', 'public-get-list-same-view');
  await api.updateParticipant(roomName, identity, { metadata: 'f21-genuine-participant-change' });
  await until(async () => { const v = await views(); return v.participant.metadata === 'f21-genuine-participant-change' && v.metadata.width === 960 && v.metadata.height === 540; }, 'real participant metadata callback did not refresh stored full TrackInfo');
  const refreshed = await views(); report.afterParticipantChange = refreshed.metadata;
  assert.equal(refreshed.participant.sid, sid); assert.equal(refreshed.participant.tracks.find(t => t.type === 1).sid, trackSid);
  report.steps.push('actual-metadata-update-refreshes-full-snapshot');
  assert.equal(await publisher.evaluate(a => videoFixture.remove(a), { token: jwt }), 200);
  await until(async () => !(await api.listParticipants(roomName)).some(p => p.identity === identity), 'removed publisher retained in participant store');
  await assert.rejects(() => api.getParticipant(roomName, identity), 'Get must reject actual removed publisher');
  report.steps.push('actual-delete-removes-stored-and-live-participant');
  if (process.env.PARTICIPANT_VIEW_REFERENCE_RESULTS) {
    const reference = JSON.parse(await readFile(process.env.PARTICIPANT_VIEW_REFERENCE_RESULTS, 'utf8'));
    assert.equal(reference.status, 'REFERENCE_CAPTURED'); assert.equal(reference.mode, mode);
    for (const field of ['initial', 'afterPrivateResize', 'afterParticipantChange']) assert.deepEqual(report[field], reference[field], `exact fixed Go ${field}`);
    report.status = 'PASS';
  } else report.status = 'REFERENCE_CAPTURED';
} catch (error) {
  report.status = 'FAIL'; report.failure = String(error.message).replaceAll(secret, '<secret>').replaceAll(roomName, '<owned-room>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>'); process.exitCode = 1;
} finally {
  const failures = [];
  for (const page of [publisher, observer]) if (page) try { await page.evaluate(() => videoFixture.close()); } catch { failures.push('owned-client-close'); }
  try { await browser?.close(); } catch { failures.push('owned-browser-close'); }
  if (created) try { await api.deleteRoom(roomName); } catch { failures.push('owned-room-delete'); }
  if (server) try { await new Promise(r => server.close(r)); } catch { failures.push('owned-http-close'); }
  if (failures.length) { report.cleanupFailures = failures; report.status = 'FAIL'; process.exitCode = 1; }
  await writeFile(process.env.PARTICIPANT_VIEW_OUTPUT || join(here, 'actual-participant-view.json'), JSON.stringify(report, null, 2) + '\n');
}
