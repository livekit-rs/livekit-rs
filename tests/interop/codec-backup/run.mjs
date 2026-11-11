// Playwright uses the unmodified official SDK. Only the late subscriber's
// browser codec preferences are restricted, to represent a VP8-only receiver.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { chromium } from 'playwright';
import { AccessToken, RoomServiceClient } from 'livekit-server-sdk';
import { SignalRequest, SignalResponse } from '@livekit/protocol';

const { LIVEKIT_URL: url, CODEC_OUTPUT: output, CODEC_PROFILE: profile,
  SINGLE_PC: single } = process.env;
const policies = { regression: 2, simulcast: 1, 'prefer-below': 0, 'prefer-equal': 0 };
const rawPolicies = { 'unknown-positive': 99, 'unknown-negative': -1 };
const rawPolicy = rawPolicies[profile];
assert.ok(Object.hasOwn(policies, profile) || rawPolicy !== undefined);
const sdk = await readFile(createRequire(import.meta.url).resolve('livekit-client'));
const lock = await readFile(new URL('../package-lock.json', import.meta.url));
const sha = data => createHash('sha256').update(data).digest('hex');
const server = createServer((req, res) => {
  if (req.url === '/sdk.js') {
    res.setHeader('Content-Type', 'text/javascript');
    res.end(sdk);
  } else {
    res.setHeader('Content-Type', 'text/html');
    res.end('<!doctype html><script src="/sdk.js"></script>');
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const room = `backup-${Date.now()}`;
const api = new RoomServiceClient(url.replace(/^ws/, 'http'), 'devkey', 'secret');
const evidence = { profile, single: single === 'true', sdkSha256: sha(sdk),
  lockSha256: sha(lock), sdkVersion: JSON.parse(lock).packages['node_modules/livekit-client'].version,
  status: 'FAIL' };
let browser;
const clients = new Map();
const signaling = [];
let created = false;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function client(identity, backupOnly = false) {
  const page = await browser.newPage();
  if (identity === 'publisher' && rawPolicy !== undefined) {
    // Keep the SDK unmodified. Change only the policy field in the first
    // video publication's binary AddTrack on its real signaling socket.
    await page.routeWebSocket(address => address.host === new URL(url).host, route => {
      const upstream = route.connectToServer();
      route.onMessage(message => {
        if (Buffer.isBuffer(message)) {
          const request = SignalRequest.fromBinary(message);
          const { case: kind, value } = request.message;
          if (kind === 'addTrack' && !value.sid && value.type === 1) {
            value.backupCodecPolicy = rawPolicy;
            evidence.injectedPolicy = rawPolicy;
            upstream.send(Buffer.from(request.toBinary()));
            return;
          }
        }
        upstream.send(message);
      });
      upstream.onMessage(message => {
        if (Buffer.isBuffer(message)) {
          const { case: kind, value } = SignalResponse.fromBinary(message).message;
          if (kind === 'trackPublished' && value.track) {
            signaling.push({ identity, kind, sid: value.track.sid, policy: value.track.backupCodecPolicy });
          }
        }
        route.send(message);
      });
    });
  }
  page.on('websocket', ws => {
    const record = (event, Type) => ws.on(event, ({ payload }) => {
      if (!Buffer.isBuffer(payload)) return;
      const { message: { case: kind, value } } = Type.fromBinary(payload);
      if (kind === 'subscribedQualityUpdate') signaling.push({ identity, kind, value: value.toJson() });
      if (kind === 'addTrack') signaling.push({ identity, kind,
        sid: value.sid, codecs: value.simulcastCodecs.map(codec => codec.codec) });
    });
    record('framereceived', SignalResponse);
    record('framesent', SignalRequest);
  });
  clients.set(identity, page);
  await page.addInitScript(({ backupOnly }) => {
    globalThis.observed = { pcs: [], video: new Map(), events: [], preferences: [] };
    const PC = globalThis.RTCPeerConnection;
    globalThis.RTCPeerConnection = class extends PC {
      constructor(...args) {
        super(...args);
        observed.pcs.push(this);
      }
      restrict() {
        if (!backupOnly) return;
        const capabilities = RTCRtpReceiver.getCapabilities('video').codecs
          .filter(codec => ['video/vp8', 'video/rtx'].includes(codec.mimeType.toLowerCase()));
        for (const transceiver of this.getTransceivers()) {
          if (transceiver.receiver.track.kind !== 'video') continue;
          transceiver.setCodecPreferences(capabilities);
          observed.preferences.push(capabilities.map(codec => codec.mimeType.toLowerCase()));
        }
      }
      createOffer(...args) {
        this.restrict();
        return super.createOffer(...args);
      }
      createAnswer(...args) {
        this.restrict();
        return super.createAnswer(...args);
      }
    };
  }, { backupOnly });
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const token = new AccessToken('devkey', 'secret', { identity });
  token.addGrant({ room, roomJoin: true, canSubscribe: true, canPublish: identity === 'publisher' });
  await page.evaluate(async ({ url, token, single }) => {
    const { Room, RoomEvent } = LivekitClient;
    globalThis.lkRoom = new Room({ singlePeerConnection: single, dynacast: true });
    lkRoom.on(RoomEvent.TrackSubscribed, (track, publication) => {
      if (track.kind !== 'video') return;
      const element = track.attach();
      element.muted = true;
      element.autoplay = true;
      document.body.append(element);
      observed.video.set(publication.trackSid, { element, id: track.mediaStreamTrack.id });
      observed.events.push(['subscribed', publication.trackSid]);
    });
    lkRoom.on(RoomEvent.TrackUnsubscribed, (track, publication) => {
      observed.events.push(['unsubscribed', publication.trackSid]);
    });
    await lkRoom.connect(url, token);
  }, { url, token: await token.toJwt(), single: single === 'true' });
  return page;
}

async function snapshot(page) {
  return page.evaluate(async () => {
    const stats = [];
    for (const pc of observed.pcs) {
      const report = await pc.getStats();
      for (const row of report.values()) {
        if (!['inbound-rtp', 'outbound-rtp'].includes(row.type) || row.kind !== 'video') continue;
        const mime = report.get(row.codecId)?.mimeType?.toLowerCase();
        if (mime) stats.push({ id: row.id, type: row.type, mime,
          frames: row.framesDecoded ?? row.framesEncoded ?? 0 });
      }
    }
    const video = [...observed.video].map(([sid, { element, id }]) => ({
      sid, id, width: element.videoWidth, height: element.videoHeight,
    }));
    const videoCodecs = description => description?.sdp.split(/(?=^m=)/m)
      .filter(section => section.startsWith('m=video'))
      .map(section => section.split(/\r?\n/).filter(line => /^m=|^a=(mid|rtpmap):/.test(line)));
    const sdps = observed.pcs.map(pc => ({
      local: videoCodecs(pc.localDescription), remote: videoCodecs(pc.remoteDescription),
    }));
    return { stats, video, events: observed.events, preferences: observed.preferences, sdps };
  });
}

function frames(row, kind, mime) {
  return row.stats.filter(stat => stat.type === kind && stat.mime === `video/${mime}`)
    .reduce((total, stat) => total + stat.frames, 0);
}

async function waitFor(label, sample, valid) {
  const end = Date.now() + 20000;
  let result;
  do {
    result = await sample();
    if (valid(result)) return result;
    await delay(150);
  } while (Date.now() < end);
  evidence.timedOut = { label, result };
  throw Error(label);
}

try {
  browser = await chromium.launch({ channel: 'chromium', headless: true,
    args: ['--autoplay-policy=no-user-gesture-required'] });
  evidence.browser = browser.version();
  await api.createRoom({ name: room, emptyTimeout: 30 });
  created = true;
  const publisher = await client('publisher');
  const first = await client('primary');
  evidence.sid = await publisher.evaluate(async policy => {
    const canvas = document.createElement('canvas');
    canvas.width = 640;
    canvas.height = 360;
    const ctx = canvas.getContext('2d');
    let frame = 0;
    const draw = () => {
      ctx.fillStyle = '#24a3af';
      ctx.fillRect(0, 0, 640, 360);
      ctx.fillStyle = '#ffdd60';
      ctx.fillRect(frame++ % 480, 70, 160, 100);
    };
    draw();
    setInterval(draw, 33);
    const [track] = canvas.captureStream(30).getVideoTracks();
    const publication = await lkRoom.localParticipant.publishTrack(track, {
      source: LivekitClient.Track.Source.Camera, videoCodec: 'vp9',
      backupCodec: { codec: 'vp8' }, backupCodecPolicy: policy, simulcast: false,
      scalabilityMode: 'L1T1',
    });
    return publication.trackSid;
  }, policies[profile] ?? 0);
  evidence.before = await waitFor('unrestricted subscriber must decode primary VP9',
    () => snapshot(first), row => frames(row, 'inbound-rtp', 'vp9') >= 12 && row.video.some(v => v.width > 0));
  const backup = await client('backup', true);
  evidence.backup = await waitFor('VP8-only subscriber must decode actual backup',
    () => snapshot(backup), row => frames(row, 'inbound-rtp', 'vp8') >= 12 && row.video.some(v => v.width > 0));
  const regresses = ['regression', 'prefer-below'].includes(profile);
  const expected = regresses ? 'vp8' : 'vp9';
  evidence.transition = await waitFor(`first subscriber must decode ${expected}`,
    () => snapshot(first), row => frames(row, 'inbound-rtp', expected) >= 12);
  const during = { first: await snapshot(first), backup: await snapshot(backup),
    publisher: await snapshot(publisher) };
  await delay(2000);
  evidence.after = { first: await snapshot(first), backup: await snapshot(backup),
    publisher: await snapshot(publisher) };
  const { after } = evidence;
  assert.ok(after.backup.preferences.length && after.backup.preferences.every(
    codecs => codecs.includes('video/vp8') && !codecs.includes('video/vp9')));
  const delta = (side, kind, mime) => frames(after[side], kind, mime) - frames(during[side], kind, mime);
  evidence.deltas = {
    firstVp9: delta('first', 'inbound-rtp', 'vp9'), firstVp8: delta('first', 'inbound-rtp', 'vp8'),
    backupVp8: delta('backup', 'inbound-rtp', 'vp8'), publisherVp8: delta('publisher', 'outbound-rtp', 'vp8'),
    publisherVp9: delta('publisher', 'outbound-rtp', 'vp9'),
  };
  if (rawPolicy !== undefined) {
    assert.equal(evidence.injectedPolicy, rawPolicy);
    const published = signaling.filter(row => row.kind === 'trackPublished' && row.sid === evidence.sid);
    assert.ok(published.length, 'missing binary TrackPublished for raw policy');
    assert.deepEqual(published.map(row => row.policy), published.map(() => rawPolicy));
  }
  assert.ok(evidence.deltas.backupVp8 > 5 && evidence.deltas.publisherVp8 > 5);
  assert.ok(regresses ? evidence.deltas.firstVp8 > 5 : evidence.deltas.firstVp9 > 5);
  assert.ok(regresses ? evidence.deltas.firstVp9 <= 2 : evidence.deltas.firstVp8 === 0);
  for (const row of [after.first, after.backup]) {
    assert.deepEqual(row.video.map(v => v.sid), [evidence.sid]);
  }
  assert.equal(after.first.video[0].id, evidence.before.video[0].id);
  await browser.close();
  browser = undefined;
  await api.deleteRoom(room);
  evidence.roomRemoved = !(await api.listRooms([room])).length;
  assert.ok(evidence.roomRemoved);
  evidence.status = 'PASS';
} catch (error) {
  evidence.error = String(error).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>');
  evidence.failureSnapshots = Object.fromEntries(await Promise.all(
    [...clients].map(async ([identity, page]) => [identity, await snapshot(page).catch(() => null)])));
  process.exitCode = 1;
} finally {
  await browser?.close().catch(() => {});
  if (created && !evidence.roomRemoved) await api.deleteRoom(room).catch(() => {});
  await new Promise(resolve => server.close(resolve));
  evidence.signaling = signaling;
  await writeFile(output, JSON.stringify(evidence, null, 2) + '\n');
}
