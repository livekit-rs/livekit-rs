import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { LossyUdpProxy } from '../media-recovery/lossy-proxy.mjs';

const require = createRequire(import.meta.url);
const { chromium } = require('playwright');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const sdk = await readFile(require.resolve('livekit-client'));
const lock = await readFile(new URL('../package-lock.json', import.meta.url));
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const url = process.env.LIVEKIT_URL;
const turnPort = Number(process.env.TURN_PORT);
const testLoss = process.env.LK_AUDIO_RED_LOSS === '1';
const testVideo = process.env.LK_AUDIO_RED_VIDEO === '1';
const encryptPlain = process.env.LK_AUDIO_RED_ENCRYPT_PLAIN === '1';
const e2eeWorker = encryptPlain ? await readFile(require.resolve('livekit-client/e2ee-worker')) : null;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const host = createServer((request, response) => {
  if (request.url === '/sdk.js' || (request.url === '/e2ee-worker.js' && e2eeWorker)) {
    response.writeHead(200, { 'content-type': 'text/javascript' });
    response.end(request.url === '/sdk.js' ? sdk : e2eeWorker);
    return;
  }
  response.writeHead(200, { 'content-type': 'text/html' });
  response.end(`<!doctype html><body><script>
    window.pcs = [];
    const NativePC = window.RTCPeerConnection;
    window.RTCPeerConnection = class extends NativePC {
      constructor(config, ...rest) { super(window.route(config), ...rest); pcs.push(this); }
      setConfiguration(config) { return super.setConfiguration(window.route(config)); }
    };
  </script><script src="/sdk.js"></script></body>`);
});
await new Promise(resolve => host.listen(0, '127.0.0.1', resolve));

async function connect(page, proxy, room, identity, single) {
  await page.goto(`http://127.0.0.1:${host.address().port}`);
  const token = new AccessToken('devkey', 'secret', { identity, ttl: '5m' });
  token.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  await page.evaluate(async ({ url, token, identity, single, proxyPort, turnPort, encryptPlain }) => {
    window.route = config => {
      if (!config?.iceServers) return config;
      return {
        ...config,
        iceServers: config.iceServers.map(server => ({
          ...server,
          urls: (Array.isArray(server.urls) ? server.urls : [server.urls]).map(uri =>
            uri.endsWith(`:${turnPort}?transport=udp`)
              ? `turn:127.0.0.1:${proxyPort}?transport=udp` : uri),
        })),
      };
    };
    const options = { singlePeerConnection: single, adaptiveStream: false, dynacast: false };
    if (encryptPlain && identity !== 'red') {
      const keyProvider = new LivekitClient.ExternalE2EEKeyProvider();
      await keyProvider.setKey('public-mixed-media-fixture');
      window.e2eeWorker = new Worker('/e2ee-worker.js', { type: 'module' });
      options.encryption = { keyProvider, worker: e2eeWorker };
    }
    window.room = new LivekitClient.Room(options);
    window.encryptionErrors = [];
    room.on(LivekitClient.RoomEvent.EncryptionError, error => encryptionErrors.push(String(error)));
    room.on(LivekitClient.RoomEvent.TrackSubscribed, track => document.body.appendChild(track.attach()));
    await room.connect(url, token, { rtcConfig: { iceTransportPolicy: 'relay' } });
    if (encryptPlain && identity !== 'red') await room.setE2EEEnabled(true);
  }, { url, token: await token.toJwt(), identity, single, proxyPort: proxy.port, turnPort, encryptPlain });
}

async function publish(page, name, red) {
  await page.evaluate(async ({ name, red }) => {
    const context = new AudioContext();
    await context.resume();
    const tone = context.createOscillator();
    const destination = context.createMediaStreamDestination();
    tone.frequency.value = red ? 440 : 660;
    tone.connect(destination);
    tone.start();
    window.source = { context, tone, track: destination.stream.getAudioTracks()[0] };
    await room.localParticipant.publishTrack(source.track, {
      name, source: LivekitClient.Track.Source.Microphone, dtx: false, red,
    });
  }, { name, red });
}

async function publishVideo(page) {
  await page.evaluate(async () => {
    const canvas = document.createElement('canvas');
    canvas.width = 320;
    canvas.height = 180;
    const context = canvas.getContext('2d');
    let frame = 0;
    const draw = () => {
      context.fillStyle = frame % 2 ? '#153e6d' : '#174329';
      context.fillRect(0, 0, canvas.width, canvas.height);
      context.fillStyle = '#ffffff';
      context.fillRect((frame * 9) % (canvas.width - 32), 50, 32, 64);
      frame++;
    };
    draw();
    window.videoTimer = setInterval(draw, 65);
    await room.localParticipant.publishTrack(canvas.captureStream(15).getVideoTracks()[0], {
      name: 'camera', source: LivekitClient.Track.Source.Camera, videoCodec: 'vp8',
      simulcast: false, videoEncoding: { maxBitrate: 400000, maxFramerate: 15 },
    });
  });
}

async function snapshot(page) {
  return page.evaluate(async () => {
    const active = pcs.filter(pc => pc.connectionState !== 'closed');
    const reports = await Promise.all(active.map(pc => pc.getStats()));
    const tracks = [...room.remoteParticipants.values()].flatMap(participant =>
      [...participant.audioTrackPublications.values()].filter(track => track.isSubscribed).map(track =>
        ({ name: track.trackName, id: track.track.mediaStreamTrack.id, encrypted: track.isEncrypted })));
    const videoTrack = [...room.remoteParticipants.values()].flatMap(participant =>
      [...participant.videoTrackPublications.values()]).find(track => track.trackName === 'camera' && track.isSubscribed);
    const streams = reports.flatMap(report => [...report.values()]
      .filter(entry => ['audio', 'video'].includes(entry.kind) && ['inbound-rtp', 'outbound-rtp'].includes(entry.type))
      .map(entry => ({
        kind: entry.kind, direction: entry.type, track: entry.trackIdentifier, ssrc: entry.ssrc, mid: entry.mid,
        samples: entry.totalSamplesReceived ?? 0, packets: entry.packetsReceived ?? entry.packetsSent ?? 0,
        frames: entry.framesDecoded ?? entry.framesEncoded ?? 0,
        width: entry.frameWidth, height: entry.frameHeight,
        codec: report.get(entry.codecId)?.mimeType, payloadType: report.get(entry.codecId)?.payloadType,
        nackCount: entry.nackCount ?? 0,
      })));
    const feedback = Object.assign({}, ...active.map(pc => Object.fromEntries(
      (pc.remoteDescription?.sdp ?? '').split(/(?=^m=)/m).filter(section => section.startsWith('m=audio '))
        .map(section => {
          const mid = /^a=mid:([^\r\n]+)/m.exec(section)?.[1];
          const opus = /^a=rtpmap:(\d+) opus\//im.exec(section)?.[1];
          return [mid, section.split(/\r?\n/).includes(`a=rtcp-fb:${opus} nack`)];
        }),
    )));
    return {
      tracks, videoTrack: videoTrack?.track.mediaStreamTrack.id, videoEncrypted: videoTrack?.isEncrypted,
      streams, feedback,
    };
  });
}

function wire(proxy, direction, stream, since) {
  assert.ok(stream?.ssrc, 'browser stats must identify the audio source');
  const packetsByPt = {};
  for (const packet of proxy.rtp[direction]) {
    if (packet.ssrc === stream.ssrc && packet.t >= since) {
      packetsByPt[packet.pt] = (packetsByPt[packet.pt] ?? 0) + 1;
    }
  }
  assert.ok(Object.values(packetsByPt).some(count => count >= 10), 'missing physical TURN/SRTP packets');
  return { ssrc: stream.ssrc, samples: stream.samples, packets: stream.packets, packetsByPt };
}

function videoObservation(view, proxy, direction, since) {
  const stream = view.streams.find(entry => entry.kind === 'video'
    && entry.direction === (direction === 'up' ? 'outbound-rtp' : 'inbound-rtp')
    && (direction === 'up' || entry.track === view.videoTrack));
  assert.equal(stream?.codec?.toLowerCase(), 'video/vp8');
  assert.deepEqual([stream.width, stream.height], [320, 180]);
  const physical = wire(proxy, direction, stream, since);
  assert.ok(physical.packetsByPt[stream.payloadType] >= 10, 'missing primary VP8 payload on TURN');
  assert.ok(stream.frames >= 10, 'missing encoded or decoded VP8 frames');
  return {
    ...physical, frames: stream.frames, codec: stream.codec, payloadType: stream.payloadType,
    encrypted: direction === 'down' ? view.videoEncrypted : undefined,
  };
}

function incoming(view, proxy, name, since) {
  const track = view.tracks.find(track => track.name === name);
  const stream = view.streams.find(stream => stream.direction === 'inbound-rtp' && stream.track === track?.id);
  assert.ok(stream?.samples > 0 && stream.packets >= 10, `missing decoded ${name} audio`);
  return { ...wire(proxy, 'down', stream, since), opusNack: view.feedback[stream.mid] ?? false };
}

async function waitForAudio(page, names) {
  await page.waitForFunction(expected =>
    expected.every(name => [...room.remoteParticipants.values()].some(participant =>
      [...participant.audioTrackPublications.values()].some(track => track.trackName === name && track.isSubscribed))),
  names, { timeout: 8000 });
  await sleep(900);
  return snapshot(page);
}

async function decodedTones(page) {
  return page.evaluate(async () => {
    const publications = [...room.remoteParticipants.values()].flatMap(participant =>
      [...participant.audioTrackPublications.values()]).filter(publication => publication.isSubscribed);
    const context = new AudioContext();
    await context.resume();
    try {
      const probes = publications.map(publication => {
        const track = publication.track.mediaStreamTrack;
        const source = context.createMediaStreamSource(new MediaStream([track]));
        const analyser = context.createAnalyser();
        analyser.fftSize = 4096;
        source.connect(analyser);
        return { publication, analyser };
      });
      await new Promise(resolve => setTimeout(resolve, 200));
      return Object.fromEntries(probes.map(({ publication, analyser }) => {
        const spectrum = new Float32Array(analyser.frequencyBinCount);
        analyser.getFloatFrequencyData(spectrum);
        const peak = spectrum.reduce((best, sample, index) => sample > spectrum[best] ? index : best, 0);
        return [publication.trackName, {
          encrypted: publication.isEncrypted,
          hz: peak * context.sampleRate / analyser.fftSize,
          amplitude: Number.isFinite(spectrum[peak]) ? spectrum[peak] : null,
        }];
      }));
    } finally {
      await context.close();
    }
  });
}

async function profile(browser, api, single) {
  const name = `red-policy-${single}-${Date.now()}`;
  const proxies = [];
  const pages = [];
  try {
    for (const identity of ['subscriber', 'plain', 'red']) {
      const proxy = await new LossyUdpProxy({ seed: proxies.length + 1, upstreamPort: turnPort }).start();
      proxies.push(proxy);
      const page = await browser.newPage();
      pages.push(page);
      await connect(page, proxy, name, identity, single);
    }
    const [subscriber, plain, red] = pages;
    const [subscriberWire, plainWire, redWire] = proxies;
    const start = performance.now();
    await publish(plain, 'plain', false);
    if (testVideo) {
      await publishVideo(plain);
      await subscriber.waitForFunction(() => [...room.remoteParticipants.values()].some(participant =>
        [...participant.videoTrackPublications.values()].some(track => track.trackName === 'camera' && track.isSubscribed)),
      undefined, { timeout: 8000 });
    }
    const initial = await waitForAudio(subscriber, ['plain']);
    const plainOnly = incoming(initial, subscriberWire, 'plain', start);
    const video = testVideo ? { initial: videoObservation(initial, subscriberWire, 'down', start) } : undefined;
    const mixedAt = performance.now();
    await publish(red, 'red', true);
    const mixed = await waitForAudio(subscriber, ['plain', 'red']);
    const publications = await Promise.all([snapshot(plain), snapshot(red)]);
    const uplink = {
      plain: wire(plainWire, 'up', publications[0].streams.find(s => s.kind === 'audio' && s.direction === 'outbound-rtp'), start),
      red: wire(redWire, 'up', publications[1].streams.find(s => s.kind === 'audio' && s.direction === 'outbound-rtp'), mixedAt),
    };
    const downlink = {
      plain: incoming(mixed, subscriberWire, 'plain', mixedAt),
      red: incoming(mixed, subscriberWire, 'red', mixedAt),
    };
    const tones = encryptPlain ? await decodedTones(subscriber) : undefined;
    if (tones) {
      for (const [track, hz] of [['plain', 660], ['red', 440]]) {
        assert.ok(Number.isFinite(tones[track]?.amplitude) && Math.abs(tones[track]?.hz - hz) < 30,
          `wrong decoded tone for ${track}: ${JSON.stringify(tones[track])}`);
      }
      assert.deepEqual(await subscriber.evaluate(() => encryptionErrors), []);
    }
    if (video) {
      video.uplink = videoObservation(publications[0], plainWire, 'up', mixedAt);
      video.mixed = videoObservation(mixed, subscriberWire, 'down', mixedAt);
    }

    let loss;
    if (testLoss && !single) {
      const dropped = [];
      const beforeNacks = mixed.streams.find(stream => stream.ssrc === downlink.plain.ssrc).nackCount;
      const lossAt = performance.now();
      subscriberWire.set('down', { dropRtp: packet => {
        if (packet.ssrc !== downlink.plain.ssrc || packet.pt !== 111 || dropped.length >= 3) return false;
        if (dropped.some(previous => previous.seq === packet.seq)) return false;
        dropped.push(packet);
        return true;
      } });
      await sleep(1800);
      subscriberWire.clean();
      const afterLoss = await snapshot(subscriber);
      if (video) video.loss = videoObservation(afterLoss, subscriberWire, 'down', lossAt);
      loss = {
        nackCountDelta: afterLoss.streams.find(stream => stream.ssrc === downlink.plain.ssrc).nackCount - beforeNacks,
        dropped: dropped.map(packet => ({
          seq: packet.seq, timestamp: packet.ts,
          recovered: subscriberWire.rtp.down.some(forwarded => forwarded.ssrc === packet.ssrc
            && forwarded.seq === packet.seq && forwarded.ts === packet.ts && forwarded.t > packet.t),
        })),
        // Only the first compound SRTCP header is visible at the TURN proxy.
        firstHeaderNacks: subscriberWire.forwardedRtcp.up.filter(packet =>
          packet.t > lossAt && packet.pt === 205 && packet.fmt === 1).length,
      };
    }

    const retiredAt = performance.now();
    await red.evaluate(async () => {
      await room.localParticipant.unpublishTrack(source.track, true);
      source.tone.stop();
      await source.context.close();
    });
    const retired = await waitForAudio(subscriber, ['plain']);
    assert.deepEqual(retired.tracks.map(track => track.name), ['plain']);
    const after = incoming(retired, subscriberWire, 'plain', retiredAt);
    assert.equal(after.ssrc, downlink.plain.ssrc, 'retiring RED replaced the independent plain stream');
    assert.ok(after.samples > downlink.plain.samples, 'plain decoding stopped on RED retirement');
    if (video) video.after = videoObservation(retired, subscriberWire, 'down', retiredAt);
    return { single, plainOnly, uplink, downlink, after, loss, video, tones };
  } finally {
    await Promise.allSettled(pages.map(page => page.evaluate(async () => {
      clearInterval(window.videoTimer);
      try { await window.room?.disconnect(); } finally { window.e2eeWorker?.terminate(); }
    })));
    await Promise.all(pages.map(page => page.close()));
    await Promise.all(proxies.map(proxy => proxy.close()));
    await api.deleteRoom(name);
    assert.deepEqual(await api.listRooms([name]), []);
  }
}

let browser;
try {
  browser = await chromium.launch({
    channel: process.env.BROWSER_CHANNEL || 'chromium', headless: true,
    args: ['--autoplay-policy=no-user-gesture-required'],
  });
  const api = new RoomServiceClient(url.replace(/^ws/, 'http'), 'devkey', 'secret');
  const runs = [];
  for (const single of [true, false]) runs.push(await profile(browser, api, single));
  console.log(JSON.stringify({
    sdkSha256: sha(sdk), workerSha256: e2eeWorker ? sha(e2eeWorker) : null, lockSha256: sha(lock), browser: browser.version(),
    sdkVersion: JSON.parse(lock).packages['node_modules/livekit-client'].version, runs,
  }, null, 2));
} finally {
  await browser?.close();
  await new Promise(resolve => host.close(resolve));
}
