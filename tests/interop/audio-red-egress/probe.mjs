// Audio RED egress ground truth with the original browser SDK.
//
// A publisher sends an Opus track and a RED track; a second browser joins after both are already
// flowing. The TURN proxy records the cleartext RTP header of every egress packet (payload type,
// sequence, timestamp, datagram length) and an insertable-stream transform records the payload
// itself, so the RFC 2198 redundancy chain is measured rather than inferred. The publisher side gets
// a sender transform too, which makes the first uplink frame and the uplink payload bytes available:
// matching a downlink payload digest against the uplink digest for the same RTP timestamp is what
// separates "the server forwarded the client chain" from "the server rebuilt it".
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { LossyUdpProxy } from '../media-recovery/lossy-proxy.mjs';

const require = createRequire(import.meta.url);

// Exact RTP header accounting. The shared proxy records a lossy extension-id list, and TURN
// ChannelData pads every datagram to a 4-octet security margin, so a bare per-packet overhead
// cannot name the difference between two servers. This tap reads the fixed header, the extension
// block length and the raw extension bytes of each forwarded datagram in this process only, which
// leaves the shared proxy and every other harness's fixture hashes untouched.
LossyUdpProxy.prototype.observe = (function (original) {
  return function (direction, bytes, receivedAt, stage = 'forward') {
    if (stage === 'forward' && bytes.length > 16 && bytes[0] >= 0x40 && bytes[0] <= 0x7f) {
      const d = bytes.subarray(4);
      // SRTCP is identified by its payload type range, not by the RTP marker bit: an audio packet
      // opening a talkspurt carries marker=1 and must stay in the accounting.
      if ((d[0] >> 6) === 2 && !(d[1] >= 192 && d[1] <= 223)) {
        const x = (d[0] >> 4) & 1;
        const padBit = (d[0] >> 5) & 1;
        const cc = d[0] & 0x0f;
        const base = 12 + cc * 4;
        let extProfile = null, extWords = 0, extHead = [];
        if (x && d.length >= base + 4) {
          extProfile = d.readUInt16BE(base) === 0xbede ? '1B' : '2B';
          extWords = d.readUInt16BE(base + 2);
          extHead = Array.from(d.subarray(base, Math.min(d.length, base + 20)));
        }
        const rows = ((this.rawRows ??= {})[direction] ??= []);
        if (rows.length < 20000) {
          rows.push({
            t: performance.now(), ssrc: d.readUInt32BE(8), seq: d.readUInt16BE(2), ts: d.readUInt32BE(4),
            pt: d[1] & 0x7f, marker: (d[1] >> 7) & 1, cc, padBit, x, extProfile, extWords,
            headerBytes: base + (x ? 4 + extWords * 4 : 0), len: d.length,
            padLen: padBit && d.length ? d[d.length - 1] : 0, extHead: extHead.slice(0, 8),
          });
        }
      }
    }
    return original.call(this, direction, bytes, receivedAt, stage);
  };
})(LossyUdpProxy.prototype.observe);
const { chromium } = require('playwright');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const sdk = await readFile(require.resolve('livekit-client'));
const worker = await readFile(new URL('./red-observer.worker.mjs', import.meta.url));
const lock = await readFile(new URL('../package-lock.json', import.meta.url));
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const url = process.env.LIVEKIT_URL;
const turnPort = Number(process.env.TURN_PORT);
const activeRed = process.env.LK_RED_EGRESS_ACTIVE !== '0';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const host = createServer(async (request, response) => {
  if (request.url === '/sdk.js') {
    response.writeHead(200, { 'content-type': 'text/javascript' });
    return response.end(sdk);
  }
  if (request.url === '/red-observer.worker.mjs') {
    response.writeHead(200, { 'content-type': 'text/javascript' });
    return response.end(worker);
  }
  response.writeHead(200, { 'content-type': 'text/html' });
  response.end(`<!doctype html><body><script>
    window.pcs = [];
    window.frames = [];
    window.hooks = [];
    window.attachments = [];
    window.workerErrors = [];
    window.observeSend = false;
    window.observeRecv = false;
    // Chromium wires an insertable-stream transform into the RTP path when the stream starts, so a
    // receiver transform installed from the track event is accepted but never fed (measured: 0
    // frames against 227 inbound packets). Both directions are attached at addTransceiver time.
    const NativePC = window.RTCPeerConnection;
    window.RTCPeerConnection = class extends NativePC {
      constructor(config, ...rest) {
        super(window.route ? window.route(config) : config, ...rest);
        window.pcs.push(this);
        const addTransceiver = this.addTransceiver.bind(this);
        this.addTransceiver = (...args) => {
          const transceiver = addTransceiver(...args);
          if (window.observeSend && transceiver.sender?.track?.kind === 'audio') {
            window.attach(transceiver.sender, 'up', args[1]?.direction ?? null);
          }
          if (window.observeRecv && transceiver.receiver?.track?.kind === 'audio') {
            window.attach(transceiver.receiver, 'down', args[1]?.direction ?? null);
          }
          return transceiver;
        };
      }
      setConfiguration(config) { return super.setConfiguration(window.route ? window.route(config) : config); }
    };
    window.mine = new WeakMap();
    window.attach = (stream, origin, direction) => {
      const entry = { origin, direction, mid: null, kind: stream.track.kind, trackId: stream.track.id,
        attached: false, superseded: 0, error: null };
      window.attachments.push(entry);
      try {
        if (!window.observerWorker) {
          const w = new Worker('/red-observer.worker.mjs', { type: 'module' });
          w.onmessage = event => {
            if (event.data.hook) window.hooks.push(event.data);
            for (const row of event.data.rows ?? []) window.frames.push(row);
          };
          w.onerror = event => window.workerErrors.push([event.type, event.message, event.filename + ':' + event.lineno].join(' '));
          window.observerWorker = w;
        }
        const transform = new RTCRtpScriptTransform(window.observerWorker, { label: stream.track.id, origin });
        stream.transform = transform;
        window.mine.set(stream, transform);
        entry.attached = true;
      } catch (error) { entry.error = String(error); }
      return entry;
    };
    // The SDK may replace a transform during renegotiation; restore ours and count how often.
    window.reassert = () => {
      for (const pc of pcs.filter(entry => entry.connectionState !== 'closed')) {
        for (const transceiver of pc.getTransceivers()) {
          for (const stream of [transceiver.sender, transceiver.receiver]) {
            const ours = stream && window.mine.get(stream);
            if (ours && stream.transform !== ours) {
              const entry = window.attachments.find(item => item.trackId === stream.track.id && item.origin === (stream === transceiver.sender ? 'up' : 'down'));
              if (entry) entry.superseded += 1;
              stream.transform = ours;
            }
          }
        }
      }
      const midByTrack = {};
      for (const pc of pcs.filter(entry => entry.connectionState !== 'closed')) {
        for (const transceiver of pc.getTransceivers()) {
          if (transceiver.sender?.track?.kind === 'audio') midByTrack[transceiver.sender.track.id] = transceiver.mid ?? null;
          if (transceiver.receiver?.track?.kind === 'audio') midByTrack[transceiver.receiver.track.id] = transceiver.mid ?? null;
        }
      }
      for (const entry of window.attachments) entry.mid = midByTrack[entry.trackId] ?? entry.mid;
      return window.attachments;
    };
  </script><script src="/sdk.js"></script></body>`);
  return undefined;
});
await new Promise(resolve => host.listen(0, '127.0.0.1', resolve));

async function join(page, proxy, room, identity, observe) {
  await page.goto(`http://127.0.0.1:${host.address().port}`);
  await page.evaluate(({ role, turnPort, proxyPort }) => {
    window.observeSend = role === 'publisher';
    window.observeRecv = role === 'subscriber';
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
  }, { role: observe, turnPort, proxyPort: proxy.port });
  const token = new AccessToken('devkey', 'secret', { identity, ttl: '5m' });
  token.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  await page.evaluate(async ({ url, token }) => {
    window.room = new LivekitClient.Room({ adaptiveStream: false, dynacast: false });
    await window.room.connect(url, token, { rtcConfig: { iceTransportPolicy: 'relay' } });
  }, { url, token: await token.toJwt(), identity });
}

async function publishTracks(page, specs) {
  await page.evaluate(async (entries) => {
    const context = window.audioContext ?? (window.audioContext = new AudioContext());
    await context.resume();
    window.tones = window.tones ?? [];
    for (const [name, red, hz] of entries) {
      const tone = context.createOscillator();
      const destination = context.createMediaStreamDestination();
      tone.frequency.value = hz;
      tone.connect(destination);
      tone.start();
      const track = destination.stream.getAudioTracks()[0];
      const publication = await room.localParticipant.publishTrack(track, {
        name, source: LivekitClient.Track.Source.Microphone, dtx: false, red,
      });
      window.publishedIds = { ...(window.publishedIds ?? {}), [name]: track.id };
      window.tones.push({ tone, track, sid: publication?.trackSid ?? null });
    }
  }, specs);
}
const publishBoth = (page) => publishTracks(page, [['red', true, 440], ['plain', false, 660]]);

// The negotiated rtpmap, not the stats codec, decides which payload type carries a chain: the
// subscriber can be given a RED packet on a media section whose selected codec reads audio/opus.
async function sdpFacts(page) {
  return page.evaluate(() => {
    const maps = {};
    for (const pc of pcs.filter(entry => entry.connectionState !== 'closed')) {
      for (const description of [pc.localDescription, pc.remoteDescription]) {
        for (const match of (description?.sdp ?? '').matchAll(/^a=rtpmap:(\d+) ([^/\r\n]+)\/(\d+)/gm)) {
          maps[match[1]] = `${match[2].toLowerCase()}/${match[3]}`;
        }
      }
    }
    return maps;
  });
}

// The header-extension ids a packet carries are only interpretable against the negotiated extmap,
// and the answer side of that map is what the server agreed to.
async function extFacts(page) {
  return page.evaluate(() => {
    const facts = { local: {}, remote: {} };
    for (const pc of pcs.filter(entry => entry.connectionState !== 'closed')) {
      for (const [side, description] of [['local', pc.localDescription], ['remote', pc.remoteDescription]]) {
        for (const match of (description?.sdp ?? '').matchAll(/^a=extmap:(\d+)(?:\/([^\s]+))? (\S+)(?:\r?\n|$)/gm)) {
          facts[side][match[3]] = [match[1], match[2] ?? 'sendrecv'];
        }
      }
    }
    return facts;
  });
}

async function stats(page, direction) {
  return page.evaluate(async kind => {
    window.reassert?.();
    const live = pcs.filter(pc => pc.connectionState !== 'closed');
    // Chromium omits trackIdentifier on outbound-rtp, so a sender stream is identified through the
    // mid that getTransceivers reports once negotiation has assigned it.
    const midToTrack = {};
    for (const pc of live) {
      for (const tc of pc.getTransceivers()) {
        if (tc.mid == null) continue;
        const entry = midToTrack[tc.mid] ?? (midToTrack[tc.mid] = {});
        if (tc.sender?.track?.kind === 'audio') entry.send = tc.sender.track.id;
        if (tc.receiver?.track?.kind === 'audio') entry.recv = tc.receiver.track.id;
      }
    }
    const reports = await Promise.all(live.map(pc => pc.getStats()));
    return {
      reports: reports.flatMap(report => [...report.values()]
        .filter(entry => entry.kind === 'audio' && entry.type === kind)
        .map(entry => ({
          track: (kind === 'outbound-rtp' ? midToTrack[entry.mid]?.send : midToTrack[entry.mid]?.recv)
            ?? entry.trackIdentifier,
          ssrc: entry.ssrc, mid: entry.mid,
          packets: (kind === 'inbound-rtp' ? entry.packetsReceived : entry.packetsSent) ?? 0,
          samples: entry.totalSamplesReceived ?? 0,
          codec: report.get(entry.codecId)?.mimeType, payloadType: report.get(entry.codecId)?.payloadType,
        }))),
      subscribed: [...room.remoteParticipants.values()].flatMap(participant =>
        [...participant.audioTrackPublications.values()].filter(entry => entry.isSubscribed)
          .map(entry => ({ name: entry.trackName, id: entry.track.mediaStreamTrack.id }))),
      transports: reports.flat().map(report => [...report.values()])
        .flatMap(entries => entries.filter(entry => entry.type === 'transport')
          .map(entry => ({ dtls: entry.dtlsCipher ?? null, srtp: entry.srtpCipher ?? null,
            packetsSent: entry.packetsSent ?? null, packetsReceived: entry.packetsReceived ?? null }))),
      observed: {
        frames: window.frames.splice(0, window.frames.length), hooks: [...window.hooks],
        attachments: window.attachments, workerErrors: window.workerErrors,
      },
    };
  }, direction);
}

function group(frames) {
  const counts = {};
  for (const row of frames) counts[`${row.origin}:${row.label}`] = (counts[`${row.origin}:${row.label}`] ?? 0) + 1;
  return counts;
}

function wireFor(proxy, direction, ssrc, since) {
  return (proxy.rawRows?.[direction] ?? []).filter(p => p.ssrc === ssrc && p.t >= since)
    .map(({ t, seq, ts, pt, marker, len, cc, padBit, x, extProfile, extWords, headerBytes, padLen, extHead }) =>
      ({ t, seq, ts, pt, marker, len, cc, padBit, x, extProfile, extWords, headerBytes, padLen, extHead }));
}

function summarise(frames, wire, redPts) {
  const byTs = new Map();
  for (const packet of wire) if (!byTs.has(packet.ts)) byTs.set(packet.ts, packet);
  const rows = frames.map(frame => {
    const packet = byTs.get(frame.ts);
    // Only a payload type negotiated as audio/red may be read as an RFC 2198 chain. A plain Opus
    // packet whose TOC happens to have the F bit set would otherwise parse into a phantom chain.
    const isRed = packet ? redPts.has(String(packet.pt)) : false;
    const blocks = isRed ? frame.chainBlocks : null;
    const opus = isRed ? frame.opusAtChain : frame.opusAtZero;
    return {
      ...frame, isRed, blocks, opus,
      wirePt: packet?.pt ?? null, wireSeq: packet?.seq ?? null, wireLen: packet?.len ?? null,
      extIds: packet?.extHead ? Array.from(packet.extHead) : null,
      headerBytes: packet?.headerBytes ?? null, extWords: packet?.extWords ?? null,
      extForm: packet ? `${packet.x}:${packet.extProfile ?? '-'}:${packet.cc}:${packet.padBit}` : null,
      tail: packet ? packet.len - packet.headerBytes - packet.padLen - frame.byteLength : null,
      headerOverhead: packet ? packet.len - frame.byteLength : null,
      blockPts: blocks?.map(block => block.pt) ?? null,
      blockOffsets: blocks?.map(block => block.timestampOffset) ?? null,
      blockLengths: blocks?.map(block => block.length) ?? null,
      blockCount: packet ? (isRed ? blocks?.length ?? null : 1) : null,
    };
  });
  const matched = rows.filter(row => row.wirePt !== null);
  const counted = matched.filter(row => row.blockCount !== null);
  const overhead = {};
  for (const row of matched) overhead[row.headerOverhead] = (overhead[row.headerOverhead] ?? 0) + 1;
  const chains = {};
  for (const row of counted) {
    const key = `${row.wirePt}:${row.blockPts?.join('+') ?? 'single'}`
      + `:${row.blockOffsets?.join('/') ?? '0'}:${row.blockLengths?.join('/') ?? row.byteLength}`;
    chains[key] = (chains[key] ?? 0) + 1;
  }
  // The chain shape without block lengths: the encoder's per-frame byte count tracks the audio
  // content, so only the structure is a stable cross-run comparison key.
  const shapes = new Set();
  const shapeTable = {};
  const primaryLengths = {};
  for (const row of counted) {
    const shape = `${row.wirePt}:${row.blockPts?.join('+') ?? 'single'}:${row.blockOffsets?.join('/') ?? '0'}`;
    shapes.add(shape);
    shapeTable[shape] = (shapeTable[shape] ?? 0) + 1;
    const length = row.blockLengths?.at(-1) ?? row.byteLength;
    primaryLengths[length] = (primaryLengths[length] ?? 0) + 1;
  }
  const extTable = {};
  for (const row of matched) extTable[row.extForm ?? '(none)'] = (extTable[row.extForm ?? '(none)'] ?? 0) + 1;
  const tailTable = {};
  for (const row of matched) tailTable[row.tail] = (tailTable[row.tail] ?? 0) + 1;
  const blockCountTable = {};
  for (const row of counted) blockCountTable[row.blockCount] = (blockCountTable[row.blockCount] ?? 0) + 1;
  const dominant = (table, total) => Object.entries(table)
    .filter(([, count]) => count >= Math.max(1, Math.ceil(total / 20)))
    .map(([key]) => Number(key)).sort((a, b) => a - b);
  const modal = table => Number(Object.entries(table).sort((a, b) => b[1] - a[1])[0]?.[0] ?? -1);
  // Rare shapes (an occasional single-block frame at the head of a stream) are real but not stable
  // across runs, so the parity comparison uses the shapes that carry at least 5% of the leg.
  const dominantShapes = Object.entries(shapeTable)
    .filter(([, count]) => count >= Math.max(1, Math.ceil(counted.length / 20)))
    .map(([shape]) => shape).sort();
  return {
    frames: rows.length,
    matchedInWire: matched.length,
    wirePackets: wire.length,
    redRows: matched.filter(row => row.isRed).length,
    wirePts: [...new Set(wire.map(packet => packet.pt))].sort((a, b) => a - b),
    redWirePts: [...new Set(matched.filter(row => row.isRed).map(row => row.wirePt))].sort((a, b) => a - b),
    blockCounts: [...new Set(counted.map(row => row.blockCount))].sort((a, b) => a - b),
    blockCountTable,
    dominantBlockCounts: dominant(blockCountTable, counted.length),
    extTable,
    extForms: Object.keys(extTable).sort(),
    tailTable,
    tailMin: Math.min(...matched.map(row => row.tail).filter(value => Number.isFinite(value))),
    dominantTail: dominant(tailTable, matched.length),
    headerSample: matched.slice(0, 3).map(row => ({
      seq: row.wireSeq, len: row.wireLen, byteLength: row.byteLength, headerBytes: row.headerBytes,
      extWords: row.extWords, extIds: row.extIds, tail: row.tail, form: row.extForm,
    })),
    headerOverhead: overhead,
    chains,
    chainShapes: [...shapes].sort(),
    dominantShapes,
    primaryLengthMode: modal(primaryLengths),
    overheadMode: modal(overhead),
    parseErrors: matched.filter(row => row.isRed && row.chainError).length,
    phantomChains: matched.filter(row => !row.isRed && row.chainBlocks !== null).length,
    tocValidPlain: matched.filter(row => !row.isRed && row.opus?.valid === true).length,
    tocValidChainPrimary: matched.filter(row => row.isRed && row.opus?.valid === true).length,
    unmatchedTimestamps: rows.filter(row => row.wirePt === null).length,
    first: matched.slice(0, 6).map(row => ({
      wireSeq: row.wireSeq, wirePt: row.wirePt, ts: row.ts, byteLength: row.byteLength, wireLen: row.wireLen,
      chainPrimaryOffset: row.chainPrimaryOffset, blockCount: row.blockCount, blockPts: row.blockPts,
      blockOffsets: row.blockOffsets, blockLengths: row.blockLengths, head: row.head, opus: row.opus,
      digest: row.digest, chainError: row.chainError, tail: row.tail,
      headerBytes: row.headerBytes, extWords: row.extWords, extIds: row.extIds, form: row.extForm,
    })),
    distinctPayloadBytes: [...new Set(matched.map(row => row.byteLength))].sort((a, b) => a - b).slice(0, 12),
    ctor: [...new Set(rows.map(row => row.ctor))],
    tsField: [...new Set(rows.map(row => row.rtpTs !== null ? 'rtpTimestamp' : 'timestamp'))],
    digests: matched.map(row => [row.ts, row.digest, row.primaryDigest]),
  };
}

const subscriptionMap = (subscriber) =>
  Object.fromEntries(subscriber.subscribed.map(entry => [entry.name, entry.id]));

async function buildLegs(trackNames, ctx) {
  const { publisher, subscriber, published, subscribed, redPts } = ctx;
  const legs = {};
  for (const trackName of trackNames) {
    const publisherStream = publisher.reports.find(entry => entry.track === published[trackName]);
    const subscriberStream = subscriber.reports.find(entry => entry.track === subscribed[trackName]);
    assert.ok(publisherStream?.ssrc, `no outbound-rtp entry for the ${trackName} track: `
      + `published=${JSON.stringify(published)} mid=${JSON.stringify(publisher.reports.map(r => [r.mid, r.ssrc, r.track]))} `
      + `attachments=${JSON.stringify(subscriber.observed.attachments)}`);
    assert.ok(subscriberStream?.ssrc, `no inbound-rtp entry for the ${trackName} track: `
      + `subscribed=${JSON.stringify(subscribed)} mid=${JSON.stringify(subscriber.reports.map(r => [r.mid, r.ssrc, r.track]))} `
      + `attachments=${JSON.stringify(subscriber.observed.attachments)}`);
    const upWire = wireFor(ctx.publisherProxy, 'up', publisherStream.ssrc, ctx.upSince);
    const downWire = wireFor(ctx.subscriberProxy, 'down', subscriberStream.ssrc, ctx.downSince);
    const upFrames = publisher.observed.frames.filter(row => row.label === published[trackName]);
    const downFrames = subscriber.observed.frames.filter(row => row.label === subscribed[trackName]);
    // A capture with no payloads is not evidence of a single-block chain: fail the leg instead.
    assert.ok(upFrames.length >= 15,
      `only ${upFrames.length} sender-transform frames for ${trackName}, `
      + `published=${JSON.stringify(published)} subscribed=${JSON.stringify(subscribed)} `
      + `upRows=${JSON.stringify(group(publisher.observed.frames))} `
      + `attachments=${JSON.stringify(publisher.observed.attachments)} hooks=${JSON.stringify(publisher.observed.hooks)} `
      + `workerErrors=${JSON.stringify(publisher.observed.workerErrors)}`);
    assert.ok(downFrames.length >= 15,
      `only ${downFrames.length} receiver-transform frames for ${trackName} of ${subscriber.observed.frames.length} total, `
      + `downRows=${JSON.stringify(group(subscriber.observed.frames))} `
      + `attachments=${JSON.stringify(subscriber.observed.attachments)} hooks=${JSON.stringify(subscriber.observed.hooks)} `
      + `workerErrors=${JSON.stringify(subscriber.observed.workerErrors)}`);
    const up = summarise(upFrames, upWire, redPts);
    const down = summarise(downFrames, downWire, redPts);
    const upDigest = new Map(up.digests.map(([ts, whole, primary]) => [ts, [whole, primary]]));
    const shared = down.digests.filter(([ts]) => upDigest.has(ts));
    const identicalWhole = shared.filter(([ts, whole]) => upDigest.get(ts)[0] === whole).length;
    legs[trackName] = {
      publisherStream, subscriberStream, up, down,
      crossLeg: {
        sharedTimestamps: shared.length,
        identicalWholePayloads: identicalWhole,
        identicalPrimaryPayloads: shared.filter(([ts, , primary]) => upDigest.get(ts)[1] === primary).length,
        // Verbatim forwarding is only a conclusion when the same RTP timestamp carries the same
        // bytes end to end; a rebuild changes at least the redundant-block framing.
        forwardMode: shared.length < 15 ? 'undetermined'
          : identicalWhole === shared.length ? 'verbatim'
            : identicalWhole === 0 ? 'rebuilt' : 'mixed',
      },
    };
  }
  return legs;
}

const browser = await chromium.launch({
  channel: process.env.BROWSER_CHANNEL || 'chromium', headless: true,
  args: ['--autoplay-policy=no-user-gesture-required'],
});
const name = `red-egress-${Date.now()}`;
const api = new RoomServiceClient(url.replace(/^ws/, 'http'), 'devkey', 'secret');
const proxies = [];
const pages = [];
let probeExit = 0;
let receipt;
try {
  const publisherProxy = await new LossyUdpProxy({ seed: 11, upstreamPort: turnPort }).start();
  const subscriberProxy = await new LossyUdpProxy({ seed: 12, upstreamPort: turnPort }).start();
  proxies.push(publisherProxy, subscriberProxy);
  const publisherPage = await browser.newPage();
  const subscriberPage = await browser.newPage();
  pages.push(publisherPage, subscriberPage);
  // Observation is on for both pages: the publisher leg needs the sender transform, attached before
  // negotiation, to record the first frame of each published track.
  await join(publisherPage, publisherProxy, name, 'publisher', 'publisher');
  await publishBoth(publisherPage);
  await sleep(2500);
  const joinedAt = performance.now();
  await join(subscriberPage, subscriberProxy, name, 'subscriber', 'subscriber');
  await sleep(2500);
  const [publisher, subscriber, publisherSdp, subscriberSdp, publisherExt, subscriberExt] = await Promise.all([
    stats(publisherPage, 'outbound-rtp'), stats(subscriberPage, 'inbound-rtp'),
    sdpFacts(publisherPage), sdpFacts(subscriberPage),
    extFacts(publisherPage), extFacts(subscriberPage),
  ]);
  const redPts = new Set(Object.entries({ ...publisherSdp, ...subscriberSdp })
    .filter(([, codec]) => codec.startsWith('red/')).map(([pt]) => pt));
  assert.ok(redPts.size > 0, `no audio/red payload type in the negotiated SDP: ${JSON.stringify(subscriberSdp)}`);
  const published = await publisherPage.evaluate(() => ({ ...window.publishedIds }));
  const legs = {
    // Mid-join: the subscriber enters a room whose tracks already flow, so the egress chain is
    // observed with uplink history already behind it.
    midJoin: await buildLegs(['red', 'plain'], {
      publisher, subscriber,
      published, subscribed: subscriptionMap(subscriber),
      redPts, publisherProxy, subscriberProxy, upSince: 0, downSince: joinedAt,
    }),
  };
  // First frame: the subscriber is already present when the publisher adds a track, so the very
  // first egress packet of that stream is inside the capture window.
  const lateAt = performance.now();
  await publishTracks(publisherPage, [['late', true, 880]]);
  await sleep(2500);
  const [latePublisher, lateSubscriber] = await Promise.all([
    stats(publisherPage, 'outbound-rtp'), stats(subscriberPage, 'inbound-rtp'),
  ]);
  legs.firstFrame = await buildLegs(['late'], {
    publisher: latePublisher, subscriber: lateSubscriber,
    published: await publisherPage.evaluate(() => ({ ...window.publishedIds })),
    subscribed: subscriptionMap(lateSubscriber),
    redPts, publisherProxy, subscriberProxy, upSince: lateAt, downSince: lateAt,
  });
  const audioAttachments = [...publisher.observed.attachments, ...subscriber.observed.attachments]
    .filter(entry => entry.kind === 'audio');
  assert.ok(audioAttachments.length >= 4 && audioAttachments.every(entry => entry.attached),
    `insertable-stream attachment failed: ${JSON.stringify(audioAttachments)}`);
  receipt = {
    sdkSha256: sha(sdk), workerSha256: sha(worker), lockSha256: sha(lock),
    sdkVersion: JSON.parse(lock).packages['node_modules/livekit-client'].version,
    browser: browser.version(), activeRed, redPts: [...redPts].sort(),
    publisherSdp, subscriberSdp, publisherExt, subscriberExt, audioAttachments,
    publisherTransports: publisher.transports, subscriberTransports: subscriber.transports,
    publisherHooks: publisher.observed.hooks, subscriberHooks: subscriber.observed.hooks,
    workerErrors: [...publisher.observed.workerErrors, ...subscriber.observed.workerErrors],
    legs,
  };
} catch (error) {
  probeExit = 1;
  console.error(String(error?.stack ?? error));
  receipt = { error: String(error?.message ?? error), probeExit };
} finally {
  await Promise.allSettled(pages.map(page => page.evaluate(async () => {
    try { await window.room?.disconnect(); } catch { /* already closed */ }
  })));
  await Promise.all(pages.map(page => page.close()));
  await Promise.all(proxies.map(proxy => proxy.close()));
  try { await api.deleteRoom(name); } catch { /* server already gone */ }
}
console.log(JSON.stringify(receipt, null, 2));
process.exit(probeExit);
