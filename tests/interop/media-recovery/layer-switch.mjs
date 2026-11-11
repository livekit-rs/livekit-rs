import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';

const settledFrames = 12;
const minimumLossWindowMs = 3000;
const packetsPerDrop = 20;
const phases = [
  { name: 'initial-high', quality: 'high', impaired: false },
  { name: 'low-with-loss', quality: 'low', impaired: true },
  { name: 'high-with-loss', quality: 'high', impaired: true },
];

function activeEncoders(report, codec) {
  const encoders = report.publisher.filter(stream =>
    stream.codec === codec && stream.active && stream.frames > 0
  );
  if (codec === 'video/VP9') {
    assert.equal(encoders.length, 1, 'one actual VP9 encoder');
    assert.equal(encoders[0].rid, null, 'VP9 SVC has no simulcast RID');
    assert.equal(encoders[0].scalabilityMode, 'L3T3_KEY', 'exact VP9 source encoding mode');
  } else {
    assert.equal(encoders.length, 3, 'three actual VP8 encoders');
    assert.deepEqual(encoders.map(stream => stream.rid).sort(), ['f', 'h', 'q']);
    assert.equal(new Set(encoders.map(stream => stream.ssrc)).size, 3);
  }
  return encoders;
}

function matchesTarget(frame, initial, quality) {
  if (frame.code === null || frame.code < 0) return false;
  if (!initial) return frame.w >= 640;
  if (quality === 'low') return frame.w <= initial.w / 2 && frame.h <= initial.h / 2;
  return frame.w === initial.w && frame.h === initial.h;
}

function dropPrimaryVideoPacket(payloadTypes) {
  const counts = new Map();
  return ({ pt, ssrc }) => {
    if (!payloadTypes.has(pt)) return false;
    const count = (counts.get(ssrc) ?? 0) + 1;
    counts.set(ssrc, count);
    return count % packetsPerDrop === 0;
  };
}

async function waitForDecodedLayer(subscriber, proxy, since, from, initial, phase) {
  let decoded = [];
  let settled = [];
  while (proxy.now() < from + 14000) {
    decoded = await subscriber.evaluate(at => rec.decodedSince(at), since);
    settled = decoded.slice(-settledFrames);
    const distinct = new Set(settled.map(frame => frame.code)).size;
    if (settled.length === settledFrames && distinct >= 10
        && settled.every(frame => matchesTarget(frame, initial, phase.quality))
        && (!phase.impaired || proxy.now() - from >= minimumLossWindowMs)) {
      return { decoded, settled, stable: true };
    }
    await sleep(100);
  }
  return { decoded, settled, stable: false };
}

// The publisher uplink stays unshaped. Loss on the subscriber downlink
// continues across both public SDK quality requests.
export async function exerciseLayerSwitch({ profileName, subscriber, proxy, videoStats, codeFacts, report }) {
  const codec = profileName === 'vp9-svc' ? 'video/VP9' : 'video/VP8';
  let initial;
  let publisherWidth;
  for (const phase of phases) {
    const before = await videoStats();
    const payloadTypes = new Set(before.subscriber
      .filter(stream => stream.codec === codec).map(stream => stream.pt));
    assert.ok(payloadTypes.size > 0, `${phase.name}: negotiated primary video payload type`);
    if (phase.impaired) {
      proxy.set('down', { dropRtp: dropPrimaryVideoPacket(payloadTypes) });
    }
    const from = proxy.now();
    const request = await subscriber.evaluate(quality => rec.setVideoQuality(quality), phase.quality);
    const { decoded, settled, stable } = await waitForDecodedLayer(
      subscriber, proxy, request.at, from, initial, phase
    );
    const after = await videoStats();
    const to = proxy.now();
    const streams = [...new Map([...before.subscriber, ...after.subscriber]
      .map(stream => [`${stream.ssrc}:${stream.pt}`, stream])).values()];
    const wire = proxy.lossRtp('down', streams, from, to).filter(stream => stream.codec === codec);
    const last = settled.at(-1);
    const pictures = codeFacts(decoded);
    report.stages.push({
      ...phase, request, packetsPerDrop: phase.impaired ? packetsPerDrop : null,
      size: last ? `${last.w}x${last.h}` : null,
      elapsedMs: Math.round(to - from), pictures, before, after, settled, wire,
    });

    assert.ok(stable, `${phase.name}: no stable decoded markers at the requested spatial layer`);
    assert.ok(settled.every(frame => frame.w === last.w && frame.h === last.h),
      `${phase.name}: unstable decoded layer`);
    assert.equal(pictures.invalid, 0, `${phase.name}: corrupted decoded frame marker`);
    assert.equal(pictures.regressions, 0, `${phase.name}: frame marker regressed`);
    assert.equal(request.sid, report.stages[0].request.sid,
      `${phase.name}: quality switch replaced the actual publication`);
    assert.ok(wire.some(stream => stream.forwarded > 0
      && (phase.impaired ? stream.dropped.length > 0 : stream.dropped.length === 0)),
    `${phase.name}: no expected primary RTP loss outcome`);
    assert.ok(wire.every(stream => stream.ingress === stream.forwarded + stream.dropped.length),
      `${phase.name}: incomplete primary RTP accounting`);
    if (phase.impaired) {
      assert.ok(wire.every(stream => stream.dropped.length === Math.floor(stream.ingress / packetsPerDrop)),
        `${phase.name}: primary RTP drop cadence changed`);
    }

    const start = activeEncoders(before, codec);
    const end = activeEncoders(after, codec);
    if (codec === 'video/VP8') {
      const target = end.find(stream => stream.rid === (phase.quality === 'low' ? 'q' : 'f'));
      assert.deepEqual([last.w, last.h], [target.w, target.h],
        `${phase.name}: decoded dimensions must match the requested VP8 encoding`);
    } else if (phase.quality === 'high') {
      assert.deepEqual([last.w, last.h], [end[0].sourceW, end[0].sourceH],
        `${phase.name}: highest VP9 layer must decode at the actual source dimensions`);
    }
    assert.ok(start.every(stream => end.some(next => next.ssrc === stream.ssrc
      && next.pt === stream.pt && next.frames > stream.frames)),
    `${phase.name}: active source RTP did not progress`);
    publisherWidth ??= Math.max(...start.map(stream => stream.w));
    assert.equal(Math.max(...end.map(stream => stream.w)), publisherWidth,
      `${phase.name}: publisher resized during the subscriber control`);
    initial ??= last;
  }
  proxy.clean();
}
