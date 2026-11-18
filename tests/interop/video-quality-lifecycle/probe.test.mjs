import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { Script, createContext } from 'node:vm';
import { CASES, CASE_IDS, chooseWindows, deriveObserved, inspectDependencies, validateInput, validateReport } from './probe.mjs';

const DIMS = { low: [320, 180], medium: [640, 360], high: [1280, 720] };
const RIDS = ['q', 'h', 'f'];
const SID = 'TR_original', PUB = 'PA_publisher';
const checksum = n => ((n * 37 + 0x5b) ^ (n >> 8)) & 255;
const dependencies = await inspectDependencies();
const frame = (code, at, dims) => ({ code, at, callbackAt: at, valid: true, checksum: checksum(code),
  w: dims[0], h: dims[1], presentedFrames: code, mediaTime: code / 15, rtpTimestamp: code * 6000, receiveTime: at - 1 });
function evidence(spec, caseIndex) {
  const sid = spec.id === 'republish' ? 'TR_republished' : SID;
  const at = 100000 + caseIndex * 10000, base = 100 + caseIndex * 150;
  const demand = Math.max(0, ...['a', 'b'].map(p => spec.enabled[p] ? ['low', 'medium', 'high'].indexOf(spec.qualities[p]) + 1 : 0));
  return Array.from({ length: 5 }, (_, i) => {
    const time = at + i * 400, source = base + i * 6;
    const common = p => ({ at: time, participant: ({ pub: PUB, a: 'PA_a', b: 'PA_b' })[p],
      connection: 'connected', errors: [], events: [], readers: [], sourceFrame: p === 'pub' ? source : 0,
      peers: ['pub', 'a', 'b'].filter(k => k !== p).map(k => ({ sid: ({ pub: PUB, a: 'PA_a', b: 'PA_b' })[k],
        identity: ({ pub: 'publisher', a: 'sub-a', b: 'sub-b' })[k] })) });
    const pub = common('pub');
    pub.local = { sid, published: spec.published, paused: spec.paused, senderPresent: spec.published,
      senderTrackPresent: spec.published ? !spec.paused : null, source: { width: 1280, height: 720, frameRate: 15 },
      encodings: spec.published ? RIDS.map((rid, index) => ({ rid, active: index < demand, scaleResolutionDownBy: [4, 2, 1][index],
        maxFramerate: 15, maxBitrate: [180000, 600000, 1800000][index] })) : null,
      stats: spec.published ? RIDS.map((rid, index) => {
        const moving = !spec.paused && index < demand;
        return { type: 'outbound-rtp', codec: 'video/VP8', timestamp: time, rid, id: 'outbound-' + rid, active: index < demand,
          bytesSent: 10000 + (moving ? i * 1000 : 0), framesEncoded: 100 + (moving ? i * 6 : 0),
          frameWidth: [320, 640, 1280][index], frameHeight: [180, 360, 720][index] };
      }) : null };
    pub.sourceCodes = Array.from({ length: 6 }, (_, j) => ({ at: time - (5 - j) * 60 - 1, code: source - 5 + j, sequence: source - 5 + j }));
    const result = { at: time, errors: [], pub };
    for (const p of ['a', 'b']) {
      const s = common(p), moving = spec.published && !spec.paused && spec.enabled[p], dims = DIMS[spec.qualities[p]];
      s.requested = { quality: spec.qualities[p], enabled: spec.enabled[p] };
      s.publications = spec.published ? [{ sid, owner: PUB, ownerIdentity: 'publisher', hasTrack: true,
        subscribed: true, quality: spec.qualities[p], enabled: spec.enabled[p] }] : [];
      const frames = moving && i > 0 ? Array.from({ length: 6 }, (_, j) => frame(source - 5 + j, time - (5 - j) * 60 - 1, dims)) : [];
      s.readers = [{ sid, owner: PUB, generation: spec.id === 'republish' ? 2 : 1,
        callbacks: 50 + (moving ? i * 6 : 0), valid: 50 + (moving ? i * 6 : 0), invalid: 0,
        last: moving ? frame(source, time - 1, dims) : frame(base, at - 1, dims), frames,
        track: { enabled: spec.published, readyState: 'live', attached: true },
        stats: spec.published ? [{ id: 'inbound-video', type: 'inbound-rtp', codec: 'video/VP8', timestamp: time,
          bytesReceived: 10000 + (moving ? i * 1000 : 0), framesDecoded: 100 + (moving ? i * 6 : 0),
          frameWidth: dims[0], frameHeight: dims[1] }] : null }];
      if (!spec.published && i === 0) s.events = [{ event: 'trackUnsubscribed', sid: SID, owner: PUB, at: time }];
      result[p] = s;
    }
    return result;
  });
}
function validReport() {
  return { status: 'PASS_SCOPED', side: 'go', case_count: 12, setup_samples: [],
    cases: CASES.map((spec, index) => {
      const samples = evidence(spec, index), windows = [[0, 2], [2, 4]];
      const command = ({ 'publisher-pause': 'pause', 'publisher-resume': 'resume', unpublish: 'unpublish', republish: 'publish' })[spec.id];
      const actions = [...(command ? [['pub', command]] : []), ['a', 'request'], ['b', 'request']]
        .map(([peer, name]) => ({ peer, name, at: samples[0].at - 1, completed: true }));
      return { id: spec.id, status: 'PASS', samples, windows, actions, action_completed_at: samples[0].at - 1,
        observed: deriveObserved(spec, samples, windows, SID) };
    }),
    dependencies: { before: structuredClone(dependencies), after: structuredClone(dependencies),
      chromium_sha256: 'a'.repeat(64), fixture_sha256: { client: 'b'.repeat(64), probe: 'c'.repeat(64) } },
    cleanup_errors: [], browser_cleanup: { contextsBefore: 1, contextsAfter: 0, failures: [],
      exit: { code: 0, signal: null, observedBy: 'child-exit-event' } },
    host_close: { closed: true, error: null }, room_cleanup: { deleted: true, status: 200 },
    diagnostics: { console: {}, page_errors: { count: 0, entries: [] }, fixture_errors: 0 } };
}
function mutate(mutator, recompute = false) {
  const report = validReport(); mutator(report);
  if (recompute) report.cases.forEach((row, i) => { row.observed = deriveObserved(CASES[i], row.samples, row.windows, SID); });
  assert.equal(validateReport(report).ok, false);
}

test('synthetic contract witnesses all 12 cases, not a browser acceptance result', () => {
  const report = validReport();
  assert.deepEqual(validateReport(report), { ok: true, errors: [] });
  assert.deepEqual(CASE_IDS, ['low-high', 'high-low', 'medium-low', 'low-low', 'a-paused-b-low', 'a-low-b-paused',
    'both-paused', 'resume-high-low', 'publisher-pause', 'publisher-resume', 'unpublish', 'republish']);
  assert.deepEqual(report.cases[2].observed.publisher_active_rids, ['q', 'h']);
  assert.deepEqual(report.cases[3].observed.publisher_active_rids, ['q']);
  assert.deepEqual(report.cases[6].observed.publisher_active_rids, []);
  assert.equal(report.cases[8].observed.publisher_active_rids, null);
  assert.deepEqual(report.cases[8].samples[0].pub.local.encodings.filter(e => e.active).map(e => e.rid), ['q', 'h', 'f']);
  assert.deepEqual(report.cases[10].observed.publisher_active_rids, []);
});
test('missing, duplicated and reordered cases cannot pass', () => {
  mutate(r => r.cases.pop());
  mutate(r => { r.cases[1] = r.cases[0]; });
  mutate(r => { [r.cases[0], r.cases[1]] = [r.cases[1], r.cases[0]]; });
  mutate(r => { r.case_count = 11; });
  mutate(r => { r.cases[9].status = 'NOT_RUN'; });
});
test('false, missing, extra or forged observed fields fail', () => {
  mutate(r => { r.cases[0].observed.contract.receiver_bytes = false; });
  mutate(r => { delete r.cases[0].observed.contract.frame_codes; });
  mutate(r => { r.cases[0].observed.ignored_mismatch = true; });
  mutate(r => { r.cases[2].observed.subscriber_dimensions.a = [320, 180]; });
  mutate(r => { r.cases[2].observed.enabled.a = false; });
});
test('equal-but-wrong dimensions fail even when observed is recomputed', () => {
  mutate(r => r.cases[0].samples.forEach(s => {
    s.b.readers[0].stats[0].frameWidth = 640; s.b.readers[0].stats[0].frameHeight = 360;
    s.b.readers[0].last.w = 640; s.b.readers[0].last.h = 360;
    s.b.readers[0].frames.forEach(f => { f.w = 640; f.h = 360; });
  }), true);
});
test('empty, overlapping, short or pre-action windows fail', () => {
  mutate(r => { r.cases[0].windows = []; }, true);
  mutate(r => { r.cases[0].windows = [[0, 2], [1, 4]]; }, true);
  mutate(r => { r.cases[0].windows = [[0, 1], [1, 2]]; }, true);
  mutate(r => { r.cases[0].action_completed_at = r.cases[0].samples[1].at; });
  mutate(r => { r.cases[0].samples = []; }, true);
  mutate(r => { r.cases[0].samples[2].a.at = r.cases[0].samples[1].a.at; }, true);
  assert.deepEqual(chooseWindows(evidence(CASES[0], 0)), [[0, 2], [2, 4]]);
  assert.deepEqual(chooseWindows(evidence(CASES[0], 0), 100001), []);
});
test('cumulative nonzero byte or decoded counters cannot substitute for progress', () => {
  for (const field of ['bytesReceived', 'framesDecoded']) mutate(r => r.cases[0].samples.forEach(s => { s.a.readers[0].stats[0][field] = 10000; }), true);
  mutate(r => r.cases[0].samples.forEach((s, i) => { s.a.readers[0].stats[0].bytesReceived = 10000 + Math.min(i, 2) * 1000; }), true);
  mutate(r => { r.cases[0].samples[3].a.readers[0].stats[0].framesDecoded = null; }, true);
  mutate(r => { r.cases[0].samples[3].a.readers[0].stats = []; }, true);
  mutate(r => { r.cases[0].samples[3].a.readers[0].stats[0].codec = 'video/H264'; }, true);
});
test('old, invalid, stale and frozen reader frames cannot pass', () => {
  mutate(r => r.cases[0].samples.forEach(s => { s.a.readers[0].sid = 'TR_old'; }), true);
  mutate(r => r.cases[0].samples.forEach(s => {
    s.a.readers[0].frames.forEach(f => { f.code -= 90; f.checksum = checksum(f.code); });
    s.a.readers[0].last.code -= 90;
  }), true);
  mutate(r => r.cases[0].samples.forEach(s => s.a.readers[0].frames.forEach(f => { f.code = 50; f.checksum = checksum(50); })), true);
  mutate(r => { r.cases[0].samples[1].a.readers[0].frames[0].valid = false; }, true);
  mutate(r => { r.cases[0].samples[1].a.readers[0].frames[0].checksum = -1; }, true);
  mutate(r => { r.cases[0].samples[1].a.readers[0].frames[0].at -= 2000; }, true);
  mutate(r => r.cases[0].samples.forEach(s => s.a.readers[0].frames.forEach(f => { f.presentedFrames = 100; })), true);
});
test('paused subscribers must stop forwarding while the bystander keeps growing', () => {
  mutate(r => r.cases[4].samples.forEach((s, i) => { s.a.readers[0].stats[0].bytesReceived += i * 100; }), true);
  mutate(r => r.cases[4].samples.forEach((s, i) => { s.a.readers[0].stats[0].framesDecoded += i; }), true);
  mutate(r => { r.cases[4].samples[2].a.readers[0].frames.push(frame(700, 140799, [320, 180])); }, true);
  mutate(r => r.cases[4].samples.forEach(s => { s.b.readers[0].stats[0].framesDecoded = 100; }), true);
  mutate(r => r.cases[5].samples.forEach(s => { s.a.readers[0].stats[0].bytesReceived = 10000; }), true);
});
test('sender RID identity, active set and each RID delta are independently checked', () => {
  mutate(r => r.cases[0].samples.forEach(s => { s.pub.local.encodings[0].rid = null; }), true);
  mutate(r => r.cases[2].samples.forEach(s => { s.pub.local.encodings[2].active = true; }), true);
  mutate(r => r.cases[3].samples.forEach(s => { s.pub.local.encodings[1].active = null; }), true);
  mutate(r => r.cases[0].samples.forEach(s => { s.pub.local.stats[0].framesEncoded = 100; }), true);
  mutate(r => r.cases[3].samples.forEach((s, i) => { s.pub.local.stats[1].bytesSent += i * 100; }), true);
  mutate(r => r.cases[6].samples.forEach(s => { s.pub.local.encodings[0].active = true; }), true);
  mutate(r => r.cases[0].samples.forEach(s => { s.pub.local.encodings[0].scaleResolutionDownBy = 2; }), true);
});
test('publisher pause uses null sender track, not disabled encodings; resume must grow', () => {
  mutate(r => r.cases[8].samples.forEach(s => { s.pub.local.senderTrackPresent = true; }), true);
  mutate(r => r.cases[8].samples.forEach((s, i) => { s.b.readers[0].stats[0].bytesReceived += i * 10; }), true);
  mutate(r => r.cases[9].samples.forEach(s => { s.a.readers[0].stats[0].framesDecoded = 100; }), true);
});
test('track SID and publisher owner identity must agree across all peers', () => {
  mutate(r => r.cases[0].samples.forEach(s => { s.a.publications[0].owner = 'PA_wrong'; }), true);
  mutate(r => r.cases[0].samples.forEach(s => { s.a.publications[0].sid = 'TR_wrong'; }), true);
  mutate(r => r.cases[0].samples.forEach(s => { s.a.readers[0].owner = 'PA_wrong'; }), true);
  mutate(r => r.cases[0].samples.forEach(s => { s.a.peers = []; }), true);
});
test('unpublish needs both the event and an unreadable remote track; republish needs a new SID', () => {
  mutate(r => r.cases[10].samples.forEach(s => { s.a.events = []; }), true);
  mutate(r => r.cases[10].samples.forEach(s => { s.a.readers[0].track.enabled = true; }), true);
  mutate(r => r.cases[11].samples.forEach(s => {
    s.pub.local.sid = SID;
    for (const p of ['a', 'b']) { s[p].publications[0].sid = SID; s[p].readers[0].sid = SID; }
  }), true);
});
test('unhealthy or synthetic cleanup never passes', () => {
  mutate(r => { r.cleanup_errors.push({ check: 'room-delete' }); });
  mutate(r => { r.browser_cleanup.exit = null; });
  mutate(r => { r.browser_cleanup.exit = { timeout: true }; });
  mutate(r => { r.browser_cleanup.exit.code = 1; });
  mutate(r => { r.browser_cleanup.exit.timeout = true; });
  mutate(r => { r.browser_cleanup.exit.signal = 'SIGTERM'; });
  mutate(r => { r.browser_cleanup.exit.observedBy = 'assumed'; });
  mutate(r => { r.browser_cleanup.contextsAfter = 1; });
  mutate(r => { r.browser_cleanup.failures.push('owned-browser-server-close'); });
  mutate(r => { r.host_close.closed = false; });
  mutate(r => { r.room_cleanup.deleted = false; });
});
test('runtime errors and mutated dependency identity cannot be hidden', () => {
  mutate(r => { r.diagnostics.console.error = 1; });
  mutate(r => { r.diagnostics.page_errors.count = 1; });
  mutate(r => { r.diagnostics.page_errors.entries.push({ kind: 'Error' }); });
  mutate(r => { r.failure = { stage: 'low-high', kind: 'runtime', check: 'browser-error' }; });
  mutate(r => { r.cases[0].failure = { stage: 'low-high', kind: 'runtime', check: 'browser-error' }; });
  mutate(r => { r.cases[2].samples[0].a.errors.push({ kind: 'frame-reader' }); }, true);
  mutate(r => { r.dependencies.after.packages.pop(); });
  mutate(r => { r.dependencies.before.packages[0].version = '2.23.1'; });
  mutate(r => { r.dependencies.after.packages[0].entry_sha256 = '0'.repeat(64); });
  mutate(r => { r.dependencies.after.package_lock_sha256 = '0'.repeat(64); });
});
test('input accepts only an explicit literal loopback HTTP target and absolute Chromium path', () => {
  const input = { side: 'go', url: 'http://127.0.0.1:7880', key: 'fixture', secret: 'fixture', chromium: '/fixture/chromium' };
  assert.deepEqual(validateInput(input), input);
  assert.equal(validateInput({ ...input, url: 'http://127.0.0.1:80' }).url, 'http://127.0.0.1');
  for (const url of ['http://localhost:7880', 'http://127.0.0.1:7880@evil.test', 'http://127.0.0.1.evil.test:7880',
    'http://127.1:7880', 'http://2130706433:7880', 'http://[::1]:7880', 'http://192.168.1.1:7880',
    'https://127.0.0.1:7880', 'http://127.0.0.1:0', 'http://127.0.0.1:65536', 'http://127.0.0.1:7880/path',
    'http://127.0.0.1:7880?redirect=1']) assert.throws(() => validateInput({ ...input, url }));
  assert.throws(() => validateInput({ ...input, chromium: 'chromium' }));
  assert.throws(() => validateInput({ ...input, side: 'oracle' }));
  assert.throws(() => validateInput({ ...input, worker: '/worker.js' }));
});
test('a resumed stage cannot recycle old evidence or omit its actual commands', () => {
  mutate(r => { r.cases[7].samples = structuredClone(r.cases[1].samples); r.cases[7].action_completed_at = r.cases[1].action_completed_at; }, true);
  mutate(r => { r.cases[8].actions = r.cases[8].actions.slice(1); });
  mutate(r => { r.cases[8].actions[0].completed = false; });
  mutate(r => { r.cases[11].actions[0].name = 'resume'; });
});
test('slow stats queries cannot inflate the actual RTP counter windows', () => {
  for (const peer of ['pub', 'a', 'b']) {
    mutate(r => r.cases[0].samples.forEach((s, i) => {
      const stats = peer === 'pub' ? s.pub.local.stats : s[peer].readers[0].stats;
      stats.forEach(row => { row.timestamp = 100000 + i * 100; });
    }), true);
    mutate(r => r.cases[0].samples.forEach(s => {
      const stats = peer === 'pub' ? s.pub.local.stats : s[peer].readers[0].stats;
      stats.forEach(row => { row.timestamp = null; });
    }), true);
  }
});
test('publisher pause retains raw encoding flags without declaring a demanded active set', () => {
  const report = validReport(), row = report.cases[8];
  row.samples.forEach(s => { s.pub.local.encodings[2].active = false; });
  row.observed = deriveObserved(CASES[8], row.samples, row.windows, SID);
  assert.deepEqual(validateReport(report), { ok: true, errors: [] });
  assert.equal(row.observed.publisher_active_rids, null);
  assert.equal(row.samples[0].pub.local.encodings[2].active, false);
});
test('malformed reports return rejection rather than escaping the validator', () => {
  for (const report of [null, {}, { cases: {} }, { cases: [null] }, { cases: [{ samples: {} }] }]) {
    assert.equal(validateReport(report).ok, false);
  }
});
test('browser snapshots capture clocks and frame cursors before awaiting stats', async () => {
  const html = await readFile(new URL('./client.html', import.meta.url), 'utf8');
  const script = html.match(/<script>\n([\s\S]*?)<\/script>/)[1];
  let clock = 1000, release;
  const pending = new Promise(resolve => { release = resolve; });
  const testReader = { sid: SID, owner: PUB, generation: 1, callbacks: 1, valid: 1, invalid: 0,
    frames: [{ code: 1 }], cursor: 0, last: { code: 1 }, video: { srcObject: null },
    track: { mediaStreamTrack: { enabled: true, readyState: 'live' }, getRTCStatsReport: () => pending } };
  const context = createContext({ window: { addEventListener() {} },
    LivekitClient: { VideoQuality: { LOW: 0, MEDIUM: 1, HIGH: 2 } },
    performance: { timeOrigin: 0, now: () => clock }, MediaStream: class {}, testReader });
  new Script(script).runInContext(context);
  new Script('readers.push(testReader); frame = 1; sourceCodes.push({ code: 1 });').runInContext(context);
  const snapshot = context.window.fixture.snapshot();
  clock = 2000;
  testReader.callbacks++; testReader.valid++; testReader.last = { code: 2 }; testReader.frames.push({ code: 2 });
  new Script('frame = 2; sourceCodes.push({ code: 2 });').runInContext(context);
  release(new Map());
  const first = await snapshot;
  assert.equal(first.at, 1000);
  assert.equal(first.sourceFrame, 1);
  assert.equal(first.readers[0].callbacks, 1);
  assert.equal(first.readers[0].last.code, 1);
  assert.deepEqual(Array.from(first.readers[0].frames, f => f.code), [1]);
  assert.deepEqual(Array.from(first.sourceCodes, f => f.code), [1]);
  const second = await context.window.fixture.snapshot();
  assert.equal(second.at, 2000);
  assert.equal(second.sourceFrame, 2);
  assert.deepEqual(Array.from(second.readers[0].frames, f => f.code), [2]);
  assert.deepEqual(Array.from(second.sourceCodes, f => f.code), [2]);
});
test('browser fixture compiles and does not override transports or mutate sender parameters', async () => {
  const html = await readFile(new URL('./client.html', import.meta.url), 'utf8');
  const script = html.match(/<script>\n([\s\S]*?)<\/script>/)[1];
  assert.doesNotThrow(() => new Script(script, { filename: 'client.html' }));
  assert.doesNotMatch(script, /RTCPeerConnection|WebSocket|setParameters|new Worker|replaceTrack\(/);
  assert.match(script, /adaptiveStream: false, dynacast: true/);
  assert.match(script, /getRTCStatsReport\(/);
  assert.match(script, /requestVideoFrameCallback\(/);
  assert.match(script, /new VideoPreset\(320, 180, 180000, 15\)/);
  assert.match(script, /new VideoPreset\(640, 360, 600000, 15\)/);
});
