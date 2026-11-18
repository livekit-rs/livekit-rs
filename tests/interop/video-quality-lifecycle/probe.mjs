import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createHash, randomUUID } from 'node:crypto';
import { createServer, request } from 'node:http';
import { readFileSync } from 'node:fs';
import { access, open, readFile, realpath, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { closeOwnedBrowser } from '../owned-browser-exit.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const INTEROP = resolve(HERE, '..');
const loader = createRequire(resolve(INTEROP, 'package.json'));
const PIN_PATH = resolve(INTEROP, 'client-browser-capabilities/sdk-pins.json');
const PINS = JSON.parse(readFileSync(PIN_PATH, 'utf8'));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const finite = Number.isFinite;
const same = (a, b) => { try { assert.deepEqual(a, b); return true; } catch { return false; } };
const dimensions = { low: [320, 180], medium: [640, 360], high: [1280, 720] };
const RIDS = ['q', 'h', 'f'];
const SID = /^(PA_|TR_)[A-Za-z0-9_-]+$/;
const HEX = /^[a-f0-9]{64}$/;
const PEERS = ['pub', 'a', 'b'];
const CONTRACT_KEYS = ['two_contiguous_windows', 'connected', 'identities', 'requested', 'publication_lifecycle',
  'reader_identity', 'receiver_dimensions', 'receiver_bytes', 'receiver_frames', 'frame_codes', 'frame_freshness',
  'paused_receivers_stopped', 'bystander_continuity', 'sender_rids', 'sender_activity', 'sender_progress',
  'source_continuity', 'no_runtime_errors'];
export const CASES = [
  ['low-high', 'low', 'high', true, true],
  ['high-low', 'high', 'low', true, true],
  ['medium-low', 'medium', 'low', true, true],
  ['low-low', 'low', 'low', true, true],
  ['a-paused-b-low', 'low', 'low', false, true],
  ['a-low-b-paused', 'low', 'low', true, false],
  ['both-paused', 'low', 'low', false, false],
  ['resume-high-low', 'high', 'low', true, true],
  ['publisher-pause', 'high', 'low', true, true],
  ['publisher-resume', 'high', 'low', true, true],
  ['unpublish', 'high', 'low', true, true],
  ['republish', 'high', 'low', true, true],
].map(([id, a, b, enabledA, enabledB]) => ({ id, qualities: { a, b }, enabled: { a: enabledA, b: enabledB },
  paused: id === 'publisher-pause', published: id !== 'unpublish' }));
export const CASE_IDS = CASES.map(c => c.id);
const demand = spec => !spec.published ? [] : RIDS.slice(0, Math.max(0,
  ...['a', 'b'].map(p => spec.enabled[p] ? ['low', 'medium', 'high'].indexOf(spec.qualities[p]) + 1 : 0)));
const progressing = (spec, peer) => spec.published && !spec.paused && spec.enabled[peer];
const minTime = s => Math.min(...PEERS.map(p => s?.[p]?.at ?? NaN));
const elapsed = (a, b) => Math.min(...PEERS.map(p => b?.[p]?.at - a?.[p]?.at));
const reader = (s, trackSid) => s?.readers?.find(r => r.sid === trackSid);
const delta = (a, b, key) => finite(a?.[key]) && finite(b?.[key]) ? b[key] - a[key] : null;
const rtp = (r, type) => r?.stats?.filter(s => s.type === type && s.codec?.toLowerCase() === 'video/vp8') ?? [];
function inbound(r) {
  const rows = rtp(r, 'inbound-rtp');
  return rows.length === 1 ? rows[0] : null;
}
function monotonic(values, strict = false) {
  return values.length > 1 && values.every(finite) && values.slice(1).every((n, i) => strict ? n > values[i] : n >= values[i]);
}
function statsWindow(rows) {
  const times = rows.map(row => row?.timestamp);
  return monotonic(times, true) && times.at(-1) - times[0] >= 800;
}
function identity(snapshot) {
  if (!PEERS.every(p => SID.test(snapshot?.[p]?.participant))) return false;
  if (new Set(PEERS.map(p => snapshot[p].participant)).size !== 3) return false;
  return PEERS.every(p => same(snapshot[p].peers?.map(r => r.sid).sort(), PEERS.filter(q => q !== p).map(q => snapshot[q].participant).sort()) &&
    same(snapshot[p].peers?.map(r => r.identity).sort(), PEERS.filter(q => q !== p).map(q => ({ pub: 'publisher', a: 'sub-a', b: 'sub-b' })[q]).sort()));
}
export function chooseWindows(samples, after = 0) {
  const end = samples.length - 1;
  const previous = to => {
    for (let i = to - 1; i >= 0; i--) {
      if (minTime(samples[i]) < after) break;
      if (elapsed(samples[i], samples[to]) >= 800) return i;
    }
    return -1;
  };
  const middle = previous(end), start = previous(middle);
  return start >= 0 ? [[start, middle], [middle, end]] : [];
}
export function deriveObserved(spec, samples, windows, originalSid) {
  const contract = Object.fromEntries(CONTRACT_KEYS.map(k => [k, false]));
  const last = samples.at(-1);
  const observed = { requested_qualities: { ...spec.qualities }, enabled: { ...spec.enabled },
    publisher_paused: last?.pub?.local?.paused ?? null, published: last?.pub?.local?.published ?? null,
    subscriber_dimensions: { a: null, b: null }, publisher_active_rids: null, contract };
  const validWindows = windows?.length === 2 && windows.every(w => Array.isArray(w) && w.length === 2 && w.every(Number.isInteger) &&
    w[0] >= 0 && w[1] < samples.length && w[0] < w[1] && elapsed(samples[w[0]], samples[w[1]]) >= 800) && windows[0][1] === windows[1][0];
  if (!validWindows) return observed;
  const [start] = windows[0], end = windows[1][1];
  const selected = samples.slice(start, end + 1), final = samples[end];
  const trackSid = final?.pub?.local?.sid;
  const active = final?.pub?.local?.encodings;
  observed.publisher_paused = final?.pub?.local?.paused ?? null;
  observed.published = final?.pub?.local?.published ?? null;
  observed.publisher_active_rids = spec.paused ? null : active ? RIDS.filter(rid => active.find(e => e.rid === rid)?.active === true) : final?.pub?.local?.senderPresent === false ? [] : null;
  for (const p of ['a', 'b']) {
    if (progressing(spec, p)) {
      const r = inbound(reader(final[p], trackSid));
      observed.subscriber_dimensions[p] = r ? [r.frameWidth, r.frameHeight] : null;
    }
  }
  const ok = (key, value) => { contract[key] = value === true; };
  ok('two_contiguous_windows', PEERS.every(p => monotonic(selected.map(s => s[p]?.at), true)));
  ok('connected', selected.every(s => PEERS.every(p => s[p]?.connection === 'connected')));
  ok('identities', selected.every(identity));
  ok('requested', selected.every(s => ['a', 'b'].every(p => same(s[p]?.requested, { quality: spec.qualities[p], enabled: spec.enabled[p] }) &&
    (!spec.published || s[p].publications?.length === 1 && s[p].publications[0].quality === spec.qualities[p] && s[p].publications[0].enabled === spec.enabled[p]))));
  ok('publication_lifecycle', SID.test(trackSid) && SID.test(originalSid) && selected.every(s => {
    const pub = s.pub?.local;
    if (!pub || pub.sid !== trackSid || pub.published !== spec.published || pub.paused !== spec.paused) return false;
    if (spec.id === 'republish' ? trackSid === originalSid : trackSid !== originalSid) return false;
    return ['a', 'b'].every(p => {
      if (spec.published) return s[p].publications?.length === 1 && s[p].publications[0].sid === trackSid &&
        s[p].publications[0].owner === s.pub.participant && s[p].publications[0].ownerIdentity === 'publisher' &&
        s[p].publications[0].hasTrack === true && s[p].publications[0].subscribed === true;
      const r = reader(s[p], originalSid);
      const unsubscribed = samples.slice(0, end + 1).some(t => t[p]?.events?.some(e => e.event === 'trackUnsubscribed' && e.sid === originalSid && e.owner === s.pub.participant));
      return s[p].publications?.length === 0 && unsubscribed && !!r &&
        (r.track?.readyState === 'ended' || r.track?.enabled === false || r.track?.attached === false);
    });
  }));
  ok('reader_identity', selected.every(s => ['a', 'b'].every(p => {
    const matching = s[p]?.readers?.filter(r => r.sid === trackSid);
    return matching?.length === 1 && matching[0].owner === s.pub.participant &&
      matching[0].generation === reader(final[p], trackSid)?.generation && Number.isInteger(matching[0].generation);
  })));
  ok('receiver_dimensions', ['a', 'b'].every(p => !progressing(spec, p) || selected.every(s => {
    const r = reader(s[p], trackSid), row = inbound(r);
    return same([row?.frameWidth, row?.frameHeight], dimensions[spec.qualities[p]]) &&
      same([r?.last?.w, r?.last?.h], dimensions[spec.qualities[p]]);
  })));
  const receiverWindow = (p, begin, finish, key, growing) => {
    const rows = samples.slice(begin, finish + 1).map(s => inbound(reader(s[p], trackSid)));
    const values = rows.map(row => row?.[key]);
    return statsWindow(rows) && monotonic(values) && (growing ? values.at(-1) > values[0] : values.at(-1) === values[0]);
  };
  for (const [key, field] of [['receiver_bytes', 'bytesReceived'], ['receiver_frames', 'framesDecoded']]) {
    ok(key, ['a', 'b'].every(p => windows.every(([begin, finish]) => {
      if (!spec.published && samples.slice(begin, finish + 1).every(s => reader(s[p], trackSid)?.stats === null)) return true;
      return receiverWindow(p, begin, finish, field, progressing(spec, p));
    })));
  }
  const frameWindow = (p, begin, finish) => samples.slice(begin + 1, finish + 1).flatMap(s =>
    (reader(s[p], trackSid)?.frames ?? []).map(f => ({ ...f, sourceFrame: s.pub?.sourceFrame, sourceAt: s.pub?.at })));
  ok('frame_codes', ['a', 'b'].every(p => !progressing(spec, p) || windows.every(([begin, finish]) => {
    const frames = frameWindow(p, begin, finish), codes = frames.map(f => f.code);
    const first = reader(samples[begin][p], trackSid), lastReader = reader(samples[finish][p], trackSid);
    return frames.length >= 2 && frames.every(f => f.valid === true && Number.isInteger(f.code) && f.code >= 0 && f.code < 65536 &&
      f.checksum === (((f.code * 37 + 0x5b) ^ (f.code >> 8)) & 255) && same([f.w, f.h], dimensions[spec.qualities[p]])) &&
      monotonic(codes) && codes.at(-1) > codes[0] && delta(first, lastReader, 'valid') === frames.length &&
      delta(first, lastReader, 'invalid') === 0 && delta(first, lastReader, 'callbacks') === frames.length &&
      monotonic(frames.map(f => f.presentedFrames), true) && monotonic(frames.map(f => f.mediaTime), true);
  })));
  ok('frame_freshness', ['a', 'b'].every(p => !progressing(spec, p) || windows.every(([begin, finish]) => {
    const frames = frameWindow(p, begin, finish);
    const baseline = reader(samples[begin][p], trackSid)?.last;
    return frames.length >= 2 && finite(baseline?.code) && frames.at(-1).code > baseline.code && frames.every(f =>
      finite(f.sourceFrame) && f.sourceFrame - f.code >= -2 && f.sourceFrame - f.code <= 45 &&
      finite(f.at) && f.at > samples[begin][p].at - 150 && f.at <= samples[finish][p].at &&
      finite(f.sourceAt) && Math.abs(f.sourceAt - f.at) <= 1500);
  })));
  ok('paused_receivers_stopped', ['a', 'b'].every(p => progressing(spec, p) || windows.every(([begin, finish]) => {
    const first = reader(samples[begin][p], trackSid), lastReader = reader(samples[finish][p], trackSid);
    const frames = frameWindow(p, begin, finish);
    return first && lastReader && frames.length === 0 && ['callbacks', 'valid', 'invalid'].every(k => delta(first, lastReader, k) === 0) &&
      same(first.last, lastReader.last);
  })));
  ok('bystander_continuity', !(spec.enabled.a !== spec.enabled.b && spec.published && !spec.paused) ||
    ['a', 'b'].filter(p => progressing(spec, p)).every(p => windows.every(([begin, finish]) =>
      receiverWindow(p, begin, finish, 'bytesReceived', true) && receiverWindow(p, begin, finish, 'framesDecoded', true) && frameWindow(p, begin, finish).length >= 2)));
  ok('sender_rids', selected.every(s => {
    if (!spec.published) return s.pub?.local?.senderPresent === false && s.pub.local.encodings === null;
    const local = s.pub?.local, rows = rtp(local, 'outbound-rtp');
    return local?.senderPresent === true && same(local.encodings?.map(e => e.rid).sort(), [...RIDS].sort()) &&
      same(rows.map(r => r.rid).sort(), [...RIDS].sort()) && rows.every(r => finite(r.framesEncoded) && finite(r.bytesSent)) &&
      local.encodings.every(e => typeof e.active === 'boolean' && e.scaleResolutionDownBy === ({ q: 4, h: 2, f: 1 })[e.rid] &&
        e.maxFramerate === 15 && e.maxBitrate === ({ q: 180000, h: 600000, f: 1800000 })[e.rid]);
  }));
  ok('sender_activity', selected.every(s => {
    const local = s.pub?.local;
    if (!spec.published) return local?.senderPresent === false;
    if (spec.paused) return local?.senderTrackPresent === false && local?.paused === true;
    return local?.senderTrackPresent === true && same(RIDS.filter(rid => local.encodings?.find(e => e.rid === rid)?.active === true), demand(spec));
  }));
  ok('sender_progress', !spec.published || windows.every(([begin, finish]) => RIDS.every(rid => {
    const rows = samples.slice(begin, finish + 1).map(s => rtp(s.pub?.local, 'outbound-rtp').find(r => r.rid === rid));
    const activeRid = !spec.paused && demand(spec).includes(rid);
    return statsWindow(rows) && ['bytesSent', 'framesEncoded'].every(k => {
      const values = rows.map(r => r?.[k]);
      return monotonic(values) && (activeRid ? values.at(-1) > values[0] : values.at(-1) === values[0]);
    }) && (!activeRid || rows.every(r => same([r.frameWidth, r.frameHeight], ({ q: [320, 180], h: [640, 360], f: [1280, 720] })[rid])));
  })));
  ok('source_continuity', windows.every(([begin, finish]) => samples[finish].pub?.sourceFrame > samples[begin].pub?.sourceFrame &&
    monotonic(samples.slice(begin, finish + 1).map(s => s.pub?.sourceFrame))) &&
    selected.every(s => same([s.pub?.local?.source?.width, s.pub?.local?.source?.height], [1280, 720]) || !spec.published));
  ok('no_runtime_errors', samples.every(s => !s.errors?.length && PEERS.every(p => Array.isArray(s[p]?.errors) && s[p].errors.length === 0)));
  return observed;
}

export async function inspectDependencies() {
  const packages = [];
  const rootManifest = JSON.parse(await readFile(resolve(INTEROP, 'package.json'), 'utf8'));
  const lock = JSON.parse(await readFile(resolve(INTEROP, 'package-lock.json'), 'utf8'));
  assert(same(Object.keys(PINS.packages).sort(), ['livekit-client', 'livekit-server-sdk', '@livekit/protocol', 'playwright'].sort()));
  assert.equal(PINS.packages['livekit-client'].version, '2.23.0');
  const modules = await realpath(resolve(INTEROP, 'node_modules'));
  for (const name of Object.keys(PINS.packages)) {
    const manifestPath = resolve(INTEROP, 'node_modules', name, 'package.json');
    const bytes = await readFile(manifestPath), manifest = JSON.parse(bytes);
    const entry = await realpath(loader.resolve(name));
    assert(entry.startsWith(modules + '/')); assert.equal(manifest.name, name);
    assert.equal(rootManifest.dependencies[name], PINS.packages[name].version);
    assert.equal(lock.packages[''].dependencies[name], PINS.packages[name].version);
    assert.equal(lock.packages[`node_modules/${name}`].version, PINS.packages[name].version);
    if (name === 'livekit-client') assert.equal(entry, await realpath(resolve(dirname(manifestPath), 'dist/livekit-client.umd.js')));
    packages.push({ name, version: manifest.version, package_sha256: hash(bytes), entry_sha256: hash(await readFile(entry)) });
  }
  return { packages, package_lock_sha256: hash(await readFile(resolve(INTEROP, 'package-lock.json'))),
    pins_sha256: hash(await readFile(PIN_PATH)), umd_entry: 'livekit-client/dist/livekit-client.umd.js' };
}
function dependenciesValid(value) {
  return value && same(value.packages, Object.entries(PINS.packages).map(([name, entry]) => ({ name, ...entry }))) &&
    value.package_lock_sha256 === PINS.package_lock_sha256 && value.pins_sha256 === hash(readFileSync(PIN_PATH)) &&
    value.umd_entry === 'livekit-client/dist/livekit-client.umd.js';
}
export function validateReport(report) {
  try { return validateEvidence(report); }
  catch { return { ok: false, errors: ['malformed-report'] }; }
}
function validateEvidence(report) {
  const errors = [], check = (valid, label) => { if (!valid) errors.push(label); };
  check(report?.status === 'PASS_SCOPED' && report.failure === undefined, 'overall-status');
  check(['go', 'rust'].includes(report?.side), 'side');
  check(report?.case_count === 12 && same(report?.cases?.map(c => c.id), CASE_IDS), 'fixed-ordered-cases');
  const initial = report?.cases?.[0]?.samples?.find(s => s.pub?.local?.published);
  const originalSid = initial?.pub.local.sid;
  let previous;
  for (const [i, spec] of CASES.entries()) {
    const row = report?.cases?.[i];
    check(row?.status === 'PASS' && row.failure === undefined, `${spec.id}:status`);
    check(Array.isArray(row?.samples) && row.samples.length >= 3, `${spec.id}:samples`);
    const command = ({ 'publisher-pause': 'pause', 'publisher-resume': 'resume', unpublish: 'unpublish', republish: 'publish' })[spec.id];
    const actions = [...(command ? [['pub', command]] : []), ['a', 'request'], ['b', 'request']];
    check(same(row?.actions?.map(action => [action.peer, action.name]), actions) &&
      row.actions.every(action => action.completed === true && finite(action.at) && action.at <= row.action_completed_at), `${spec.id}:actions`);
    check(row?.samples?.every(s => PEERS.every(p => s[p]?.participant === initial?.[p]?.participant)) === true, `${spec.id}:same-participants`);
    if (previous) {
      check(PEERS.every(p => row?.samples?.[0]?.[p]?.at > previous[p]?.at) &&
        row.samples[0].pub.sourceFrame >= previous.pub.sourceFrame, `${spec.id}:stage-continuity`);
    }
    previous = row?.samples?.at(-1);
    try {
      const derived = deriveObserved(spec, row?.samples ?? [], row?.windows ?? [], originalSid);
      check(same(row?.observed, derived), `${spec.id}:observed-evidence`);
      for (const key of CONTRACT_KEYS) check(derived.contract[key] === true, `${spec.id}:${key}`);
      check(row?.windows?.every(([a]) => minTime(row.samples[a]) >= row.action_completed_at) === true, `${spec.id}:post-action`);
      check(derived.publisher_paused === spec.paused && derived.published === spec.published, `${spec.id}:publisher-state`);
    } catch { errors.push(`${spec.id}:malformed-evidence`); }
  }
  check(dependenciesValid(report?.dependencies?.before), 'dependencies-before');
  check(dependenciesValid(report?.dependencies?.after), 'dependencies-after');
  check(same(report?.dependencies?.before, report?.dependencies?.after), 'dependencies-unchanged');
  check(HEX.test(report?.dependencies?.chromium_sha256), 'chromium-identity');
  check(HEX.test(report?.dependencies?.fixture_sha256?.client) && HEX.test(report?.dependencies?.fixture_sha256?.probe), 'fixture-identity');
  check(Array.isArray(report?.cleanup_errors) && report.cleanup_errors.length === 0, 'cleanup-errors');
  const cleanup = report?.browser_cleanup;
  check(cleanup?.exit?.code === 0 && cleanup.exit.signal === null && cleanup.exit.timeout !== true && ['child-exit-event', 'child-state'].includes(cleanup.exit.observedBy) &&
    Number.isInteger(cleanup.contextsBefore) && cleanup.contextsBefore >= 1 && cleanup.contextsAfter === 0 &&
    Array.isArray(cleanup.failures) && cleanup.failures.length === 0, 'browser-cleanup');
  check(report?.host_close?.closed === true && report.host_close.error === null, 'host-close');
  check(report?.room_cleanup?.deleted === true && report.room_cleanup.status === 200, 'owned-room-cleanup');
  check(report?.diagnostics?.page_errors?.count === 0 && Array.isArray(report?.diagnostics?.page_errors?.entries) &&
    report.diagnostics.page_errors.entries.length === 0 && report?.diagnostics?.console?.error === undefined &&
    report?.diagnostics?.fixture_errors === 0, 'runtime-errors');
  check(Array.isArray(report?.setup_samples) && report.setup_samples.every(s => !s.errors?.length && PEERS.every(p => s[p]?.errors?.length === 0)), 'setup-errors');
  return { ok: errors.length === 0, errors };
}

export function validateInput(input) {
  assert(input && typeof input === 'object' && !Array.isArray(input), 'input-object');
  assert(same(Object.keys(input).sort(), ['side', 'url', 'key', 'secret', 'chromium'].sort()), 'input-fields');
  assert(['go', 'rust'].includes(input.side), 'input-side');
  assert(typeof input.url === 'string' && /^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}\/?$/.test(input.url), 'literal-loopback-url');
  const url = new URL(input.url);
  assert(Number(input.url.match(/:([0-9]+)\/?$/)[1]) <= 65535 && url.hostname === '127.0.0.1', 'loopback-port');
  assert(typeof input.key === 'string' && input.key.length > 0 && typeof input.secret === 'string' && input.secret.length > 0, 'credentials');
  assert(typeof input.chromium === 'string' && isAbsolute(input.chromium), 'absolute-chromium');
  return { ...input, url: url.origin };
}
const delay = ms => new Promise(done => setTimeout(done, ms));
function fault(stage, kind, check) { return Object.assign(new Error(check), { safe: { stage, kind, check } }); }
function failure(error, stage) {
  return error?.safe ?? { stage, kind: ['AssertionError', 'TimeoutError', 'TypeError', 'SyntaxError'].includes(error?.name) ? error.name : 'Error', check: 'operation-failed' };
}
function bounded(promise, ms, stage, signal) {
  return new Promise((done, reject) => {
    const finish = (fn, value) => { clearTimeout(timeout); signal?.removeEventListener('abort', abort); fn(value); };
    const timeout = setTimeout(() => finish(reject, fault(stage, 'timeout', 'deadline')), Math.max(1, ms));
    const abort = () => finish(reject, fault(stage, 'interrupted', signal.reason === 'SIGINT' ? 'SIGINT' : signal.reason === 'SIGTERM' ? 'SIGTERM' : 'outer-deadline'));
    if (signal?.aborted) abort(); else signal?.addEventListener('abort', abort, { once: true });
    Promise.resolve(promise).then(value => finish(done, value), error => finish(reject, error));
  });
}
async function api(url, method, body, token) {
  return new Promise((done, reject) => {
    const bytes = Buffer.from(JSON.stringify(body));
    const req = request(new URL(`/twirp/livekit.RoomService/${method}`, url), { method: 'POST', agent: false,
      headers: { 'content-type': 'application/json', 'content-length': bytes.length, authorization: `Bearer ${token}` } }, res => {
      let size = 0; const chunks = [];
      res.on('data', b => { size += b.length; if (size > 1024 * 1024) req.destroy(fault('api', 'protocol', 'response-size')); else chunks.push(b); });
      res.on('end', () => {
        try { done({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) }); }
        catch { reject(fault('api', 'protocol', 'response-json')); }
      });
      res.on('error', reject);
    });
    req.on('socket', socket => socket.once('connect', () => {
      if (!['127.0.0.1', '::ffff:127.0.0.1'].includes(socket.remoteAddress)) req.destroy(fault('api', 'endpoint', 'actual-loopback'));
    }));
    req.setTimeout(10000, () => req.destroy(fault('api', 'timeout', 'request-timeout')));
    req.on('error', reject); req.end(bytes);
  });
}
export async function main(argv = process.argv.slice(2)) {
  assert(argv.length === 2 && argv[0] === '--output' && argv[1], 'usage: node probe.mjs --output FRESHFILE');
  const output = await open(resolve(argv[1]), 'wx', 0o600);
  const report = { status: 'FAIL', side: null, case_count: 12, execution: 'NOT_RUN',
    cases: CASES.map(spec => ({ id: spec.id, status: 'NOT_RUN', observed: deriveObserved(spec, [], [], null), samples: [], windows: [], actions: [] })),
    setup_samples: [], dependencies: { before: null, after: null, chromium_sha256: null, fixture_sha256: null },
    cleanup_errors: [], browser_cleanup: null, host_close: { closed: false, error: null }, room_cleanup: { deleted: false },
    diagnostics: { console: {}, page_errors: { count: 0, entries: [] }, fixture_errors: 0 } };
  const persist = async () => { const bytes = Buffer.from(JSON.stringify(report, null, 2) + '\n'); await output.truncate(0); await output.write(bytes, 0, bytes.length, 0); await output.sync(); };
  try { await persist(); } catch (error) { await output.close(); throw error; }
  const abort = new AbortController();
  const onInt = () => abort.abort('SIGINT'), onTerm = () => abort.abort('SIGTERM');
  process.once('SIGINT', onInt); process.once('SIGTERM', onTerm);
  const outer = setTimeout(() => abort.abort('outer-deadline'), 500000);
  let input, host, browserServer, browser, context, roomName, adminToken, roomOwned = false;
  let stage = 'input', current, collecting = false, sampler, samplerFailure, originalSid;
  const pages = {};
  const sample = async () => {
    const values = await Promise.allSettled(PEERS.map(p => bounded(pages[p].evaluate(() => fixture.snapshot()), 5000, 'snapshot')));
    const snapshot = { at: Date.now(), errors: [] };
    for (const [i, value] of values.entries()) {
      snapshot[PEERS[i]] = value.status === 'fulfilled' ? value.value : null;
      if (value.status === 'rejected') snapshot.errors.push(failure(value.reason, 'snapshot'));
      else report.diagnostics.fixture_errors += value.value.errors.length;
    }
    (current?.samples ?? report.setup_samples).push(snapshot);
    if (snapshot.errors.length) throw fault('snapshot', 'measurement', 'peer-snapshot');
    return snapshot;
  };
  const errorsPresent = () => report.diagnostics.page_errors.count > 0 || report.diagnostics.console.error > 0 || report.diagnostics.fixture_errors > 0;
  try {
    let text = '';
    await bounded((async () => { for await (const chunk of process.stdin) { text += chunk; if (text.length > 65536) throw fault('input', 'validation', 'stdin-size'); } })(), 10000, stage, abort.signal);
    input = validateInput(JSON.parse(text)); report.side = input.side;
    stage = 'dependencies';
    report.dependencies.before = await inspectDependencies();
    assert(dependenciesValid(report.dependencies.before));
    const executable = await realpath(input.chromium);
    assert((await stat(executable)).isFile()); await access(executable, constants.X_OK);
    report.dependencies.chromium_sha256 = hash(await readFile(executable));
    const html = await readFile(resolve(HERE, 'client.html'));
    report.dependencies.fixture_sha256 = { client: hash(html), probe: hash(await readFile(fileURLToPath(import.meta.url))) };
    const sdk = await readFile(loader.resolve('livekit-client'));
    assert.equal(hash(sdk), PINS.packages['livekit-client'].entry_sha256);
    const { chromium } = loader('playwright'), { AccessToken } = loader('livekit-server-sdk');
    roomName = 'c60-' + randomUUID();
    const admin = new AccessToken(input.key, input.secret, { ttl: '10m' });
    admin.addGrant({ room: roomName, roomCreate: true, roomAdmin: true }); adminToken = await admin.toJwt();
    stage = 'room-create'; roomOwned = true; report.execution = 'STARTED';
    const created = await bounded(api(input.url, 'CreateRoom', { name: roomName, empty_timeout: 600, departure_timeout: 60 }, adminToken), 15000, stage, abort.signal);
    assert.equal(created.status, 200); assert.equal(created.body.name, roomName);
    stage = 'browser';
    host = createServer((req, res) => {
      if (!['/', '/sdk.js', '/favicon.ico'].includes(req.url)) { res.writeHead(404); res.end(); return; }
      if (req.url === '/favicon.ico') { res.writeHead(204); res.end(); return; }
      res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'application/javascript' : 'text/html', 'cache-control': 'no-store' });
      res.end(req.url === '/sdk.js' ? sdk : html);
    });
    await new Promise((done, reject) => { host.once('error', reject); host.listen(0, '127.0.0.1', done); });
    browserServer = await chromium.launchServer({ headless: true, executablePath: executable, timeout: 30000,
      args: ['--disable-dev-shm-usage', '--autoplay-policy=no-user-gesture-required', '--disable-background-timer-throttling', '--disable-renderer-backgrounding'] });
    browser = await chromium.connect(browserServer.wsEndpoint(), { timeout: 15000 });
    context = await browser.newContext(); context.setDefaultTimeout(10000);
    for (const p of PEERS) {
      const page = await context.newPage(); pages[p] = page;
      page.on('console', message => { const type = message.type(); report.diagnostics.console[type] = (report.diagnostics.console[type] ?? 0) + 1; });
      page.on('pageerror', error => {
        report.diagnostics.page_errors.count++;
        report.diagnostics.page_errors.entries.push({ peer: p, kind: ['Error', 'TypeError', 'ReferenceError', 'SyntaxError'].includes(error.name) ? error.name : 'Error',
          message_length: error.message.length, message_sha256: hash(error.message) });
      });
      await page.goto(`http://127.0.0.1:${host.address().port}/`);
    }
    collecting = true;
    sampler = (async () => {
      while (collecting) {
        try { await sample(); } catch (error) { samplerFailure = error; return; }
        if (collecting) await delay(150);
      }
    })();
    stage = 'join-publish';
    await bounded((async () => {
      for (const p of PEERS) {
        const identity = ({ pub: 'publisher', a: 'sub-a', b: 'sub-b' })[p];
        const token = new AccessToken(input.key, input.secret, { identity, ttl: '10m' });
        token.addGrant({ room: roomName, roomJoin: true, canPublish: p === 'pub', canSubscribe: true, canPublishData: false });
        await pages[p].evaluate(arg => fixture.connect(arg), { url: input.url.replace(/^http:/, 'ws:'), identity, token: await token.toJwt() });
      }
      const published = await pages.pub.evaluate(() => fixture.publish()); originalSid = published.sid;
    })(), 30000, stage, abort.signal);
    report.execution = 'MEASURED';
    for (const [i, spec] of CASES.entries()) {
      stage = spec.id; current = report.cases[i]; current.status = 'FAIL';
      const deadline = Date.now() + 30000;
      const operation = async (peer, name, payload) => {
        const action = { peer, name, at: Date.now(), completed: false }; current.actions.push(action);
        await bounded(pages[peer].evaluate(async ({ name, payload }) => fixture[name](payload), { name, payload }), deadline - Date.now(), stage, abort.signal);
        action.completed = true;
      };
      if (spec.id === 'publisher-pause') await operation('pub', 'pause');
      if (spec.id === 'publisher-resume') await operation('pub', 'resume');
      if (spec.id === 'unpublish') await operation('pub', 'unpublish');
      if (spec.id === 'republish') await operation('pub', 'publish');
      for (const p of ['a', 'b']) await operation(p, 'request', { quality: spec.qualities[p], enabled: spec.enabled[p] });
      current.action_completed_at = Date.now();
      let seen = -1;
      while (Date.now() < deadline) {
        if (abort.signal.aborted) throw fault(stage, 'interrupted', 'signal-or-deadline');
        if (samplerFailure) throw samplerFailure;
        if (errorsPresent()) throw fault(stage, 'runtime', 'browser-or-fixture-error');
        if (seen !== current.samples.length) {
          seen = current.samples.length;
          current.windows = chooseWindows(current.samples, current.action_completed_at);
          current.observed = deriveObserved(spec, current.samples, current.windows, originalSid);
          if (Object.values(current.observed.contract).every(value => value === true)) { current.status = 'PASS'; break; }
        }
        await delay(50);
      }
      if (current.status !== 'PASS') throw fault(stage, 'contract', Object.entries(current.observed.contract).filter(([, value]) => value !== true).map(([key]) => key).join(','));
      await persist();
    }
  } catch (error) {
    report.failure = failure(error, stage);
    const row = current ?? report.cases[0]; row.status = 'FAIL'; row.failure = report.failure;
  } finally {
    collecting = false;
    if (sampler) await sampler;
    if (samplerFailure && !report.failure) { report.failure = failure(samplerFailure, 'snapshot'); if (current) { current.status = 'FAIL'; current.failure = report.failure; } }
    process.stdin.destroy();
    const cleanup = async (label, fn, timeout = 5000) => {
      try { await bounded(fn(), timeout, 'cleanup'); }
      catch { report.cleanup_errors.push({ stage: 'cleanup', kind: 'resource', check: label }); }
    };
    await Promise.all(Object.values(pages).map(page => cleanup('peer-disconnect', () => page.evaluate(() => fixture.close()))));
    if (browserServer) {
      await cleanup('owned-browser-close', async () => { report.browser_cleanup = await closeOwnedBrowser(browser, browserServer); }, 15000);
      if (!report.browser_cleanup?.exit || report.browser_cleanup.exit.timeout) {
        report.cleanup_errors.push({ stage: 'cleanup', kind: 'browser', check: 'graceful-exit-missing' });
        await cleanup('owned-browser-force-close', () => browserServer.kill());
        await cleanup('owned-browser-exit-after-force', async () => { report.browser_cleanup = await closeOwnedBrowser(browser, browserServer); }, 10000);
      }
    }
    if (browserServer && (report.browser_cleanup?.failures?.length || !report.browser_cleanup?.exit || report.browser_cleanup.exit.code !== 0 || report.browser_cleanup.contextsAfter !== 0)) {
      report.cleanup_errors.push({ stage: 'cleanup', kind: 'browser', check: 'real-clean-child-exit' });
    }
    if (host) await cleanup('host-close', () => new Promise((done, reject) => host.close(error => {
      report.host_close = { closed: !error, error: error ? 'host-close-error' : null }; error ? reject(error) : done();
    })));
    if (roomOwned) await cleanup('owned-room-delete', async () => {
      const removed = await api(input.url, 'DeleteRoom', { room: roomName }, adminToken);
      assert([200, 404].includes(removed.status)); report.room_cleanup = { deleted: true, status: removed.status };
    }, 12000);
    try { report.dependencies.after = await inspectDependencies(); assert(dependenciesValid(report.dependencies.after)); assert(same(report.dependencies.before, report.dependencies.after)); }
    catch { report.cleanup_errors.push({ stage: 'dependencies-after', kind: 'integrity', check: 'pinned-dependencies-unchanged' }); }
    if (errorsPresent() && !report.failure) report.failure = { stage: 'final', kind: 'runtime', check: 'browser-or-fixture-error' };
    report.status = !report.failure && !report.cleanup_errors.length && report.cases.every(c => c.status === 'PASS') ? 'PASS_SCOPED' : 'FAIL';
    const validation = validateReport(report);
    if (report.status === 'PASS_SCOPED' && !validation.ok) { report.status = 'FAIL'; report.failure = { stage: 'validation', kind: 'contract', check: validation.errors.join(',') }; }
    report.validation = validation;
    try { await persist(); }
    finally {
      await output.close();
      clearTimeout(outer); process.removeListener('SIGINT', onInt); process.removeListener('SIGTERM', onTerm);
    }
  }
  return report;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().then(report => { process.exitCode = report.status === 'PASS_SCOPED' ? 0 : 1; }).catch(() => { process.stderr.write('C60 probe could not create or finalize its exclusive output.\n'); process.exitCode = 1; });
}
