import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFile, writeFile, rename, realpath } from 'node:fs/promises';
import { dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const INTEROP = resolve(HERE, '..');
const loader = createRequire(resolve(INTEROP, 'package.json'));
const { AccessToken, RoomServiceClient } = loader('livekit-server-sdk');
const { SignalRequest, SignalResponse, VideoLayer } = loader('@livekit/protocol');
const TIMEOUT = 8000;
export const ITEM = 'SIGNAL:VideoLayer.OpenQualityConsumers';
export const SCOPE = 'Launched MAIN authenticated binary /rtc AddTrack metadata only; no SDP exchange, RTP, media, SRTP or dynacast coverage.';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');

class CheckFailure extends Error {}
function check(condition, label) {
  if (!condition) throw new CheckFailure(label);
}
function failure(error, stage) {
  // Never serialize SDK/transport errors, URLs, tokens, SDP or participant snapshots.
  return { stage, kind: error?.name ?? 'Error', check: error instanceof CheckFailure ? error.message : 'operation_failed' };
}
async function bounded(promise, label) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new CheckFailure(`${label}_timeout`)), TIMEOUT);
    })]);
  } finally {
    clearTimeout(timer);
  }
}

let marker = 0;
function layers(tag, qualities) {
  return qualities.map(quality => {
    const n = ++marker;
    return { quality, width: 1000 + n, height: 2000 + n, bitrate: 300000 + n,
      ssrc: 400000 + n, spatialLayer: n, rid: `${tag}-${n}`, repairSsrc: 500000 + n };
  });
}
function vector(id, common, codecQualities = []) {
  return { id, cid: `track-${id}`, layers: layers(`${id}-common`, common),
    simulcastCodecs: codecQualities.map(([codec, qualities]) => ({
      codec, cid: `${id}-${codec}`, videoLayerMode: 1, layers: layers(`${id}-${codec}`, qualities),
    })) };
}
const SIGNED = [2147483647, 0, -2147483648, 77, -1, 2];
export const CASES = [
  vector('codec-less-common-unordered', [2, 0, 1]),
  vector('vp8-common-fallback', [1, 2, 0], [['vp8', []]]),
  vector('vp8-specific-unordered', [0, 1, 2], [['vp8', [2, 0, 1]]]),
  vector('dual-codec-distinct', [1, 0, 2], [['vp8', [2, 0, 1]], ['h264', [1, 2, 0]]]),
  vector('codec-layers-without-common', [], [['vp8', [2, 1, 0]], ['h264', [1, 0, 2]]]),
  vector('signed-codec-less-common', SIGNED),
  vector('signed-common-fallback', SIGNED, [['vp8', []], ['h264', []]]),
  vector('signed-codec-specific', SIGNED, [
    ['vp8', [77, -2147483648, 2, 2147483647, 0, -1]],
    ['h264', [-1, 2147483647, 0, -2147483648, 77, 2]],
  ]),
  vector('empty-layers', [], [['vp8', []], ['h264', []]]),
  vector('codec-less-empty', []),
  vector('regular-low-medium-high', [0, 1, 2], [['vp8', [0, 1, 2]], ['h264', [0, 1, 2]]]),
];

// Only the oracle expectation is sorted. The observed arrays are never reordered.
export function expectedMetadata(spec) {
  const sorted = value => structuredClone(value).sort((a, b) => a.quality - b.quality);
  const codecs = spec.simulcastCodecs.length ? spec.simulcastCodecs.map(codec => ({
    mimeType: `video/${codec.codec}`, mid: '', cid: codec.cid, sdpCid: '', videoLayerMode: 1,
    layers: sorted(codec.layers.length ? codec.layers : spec.layers),
  })) : [{ mimeType: '', mid: '', cid: spec.cid, sdpCid: '', videoLayerMode: 1, layers: sorted(spec.layers) }];
  return { layers: sorted(spec.layers), codecs };
}
export function layerMetadata(layer) {
  return { quality: layer.quality, width: layer.width, height: layer.height, bitrate: layer.bitrate,
    ssrc: layer.ssrc, spatialLayer: layer.spatialLayer, rid: layer.rid, repairSsrc: layer.repairSsrc };
}
export function observedMetadata(track) {
  return { layers: track.layers.map(layerMetadata), codecs: track.codecs.map(codec => ({
    mimeType: codec.mimeType, mid: codec.mid, cid: codec.cid, sdpCid: codec.sdpCid,
    videoLayerMode: codec.videoLayerMode, layers: codec.layers.map(layerMetadata),
  })) };
}
export function differences(expected, observed, path = '$') {
  if (Object.is(expected, observed)) return [];
  if (expected === null || observed === null || typeof expected !== 'object' || typeof observed !== 'object') {
    return [{ path, expected: expected ?? null, observed: observed ?? null }];
  }
  const result = [];
  if (Array.isArray(expected) !== Array.isArray(observed)) return [{ path, expected, observed }];
  if (Array.isArray(expected) && expected.length !== observed.length) {
    result.push({ path: `${path}.length`, expected: expected.length, observed: observed.length });
  }
  for (const key of new Set([...Object.keys(expected), ...Object.keys(observed)])) {
    result.push(...differences(expected[key], observed[key], `${path}.${key}`));
  }
  return result;
}
export function addTrack(spec) {
  const input = { cid: spec.cid, name: spec.id, type: 1, source: 1, width: 1920, height: 1080,
    layers: spec.layers, simulcastCodecs: spec.simulcastCodecs };
  const bytes = SignalRequest.fromJson({ addTrack: input }).toBinary();
  const decoded = SignalRequest.fromBinary(bytes).message.value;
  assert.deepEqual(decoded.layers.map(layerMetadata), spec.layers, 'original protocol common-layer roundtrip');
  assert.deepEqual(decoded.simulcastCodecs.map(c => c.layers.map(layerMetadata)),
    spec.simulcastCodecs.map(c => c.layers), 'original protocol codec-layer roundtrip');
  return { input, bytes };
}

async function dependencyIdentity(name, version) {
  const entry = await realpath(loader.resolve(name));
  check(entry.startsWith(await realpath(resolve(INTEROP, 'node_modules')) + sep), 'dependency_not_in_interop');
  let dir = dirname(entry);
  for (;;) {
    try {
      const bytes = await readFile(resolve(dir, 'package.json'));
      const pkg = JSON.parse(bytes);
      if (pkg.name === name) {
        check(pkg.version === version, 'dependency_version_mismatch');
        return { version, package_sha256: hash(bytes), entry_sha256: hash(await readFile(entry)) };
      }
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    check(dirname(dir) !== dir, 'dependency_package_missing');
    dir = dirname(dir);
  }
}

class Peer {
  constructor(url, token) {
    this.events = [];
    this.waiters = new Set();
    this.closing = false;
    this.count = 0;
    const endpoint = new URL('/rtc', url);
    endpoint.protocol = 'ws:';
    for (const [key, value] of Object.entries({ access_token: token, protocol: '17', sdk: 'unknown', auto_subscribe: '0' })) {
      endpoint.searchParams.set(key, value);
    }
    this.ws = new WebSocket(endpoint);
    this.ws.binaryType = 'arraybuffer';
    this.closed = new Promise(done => this.ws.addEventListener('close', event => {
      if (!this.closing) this.fail(new CheckFailure('unexpected_ws_close'));
      done(event.code);
    }));
    this.ws.addEventListener('error', () => this.fail(new CheckFailure('ws_error')));
    this.ws.addEventListener('message', event => {
      try {
        check(event.data instanceof ArrayBuffer, 'nonbinary_signal');
        check(++this.count <= 2048, 'signal_event_limit');
        const bytes = new Uint8Array(event.data);
        const message = SignalResponse.fromBinary(bytes).message;
        let value;
        switch (message.case) {
          case 'join':
            value = { identity: message.value.participant?.identity, sid: message.value.participant?.sid };
            break;
          case 'trackPublished':
            value = { cid: message.value.cid, track_sid_present: Boolean(message.value.track?.sid),
              metadata: message.value.track ? observedMetadata(message.value.track) : null,
              response_sha256: hash(bytes) };
            break;
          case 'requestResponse':
            value = { request_id: message.value.requestId, reason: message.value.reason,
              add_track_cid: message.value.request?.case === 'addTrack' ? message.value.request.value.cid : null };
            break;
          case 'pongResp': value = { timestamp: String(message.value.lastPingTimestamp) }; break;
          case 'leave': throw new CheckFailure('server_leave');
          default: return;
        }
        this.events.push({ type: message.case, value });
        for (const wake of this.waiters) wake();
      } catch (error) {
        this.fail(error);
      }
    });
  }
  fail(error) {
    this.error ??= error;
    for (const wake of this.waiters) wake();
  }
  async wait(predicate, label, start = 0) {
    let wake;
    try {
      return await bounded(new Promise((done, reject) => {
        wake = () => {
          if (this.error) return reject(this.error);
          const event = this.events.slice(start).find(predicate);
          if (event) done(event);
        };
        this.waiters.add(wake);
        wake();
      }), label);
    } finally {
      this.waiters.delete(wake);
    }
  }
  send(bytes) {
    check(!this.error && this.ws.readyState === WebSocket.OPEN, 'ws_not_healthy');
    this.ws.send(bytes);
  }
  sendJson(json) { this.send(SignalRequest.fromJson(json).toBinary()); }
  async health() {
    const timestamp = String(Date.now());
    const start = this.events.length;
    this.sendJson({ pingReq: { timestamp } });
    await this.wait(e => e.type === 'pongResp' && e.value.timestamp === timestamp, 'ws_ping', start);
  }
  async close() {
    this.closing = true;
    if (this.ws.readyState !== WebSocket.CLOSED) this.ws.close(1000, 'fixture complete');
    return bounded(this.closed, 'ws_close');
  }
}

export async function run(options, output) {
  check(['go', 'rust'].includes(options.side), 'invalid_side');
  const url = new URL(options.url);
  check(url.protocol === 'http:' && url.hostname === '127.0.0.1' && Number(url.port) >= 1024 && Number(url.port) < 49152,
    'loopback_low_port_required');
  const report = { status: 'RUNNING', scope: SCOPE, covers: [ITEM], items: [ITEM], side: options.side,
    case_count: CASES.length, cases: [], cleanup_errors: [] };
  await writeFile(output, '', { flag: 'wx', mode: 0o600 });
  const save = async () => {
    await writeFile(`${output}.tmp`, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
    await rename(`${output}.tmp`, output);
  };
  let cancelled = false;
  let active;
  let stage = 'dependencies';
  let roomAttempted = false;
  const room = `layer-metadata-${randomUUID()}`;
  const api = new RoomServiceClient(options.url, options.key, options.secret, { requestTimeout: 5, failover: false });
  const cancel = () => { cancelled = true; active?.fail(new CheckFailure('cancelled')); };
  process.on('SIGINT', cancel);
  process.on('SIGTERM', cancel);
  try {
    report.dependencies = {
      protocol: await dependencyIdentity('@livekit/protocol', '1.52.1'),
      sdk: await dependencyIdentity('livekit-server-sdk', '2.19.2'),
      lock_sha256: hash(await readFile(resolve(INTEROP, 'package-lock.json'))),
    };
    assert.deepEqual(VideoLayer.fields.list().map(field => field.localName).sort(),
      Object.keys(layerMetadata(new VideoLayer())).sort(), 'all current VideoLayer fields covered');
    stage = 'create_room';
    roomAttempted = true;
    await bounded(api.createRoom({ name: room }), stage);
    for (const [index, spec] of CASES.entries()) {
      check(!cancelled, 'cancelled');
      const row = { id: spec.id, status: 'RUNNING', expected: expectedMetadata(spec), observed: null };
      report.cases.push(row);
      try {
        stage = 'join';
        const identity = `layer-owner-${randomUUID()}`;
        const mint = new AccessToken(options.key, options.secret, { identity, ttl: '5m' });
        mint.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: false,
          canPublishData: false, canUpdateOwnMetadata: true });
        active = new Peer(options.url, await mint.toJwt());
        const join = await active.wait(e => e.type === 'join', 'join');
        check(join.value.identity === identity && Boolean(join.value.sid), 'join_owner_mismatch');
        await active.health();
        const owner = await bounded(api.getParticipant(room, identity), 'owner_get');
        check(owner.sid === join.value.sid && owner.permission?.canPublish && owner.permission?.canUpdateMetadata,
          'owner_permissions_mismatch');
        row.join_owner_ok = true;
        stage = 'add_track';
        const request = addTrack(spec);
        row.input = request.input;
        row.request_sha256 = hash(request.bytes);
        await save();
        const start = active.events.length;
        active.send(request.bytes);
        const response = await active.wait(e =>
          (e.type === 'trackPublished' && e.value.cid === spec.cid) ||
          (e.type === 'requestResponse' && e.value.add_track_cid === spec.cid), 'add_track_response', start);
        row.response = { type: response.type, ...response.value };
        row.observed = response.type === 'trackPublished' ? response.value.metadata : null;
        delete row.response.metadata;
        row.differences = differences(row.expected, row.observed);
        // A subsequent owner operation, not a frontend Pong, fences AddTrack processing.
        stage = 'owner_barrier';
        const requestId = 1000 + index;
        const metadata = `barrier-${spec.id}`;
        const barrierStart = active.events.length;
        active.sendJson({ updateMetadata: { requestId, metadata } });
        const barrier = await active.wait(e => e.type === 'requestResponse' && e.value.request_id === requestId,
          'owner_metadata_response', barrierStart);
        row.barrier = { request_id: barrier.value.request_id, reason: barrier.value.reason };
        check(barrier.value.reason === 0, 'owner_barrier_rejected');
        const after = await bounded(api.getParticipant(room, identity), 'owner_after_barrier');
        row.barrier.owner_metadata_matches = after.sid === owner.sid && after.metadata === metadata;
        check(row.barrier.owner_metadata_matches, 'owner_metadata_mismatch');
        stage = 'ws_health';
        await active.health();
        row.ws_healthy = true;
        row.track_published_count = active.events.slice(start).filter(e => e.type === 'trackPublished' && e.value.cid === spec.cid).length;
        check(response.type === 'trackPublished' && row.response.track_sid_present && row.track_published_count === 1,
          'expected_exactly_one_publication');
        check(row.differences.length === 0, 'layer_metadata_mismatch');
        row.status = 'PASS';
      } catch (error) {
        row.status = 'FAIL';
        row.failure = failure(error, stage);
      } finally {
        if (active) {
          try {
            row.ws_close_code = await active.close();
            check(row.ws_close_code === 1000 && !active.error, 'unclean_ws_close');
          } catch (error) {
            row.status = 'FAIL';
            row.cleanup_failure = failure(error, 'ws_close');
          }
          active = undefined;
        }
        await save();
      }
    }
    report.status = !cancelled && report.cases.length === CASES.length && report.cases.every(row => row.status === 'PASS') ? 'PASS_SCOPED' : 'FAIL';
  } catch (error) {
    report.status = 'FAIL';
    report.failure = failure(error, stage);
  } finally {
    if (roomAttempted) {
      try { await bounded(api.deleteRoom(room), 'room_delete'); }
      catch (error) { report.cleanup_errors.push(failure(error, 'room_delete')); }
    }
    if (cancelled || report.cleanup_errors.length) report.status = 'FAIL';
    process.off('SIGINT', cancel);
    process.off('SIGTERM', cancel);
    await save();
  }
  return report;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    check(process.argv.length === 4 && process.argv[2] === '--output', 'usage_probe_output');
    let input = '';
    for await (const chunk of process.stdin) {
      input += chunk;
      check(input.length < 8192, 'input_limit');
    }
    const result = await run(JSON.parse(input), resolve(process.argv[3]));
    process.exitCode = result.status === 'PASS_SCOPED' ? 0 : 1;
  } catch (error) {
    process.stderr.write(JSON.stringify(failure(error, 'probe_entry')) + '\n');
    process.exitCode = 1;
  }
}
