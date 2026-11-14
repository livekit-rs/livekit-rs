/** SPEC-0077 C18 — launched-MAIN twirp ProtoJSON request-codec differential.
 *
 * Drives ONE launched MAIN (side under test) over the REAL twirp HTTP boundary
 * (/twirp/livekit.<Service>/<Method>, Content-Type application/json) with hand-crafted
 * raw JSON bodies that the official SDK would never emit (duplicate keys, exotic integer
 * lexical forms, unknown enum symbols, WKT boundary strings). The official SDK is NOT
 * modified and NOT used for these edge bodies; raw HTTP is the only way to put duplicate
 * tokens on the wire. A small authenticated value-echo group mints a standard HS256
 * LiveKit token (Node crypto, the same fixture secret) to prove the launched MAIN decodes
 * integer lexical VALUES, not merely accept/reject.
 *
 * Upstream contract (locked v1.13.10, e5655ac2): the generated twirp handler decodes the
 * JSON request with protojson BEFORE the service impl runs its permission check. A decode
 * failure -> twirp.Malformed "the json request could not be decoded" (HTTP 400). A decode
 * success with no token -> the service auth gate -> twirp.Unauthenticated "permissions
 * denied" (HTTP 401, pkg/service/auth.go:263). Rust reproduces this ordering in
 * lk-compat/src/twirp/room_service.rs:46 unary! (decode then handler) and
 * other_services.rs:109/121/160 + sip_calls.rs:37 (decode then auth), with the codec in
 * lk-protocol/src/json/{mod,builder}.rs (descriptor-bound seen-field rule, exact integer
 * normalizer, unknown-enum drop, WKT grammar/ranges).
 *
 * Therefore the codec accept/reject DECISION is observable at HTTP without any worker,
 * Redis or room business logic. The reject decision is EXACTLY the upstream malformed-decode
 * sentinel; anything else means the codec accepted the body and a later layer responded:
 *   decoded=false -> 400 {code:"malformed", msg:"the json request could not be decoded"}
 *   decoded=true  -> NOT that sentinel: usually 401 {unauthenticated,"permissions denied"},
 *                    but some services (Ingress CreateIngress) run a pre-auth business
 *                    validation -> 400 {invalid_argument,"invalid ingress type"}. Both are
 *                    codec acceptance; the full {status,code,msg} is compared cross-side.
 * Each side must INDEPENDENTLY satisfy this structural contract (probe PASS), then the two
 * normalized contracts must be deeply equal. Equal-but-wrong does not pass: a codec that
 * collapses duplicate known keys to the last value (the historical F02 Rust gap) would
 * accept where Go rejects, failing the per-side assertion before any comparison.
 *
 * Normalization: a no-auth case compares {status,code,msg}; an authenticated value-echo
 * acceptance compares {status:200, empty_timeout} (server-generated sid/creation_time and
 * JSON key order are outside the codec contract); an echo rejection compares {status,code,msg}.
 */
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { writeFile } from 'node:fs/promises';

const url = process.env.RTC_URL;
const side = process.env.SIDE;
const outPath = process.env.OUTPUT;
assert.ok(url && side && outPath, 'RTC_URL/SIDE/OUTPUT required');
const KEY = process.env.LIVEKIT_API_KEY || 'devkey';
const SECRET = process.env.LIVEKIT_API_SECRET || 'owned-room-service-fixture-secret-0123456789';
const base = url.replace(/\/$/, '');

const MALFORMED = 'the json request could not be decoded';

const report = { status: 'RUNNING', side, contract: [], errors: [], diagnostics: {} };

// ---- case inventory -------------------------------------------------------
// group -> ledger leaf. mode 'no-auth' observes the codec decision before auth;
// mode 'echo' authenticates CreateRoom and reads back the decoded integer value.
const ROOM = 'livekit.RoomService';
const INGRESS = 'livekit.Ingress';
const SIP = 'livekit.SIP';
const EGRESS = 'livekit.Egress';

const CASES = [
  // G1 known-null-duplicate (CODEC:known_null_and_duplicate) — official oracle null/dup/oneof
  { group: 'known-null-duplicate', case: 'null-scalar', service: ROOM, method: 'CreateRoom', body: '{"name":null}', decoded: true },
  { group: 'known-null-duplicate', case: 'null-then-known-duplicate', service: ROOM, method: 'CreateRoom', body: '{"name":null,"name":"r"}', decoded: false },
  { group: 'known-null-duplicate', case: 'null-alias-duplicate', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":null,"empty_timeout":1}', decoded: false },
  { group: 'known-null-duplicate', case: 'null-message-duplicate', service: SIP, method: 'CreateSIPInboundTrunk', body: '{"trunk":null,"trunk":{"name":"r"}}', decoded: false },
  { group: 'known-null-duplicate', case: 'null-optional-duplicate', service: INGRESS, method: 'CreateIngress', body: '{"enabled":null,"enabled":false}', decoded: false },
  { group: 'known-null-duplicate', case: 'different-oneof-null', service: EGRESS, method: 'StartEgress', body: '{"web":null,"template":{}}', decoded: true },
  { group: 'known-null-duplicate', case: 'null-message-accept', service: ROOM, method: 'CreateRoom', body: '{"name":"r","egress":null}', decoded: true },
  { group: 'known-null-duplicate', case: 'known-scalar-duplicate', service: ROOM, method: 'CreateRoom', body: '{"name":"a","name":"b"}', decoded: false },
  { group: 'known-null-duplicate', case: 'alias-duplicate', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":1,"empty_timeout":2}', decoded: false },

  // G2 unknown-enum (CODEC:unknown_enum) — numeric preserved, symbolic dropped, dup still seen
  { group: 'unknown-enum', case: 'unknown-numeric-enum', service: INGRESS, method: 'CreateIngress', body: '{"inputType":12345}', decoded: true },
  { group: 'unknown-enum', case: 'unknown-symbolic-enum', service: INGRESS, method: 'CreateIngress', body: '{"inputType":"FUTURE_INPUT"}', decoded: true },
  { group: 'unknown-enum', case: 'unknown-enum-then-alias-duplicate', service: INGRESS, method: 'CreateIngress', body: '{"inputType":"FUTURE_INPUT","input_type":"WHIP_INPUT"}', decoded: false },
  { group: 'unknown-enum', case: 'known-numeric-enum', service: INGRESS, method: 'CreateIngress', body: '{"inputType":0}', decoded: true },
  { group: 'unknown-enum', case: 'known-symbolic-enum', service: INGRESS, method: 'CreateIngress', body: '{"inputType":"WHIP_INPUT"}', decoded: true },

  // G3 integer-lexical (CODEC:integer_lexical) — exact decimal/exponent, no f64 loss
  { group: 'integer-lexical', case: 'unquoted-integral-exponent', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":1e2}', decoded: true },
  { group: 'integer-lexical', case: 'quoted-integral-exponent', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":"1e2"}', decoded: true },
  { group: 'integer-lexical', case: 'integral-decimal', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":1.0}', decoded: true },
  { group: 'integer-lexical', case: 'fractional-int-reject', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":1.5}', decoded: false },
  { group: 'integer-lexical', case: 'integer-whitespace-reject', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":" 1"}', decoded: false },
  { group: 'integer-lexical', case: 'uint32-overflow', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":4294967296}', decoded: false },
  // authenticated value-echo: the launched MAIN must decode the exact integer VALUE
  { group: 'integer-lexical', case: 'echo-exponent', service: ROOM, method: 'CreateRoom', body: '{"name":"c18-echo-exponent","emptyTimeout":1e2}', mode: 'echo', echo: 100 },
  { group: 'integer-lexical', case: 'echo-quoted', service: ROOM, method: 'CreateRoom', body: '{"name":"c18-echo-quoted","emptyTimeout":"100"}', mode: 'echo', echo: 100 },
  { group: 'integer-lexical', case: 'echo-decimal', service: ROOM, method: 'CreateRoom', body: '{"name":"c18-echo-decimal","emptyTimeout":1.0}', mode: 'echo', echo: 1 },
  { group: 'integer-lexical', case: 'echo-fraction-reject', service: ROOM, method: 'CreateRoom', body: '{"name":"c18-echo-fraction","emptyTimeout":1.5}', mode: 'echo', reject: true },
  { group: 'integer-lexical', case: 'echo-overflow-reject', service: ROOM, method: 'CreateRoom', body: '{"name":"c18-echo-overflow","emptyTimeout":4294967296}', mode: 'echo', reject: true },

  // G4 wkt-bounds (CODEC:wkt_bounds) — Duration/Timestamp grammar + ranges
  { group: 'wkt-bounds', case: 'duration-over-max', service: SIP, method: 'CreateSIPParticipant', body: '{"ringingTimeout":"315576000001s"}', decoded: false },
  { group: 'wkt-bounds', case: 'duration-empty-seconds', service: SIP, method: 'CreateSIPParticipant', body: '{"ringingTimeout":".5s"}', decoded: true },
  { group: 'wkt-bounds', case: 'duration-leading-plus', service: SIP, method: 'CreateSIPParticipant', body: '{"ringingTimeout":"+1s"}', decoded: true },
  { group: 'wkt-bounds', case: 'duration-negative-subsecond', service: SIP, method: 'CreateSIPParticipant', body: '{"ringingTimeout":"-0.000000001s"}', decoded: true },
  { group: 'wkt-bounds', case: 'duration-overprecision', service: SIP, method: 'CreateSIPParticipant', body: '{"ringingTimeout":"1.0000000001s"}', decoded: false },
  { group: 'wkt-bounds', case: 'timestamp-offset', service: SIP, method: 'CreateSIPInboundTrunk', body: '{"trunk":{"createdAt":"2020-01-01T00:00:00+01:00"}}', decoded: true },
  { group: 'wkt-bounds', case: 'timestamp-over-max', service: SIP, method: 'CreateSIPInboundTrunk', body: '{"trunk":{"createdAt":"10000-01-01T00:00:00Z"}}', decoded: false },

  // G5 duplicate-dispatch (CODEC:duplicate_known_json_field) — cross-method, before auth
  { group: 'duplicate-dispatch', case: 'CreateRoom-duplicate-known-scalar', service: ROOM, method: 'CreateRoom', body: '{"name":"a","name":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'CreateRoom-duplicate-snake-camel', service: ROOM, method: 'CreateRoom', body: '{"empty_timeout":1,"emptyTimeout":2}', decoded: false },
  { group: 'duplicate-dispatch', case: 'CreateRoom-duplicate-camel-snake', service: ROOM, method: 'CreateRoom', body: '{"emptyTimeout":1,"empty_timeout":2}', decoded: false },
  { group: 'duplicate-dispatch', case: 'CreateRoom-duplicate-unknown-field', service: ROOM, method: 'CreateRoom', body: '{"bogus":1,"bogus":2}', decoded: true },
  { group: 'duplicate-dispatch', case: 'ListRooms-duplicate-known', service: ROOM, method: 'ListRooms', body: '{"names":["a"],"names":["b"]}', decoded: false },
  { group: 'duplicate-dispatch', case: 'ListRooms-duplicate-unknown', service: ROOM, method: 'ListRooms', body: '{"bogus":1,"bogus":2}', decoded: true },
  { group: 'duplicate-dispatch', case: 'DeleteRoom-duplicate-known', service: ROOM, method: 'DeleteRoom', body: '{"room":"a","room":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'GetParticipant-duplicate-known', service: ROOM, method: 'GetParticipant', body: '{"room":"a","room":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'RemoveParticipant-duplicate-known', service: ROOM, method: 'RemoveParticipant', body: '{"identity":"a","identity":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'UpdateParticipant-duplicate-known', service: ROOM, method: 'UpdateParticipant', body: '{"name":"a","name":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'MutePublishedTrack-duplicate-known', service: ROOM, method: 'MutePublishedTrack', body: '{"room":"a","room":"b"}', decoded: false },
  { group: 'duplicate-dispatch', case: 'UpdateRoomMetadata-duplicate-known', service: ROOM, method: 'UpdateRoomMetadata', body: '{"room":"a","room":"b"}', decoded: false },
];

const GROUP_LEAF = {
  'known-null-duplicate': 'CODEC:known_null_and_duplicate',
  'unknown-enum': 'CODEC:unknown_enum',
  'integer-lexical': 'CODEC:integer_lexical',
  'wkt-bounds': 'CODEC:wkt_bounds',
  'duplicate-dispatch': 'CODEC:duplicate_known_json_field',
};

// ---- transport ------------------------------------------------------------
function b64url(x) { return Buffer.from(JSON.stringify(x)).toString('base64url'); }
function token() {
  const head = b64url({ alg: 'HS256', typ: 'JWT' });
  const body = b64url({ iss: KEY, sub: 'c18-codec-fixture', exp: Math.floor(Date.now() / 1000) + 3600,
    video: { roomCreate: true, roomAdmin: true, roomList: true } });
  const sig = createHmac('sha256', SECRET).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${sig}`;
}

async function post(service, method, rawBody, auth) {
  const headers = { 'Content-Type': 'application/json' };
  if (auth) headers.Authorization = `Bearer ${token()}`;
  const res = await fetch(`${base}/twirp/${service}/${method}`, { method: 'POST', headers, body: rawBody });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON body */ }
  return { status: res.status, json, text };
}

function errorEnvelope(obs) {
  // twirp error body is {code, msg, meta?}; compare the deterministic {code,msg}.
  assert.ok(obs.json && typeof obs.json === 'object', `expected a twirp JSON error body, got: ${obs.text.slice(0, 120)}`);
  return { code: obs.json.code, msg: obs.json.msg };
}

// The codec REJECT decision is exactly the upstream malformed-decode sentinel. Anything
// else (401 auth, 400 business invalid_argument, 200) means the codec ACCEPTED the body and
// a later layer produced the response. Some services (Ingress) run a business validation
// before auth, so a decoded body can yield 400 invalid_argument rather than 401; that is
// still a codec acceptance and is compared cross-side byte-for-byte.
function isDecodeReject(status, env) {
  return status === 400 && env && env.code === 'malformed' && env.msg === MALFORMED;
}

// ---- per-case structural assertion + normalization ------------------------
async function runCase(c) {
  const endpoint = `${c.service}/${c.method}`;
  if (c.mode === 'echo') {
    const obs = await post(c.service, c.method, c.body, true);
    if (c.reject) {
      assert.equal(obs.status, 400, `${c.case}: over-limit/fractional integer must reject 400 (side ${side})`);
      const env = errorEnvelope(obs);
      assert.equal(env.code, 'malformed', `${c.case}: rejection code must be malformed (side ${side})`);
      assert.equal(env.msg, MALFORMED, `${c.case}: rejection must be the decode error (side ${side}): ${env.msg}`);
      return { case: c.case, group: c.group, leaf: GROUP_LEAF[c.group], endpoint, mode: 'echo', codec_decision: 'reject', status: obs.status, code: env.code, msg: env.msg };
    }
    assert.equal(obs.status, 200, `${c.case}: integral lexical form must be accepted 200 (side ${side}): ${obs.text.slice(0, 160)}`);
    assert.ok(obs.json && typeof obs.json === 'object', `${c.case}: expected a Room JSON body`);
    const got = Number(obs.json.empty_timeout);
    assert.equal(got, c.echo, `${c.case}: launched MAIN must decode emptyTimeout to exactly ${c.echo}, got ${got} (side ${side})`);
    return { case: c.case, group: c.group, leaf: GROUP_LEAF[c.group], endpoint, mode: 'echo', codec_decision: 'accept', status: obs.status, empty_timeout: got };
  }
  // no-auth codec decision
  const obs = await post(c.service, c.method, c.body, false);
  const env = errorEnvelope(obs);
  const rejected = isDecodeReject(obs.status, env);
  if (c.decoded === false) {
    assert.ok(rejected, `${c.case}: codec must REJECT before auth with the malformed-decode sentinel (side ${side}); got ${obs.status} ${obs.text.slice(0, 160)}`);
  } else {
    // The codec accepted the body. A later layer produced the response: the auth gate
    // (401 unauthenticated) for most services, OR a pre-auth business validation
    // (400 invalid_argument, e.g. Ingress "invalid ingress type"). Both are codec
    // ACCEPTANCE; the exact {status,code,msg} is compared cross-side byte-for-byte.
    assert.ok(!rejected, `${c.case}: codec must ACCEPT the body (not the malformed-decode sentinel) (side ${side}); got ${obs.status} ${obs.text.slice(0, 160)}`);
  }
  return { case: c.case, group: c.group, leaf: GROUP_LEAF[c.group], endpoint, mode: 'no-auth', codec_decision: rejected ? 'reject' : 'accept', status: obs.status, code: env.code, msg: env.msg };
}

async function scenario() {
  const contract = [];
  for (const c of CASES) {
    contract.push(await runCase(c));
  }
  return contract;
}

try {
  report.contract = await scenario();
  const groups = {};
  for (const r of report.contract) {
    const g = (groups[r.group] ||= { accepted: 0, rejected: 0, echo_accepted: 0, echo_rejected: 0, cases: 0 });
    g.cases += 1;
    if (r.mode === 'echo') {
      if (r.codec_decision === 'accept') g.echo_accepted += 1; else g.echo_rejected += 1;
    } else if (r.codec_decision === 'accept') g.accepted += 1;
    else g.rejected += 1;
  }
  report.diagnostics.groups = groups;
  report.diagnostics.case_count = report.contract.length;
  report.diagnostics.leaf_coverage = Object.fromEntries(
    Object.entries(GROUP_LEAF).map(([g, leaf]) => [leaf, report.contract.filter((r) => r.group === g).map((r) => r.case)]));
  // Anti-vacuous structural sanity: every group must exercise BOTH a codec rejection
  // (the malformed-decode sentinel) and a codec acceptance (decoded past the codec, whether
  // it then hit 401 auth, 400 business or echo-200). An all-accept or all-reject contract
  // cannot pass.
  for (const [g, s] of Object.entries(groups)) {
    const accepts = s.accepted + s.echo_accepted;
    const rejects = s.rejected + s.echo_rejected;
    assert.ok(accepts > 0, `group ${g} must exercise at least one codec acceptance`);
    assert.ok(rejects > 0, `group ${g} must exercise at least one codec rejection`);
  }
  // The integer-lexical group must additionally prove VALUE fidelity (echo-200 with exact value).
  assert.ok(groups['integer-lexical'].echo_accepted >= 3, 'integer-lexical must echo at least 3 exact decoded values');
  assert.equal(report.contract.length, CASES.length, 'every inventory case must produce a contract record');
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  report.errors.push({ name: error.name, message: String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>') });
  process.exitCode = 1;
} finally {
  await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ status: report.status, side, cases: report.contract.length }));
}