/** SPEC-0077 C19 — launched-MAIN /rtc route HTTP-boundary differential.
 *
 * Drives ONE launched MAIN (side under test) over the REAL signaling HTTP boundary
 * (/rtc, /rtc/validate, /rtc/v1, /rtc/v1/validate) with raw Node http.request — no
 * browser, no media plane, no official SDK mutation. The official SDK is unchanged
 * and NOT used here: raw HTTP is the only way to put duplicate/empty/malformed
 * access_token pairs, arbitrary methods and hand-built WrappedJoinRequest bytes on
 * the wire. @livekit/protocol (unchanged) encodes the v1 wrapped join protobufs.
 *
 * Upstream contract (locked v1.13.10, e5655ac2):
 *   - rtcservice.go SetupRoutes uses mux.HandleFunc -> ANY HTTP method is accepted.
 *     serve() answers a non-WebSocket request with a bare 404 BEFORE inspecting the
 *     token or join params; v0Validate/v1Validate run validateInternal regardless of
 *     method. A GET-only router would 405 on POST/PUT/DELETE/OPTIONS/PATCH (the C19
 *     Rust gap, fixed by registering the four routes with axum `any`).
 *   - auth.go APIKeyAuthMiddleware: Authorization Bearer header takes precedence over
 *     the access_token query; r.URL.Query().Get is first-value-wins; ParseQuery drops
 *     pairs with invalid percent escapes and segments containing ';'. Validate paths
 *     always carry Access-Control-Allow-Origin: *.
 *   - ValidateConnectRequest (utils.go:345): no claim -> 401 "no permissions to access
 *     the room"; roomJoin false -> 401 "permissions denied"; empty identity -> 400
 *     "identity cannot be empty"; over-limit identity/room -> 400 "... length exceeds
 *     limits: max length N"; empty room -> 400 "no room name"; strict v0 attributes
 *     decode failure -> 400 "cannot decode attributes".
 *   - v1 wrapped join (rtcservice.go validateInternal): missing join_request -> 400
 *     "join_request is required"; bad base64 / wrapped proto / inner proto / gzip read
 *     -> the matching 400 sentinel; gzip bomb over DefaultMaxHeaderBytes -> 400 "join
 *     request too large".
 *   - A valid WebSocket join upgrades to 101 with the deterministic RFC6455
 *     Sec-WebSocket-Accept = base64(SHA1(key + GUID)); the gate runs BEFORE the upgrade.
 *
 * Each side must INDEPENDENTLY satisfy this structural contract (probe PASS), then the
 * two normalized contracts must be deeply equal. Equal-but-wrong does not pass: a
 * GET-only router 405s where Go 404/401s, failing the per-side method assertion before
 * any comparison; a router that fakes 101 without the real accept header fails the
 * upgrade assertion. The configured identity/room limit is echoed in the rejection
 * text, so a server that ignores the limit config could not satisfy a small-limit
 * profile (config_projection evidence).
 *
 * Normalization: every observed field is deterministic — status, the exact error/success
 * body, Access-Control-Allow-Origin, and (for WS) the Sec-WebSocket-Accept. HEAD bodies
 * are empty by HTTP spec. The full record is compared cross-side byte-for-byte.
 */
import assert from 'node:assert/strict';
import { createHmac, createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import http from 'node:http';
import { writeFile } from 'node:fs/promises';
import { JoinRequest, WrappedJoinRequest, WrappedJoinRequest_Compression, ConnectionSettings } from '@livekit/protocol';

const url = process.env.RTC_URL;
const side = process.env.SIDE;
const profile = process.env.PROFILE;
const outPath = process.env.OUTPUT;
assert.ok(url && side && profile && outPath, 'RTC_URL/SIDE/PROFILE/OUTPUT required');
const KEY = process.env.LIVEKIT_API_KEY || 'devkey';
const SECRET = process.env.LIVEKIT_API_SECRET || 'owned-room-service-fixture-secret-0123456789';
const ID_LIMIT = Number(process.env.IDENTITY_LIMIT || 256);
const ROOM_LIMIT = Number(process.env.ROOM_LIMIT || 256);
assert.ok(Number.isFinite(ID_LIMIT) && Number.isFinite(ROOM_LIMIT), 'limits must be integers');
const base = url.replace(/\/$/, '');

const L = {
  rtc0: 'HTTP:ANY /rtc',
  val0: 'HTTP:ANY /rtc/validate',
  rtc1: 'HTTP:ANY /rtc/v1',
  val1: 'HTTP:ANY /rtc/v1/validate',
};
const JOIN_ROOM = 'joinroom'; // 8 bytes, within every profile room limit (>=10)
const WS_KEY = 'dGhlIHNhbXBsZSBub25jZQ=='; // RFC6455 §1.3 example key
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const WS_ACCEPT = createHash('sha1').update(WS_KEY + WS_GUID).digest('base64');
assert.equal(WS_ACCEPT, 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=', 'RFC6455 example accept self-check');

const report = { status: 'RUNNING', side, profile, identity_limit: ID_LIMIT, room_limit: ROOM_LIMIT,
  contract: [], errors: [], diagnostics: {} };

// ---- token minting (raw HS256, full control over claims/signature) ----------
const b64url = (x) => Buffer.from(JSON.stringify(x)).toString('base64url');
const now = () => Math.floor(Date.now() / 1000);
function jwt({ iss = KEY, sub = 'u', exp = now() + 3600, video = {}, secret = SECRET, sig } = {}) {
  const head = b64url({ alg: 'HS256', typ: 'JWT' });
  const body = b64url({ iss, sub, exp, video });
  const signature = sig ?? createHmac('sha256', secret).update(`${head}.${body}`).digest('base64url');
  return `${head}.${body}.${signature}`;
}
const joinVideo = (room = JOIN_ROOM, extra = {}) => ({ roomJoin: true, room, canPublish: true, canSubscribe: true, canUpdateOwnMetadata: true, ...extra });
const validToken = (room = JOIN_ROOM, extra = {}) => jwt({ sub: 'u', video: joinVideo(room, extra) });

// ---- v1 wrapped join protobufs (@livekit/protocol, unchanged) ---------------
// Both servers decode join_request with PADDED base64url (Go base64.URLEncoding,
// Rust base64 URL_SAFE). Node's toString('base64url') is UNPADDED, so emit standard
// base64 (which keeps '=' padding) and translate +/ to -_ to match the wire format.
const b64urlPad = (buf) => Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
const innerJoin = new JoinRequest({ connectionSettings: new ConnectionSettings({ autoSubscribe: true }) }).toBinary();
const wrap = (compression, joinRequest) => b64urlPad(new WrappedJoinRequest({ compression, joinRequest }).toBinary());
const JOIN_NONE = wrap(WrappedJoinRequest_Compression.NONE, innerJoin);
const JOIN_GZIP = wrap(WrappedJoinRequest_Compression.GZIP, gzipSync(Buffer.from(innerJoin)));
const JOIN_GZIP_BAD = wrap(WrappedJoinRequest_Compression.GZIP, Buffer.from('this is not gzip'));
const JOIN_GZIP_BIG = wrap(WrappedJoinRequest_Compression.GZIP, gzipSync(Buffer.alloc(1048577))); // > DefaultMaxHeaderBytes
const JOIN_BAD_WRAP = b64urlPad(Buffer.from([0x0a, 0x7f])); // truncated length-delimited field
const JOIN_BAD_INNER = wrap(WrappedJoinRequest_Compression.NONE, Buffer.from([0x0a, 0x7f]));

// ---- raw HTTP / WS driver ---------------------------------------------------
function drive(method, path, opts = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(base);
    const headers = { ...(opts.headers || {}) };
    if (opts.ws) {
      headers.Connection = 'Upgrade';
      headers.Upgrade = 'websocket';
      headers['Sec-WebSocket-Key'] = WS_KEY;
      headers['Sec-WebSocket-Version'] = '13';
    }
    const req = http.request({ hostname: u.hostname, port: u.port, path, method, headers });
    req.on('upgrade', (res, socket) => {
      const accept = res.headers['sec-websocket-accept'] ?? null;
      socket.destroy();
      resolve({ status: 101, body: '', cors: res.headers['access-control-allow-origin'] ?? null, accept });
    });
    req.on('response', (res) => {
      let b = '';
      res.on('data', (c) => { b += c; });
      res.on('end', () => resolve({
        status: res.statusCode, body: method === 'HEAD' ? '' : b,
        cors: res.headers['access-control-allow-origin'] ?? null, accept: null,
      }));
    });
    req.on('error', reject);
    req.setTimeout(10000, () => req.destroy(new Error('request timeout')));
    req.end();
  });
}

// ---- case inventory ---------------------------------------------------------
const NO_PERM = 'no permissions to access the room';
const METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'];
const ROUTES = [
  { path: '/rtc', leaf: L.rtc0, connect: true },
  { path: '/rtc/validate', leaf: L.val0, connect: false },
  { path: '/rtc/v1', leaf: L.rtc1, connect: true },
  { path: '/rtc/v1/validate', leaf: L.val1, connect: false },
];

function buildCases() {
  const cases = [];
  // G1 method-any: mux.HandleFunc accepts ANY method (the C19 fix). Non-WS, no auth.
  for (const r of ROUTES) {
    for (const m of METHODS) {
      cases.push({
        group: 'method-any', leaf: r.leaf, case: `${m} ${r.path}`, endpoint: r.path, method: m,
        path: r.path, ws: false,
        expect: r.connect
          ? { status: 404, body: '', cors: null }
          : { status: 401, body: m === 'HEAD' ? '' : NO_PERM, cors: '*' },
      });
    }
  }
  // G2 v0-validate-auth: auth boundary on /rtc/validate (GET, pure HTTP).
  const v0 = (caseId, rawQuery, headers, expect) => cases.push({
    group: 'v0-validate-auth', leaf: L.val0, case: caseId, endpoint: '/rtc/validate', method: 'GET',
    path: `/rtc/validate?${rawQuery}`, headers, ws: false, expect,
  });
  const ok = { status: 200, body: 'success', cors: '*' };
  const unauth = (body) => ({ status: 401, body, cors: '*' });
  v0('no-token', '', undefined, unauth(NO_PERM));
  v0('garbage-query', 'access_token=garbage', undefined, unauth('invalid authorization token'));
  v0('wrong-api-key', `access_token=${jwt({ iss: 'unknownkey', video: joinVideo() })}`, undefined, unauth('invalid API key'));
  v0('non-bearer-header', '', { Authorization: 'Basic xyz' }, unauth('invalid authorization header. Must start with Bearer '));
  v0('bad-signature', `access_token=${jwt({ video: joinVideo(), sig: 'aaaa' })}`, undefined,
    unauth('invalid authorization token: token signature is invalid: signature is invalid'));
  v0('expired', `access_token=${jwt({ exp: now() - 3600, video: joinVideo() })}`, undefined,
    unauth('invalid authorization token: token has invalid claims: token is expired'));
  v0('valid-bearer-header', `room=${JOIN_ROOM}`, { Authorization: `Bearer ${validToken()}` }, ok);
  v0('valid-query-token', `access_token=${validToken()}`, undefined, ok);
  v0('header-precedence', 'access_token=garbage', { Authorization: `Bearer ${validToken()}` }, ok);
  v0('duplicate-first-valid', `access_token=${validToken()}&access_token=garbage`, undefined, ok);
  v0('duplicate-first-invalid', `access_token=garbage&access_token=${validToken()}`, undefined, unauth('invalid authorization token'));
  v0('empty-first', `access_token=&access_token=${validToken()}`, undefined, unauth(NO_PERM));
  v0('malformed-last', `access_token=${validToken()}&access_token=%zz`, undefined, ok);
  v0('malformed-first', `access_token=%zz&access_token=${validToken()}`, undefined, ok);
  v0('unescaped-semicolon', `access_token=${validToken()}&x=1;access_token=garbage`, undefined, ok);
  v0('roomjoin-false', `access_token=${jwt({ video: { roomJoin: false, room: JOIN_ROOM } })}`, undefined, unauth('permissions denied'));

  // G3 v0-validate-params: param errors with a valid token (GET).
  const p0 = (caseId, rawQuery, expect) => cases.push({
    group: 'v0-validate-params', leaf: L.val0, case: caseId, endpoint: '/rtc/validate', method: 'GET',
    path: `/rtc/validate?${rawQuery}`, ws: false, expect,
  });
  const bad = (body) => ({ status: 400, body, cors: '*' });
  p0('no-room', `access_token=${jwt({ video: joinVideo('') })}`, bad('no room name'));
  p0('identity-empty', `access_token=${jwt({ sub: '', video: joinVideo() })}`, bad('identity cannot be empty'));
  p0('identity-over', `access_token=${jwt({ sub: 'a'.repeat(ID_LIMIT + 1), video: joinVideo() })}`,
    bad(`participant identity length exceeds limits: max length ${ID_LIMIT}`));
  p0('room-over', `access_token=${jwt({ video: joinVideo('a'.repeat(ROOM_LIMIT + 1)) })}`,
    bad(`room name length exceeds limits: max length ${ROOM_LIMIT}`));
  p0('identity-at-limit', `access_token=${jwt({ sub: 'a'.repeat(ID_LIMIT), video: joinVideo() })}`, ok);
  p0('room-at-limit', `access_token=${jwt({ video: joinVideo('a'.repeat(ROOM_LIMIT)) })}`, ok);
  p0('malformed-attributes-strict', `access_token=${validToken()}&attributes=!`, bad('cannot decode attributes'));
  p0('valid-attributes', `access_token=${validToken()}&attributes=${b64url({ k: 'v' })}`, ok);
  // ValidateConnectRequest's own attribute/metadata permission branch returns
  // rtc.ErrPermissionDenied ("no permissions to access the room"), NOT the
  // service.ErrPermissionDenied ("permissions denied") used by EnsureJoinPermission.
  p0('attributes-no-permission', `access_token=${validToken(JOIN_ROOM, { canUpdateOwnMetadata: false })}&attributes=${b64url({ k: 'v' })}`,
    unauth(NO_PERM));

  // G4 v1-validate-join: wrapped join on /rtc/v1/validate (GET).
  const v1 = (caseId, rawQuery, expect) => cases.push({
    group: 'v1-validate-join', leaf: L.val1, case: caseId, endpoint: '/rtc/v1/validate', method: 'GET',
    path: `/rtc/v1/validate?${rawQuery}`, ws: false, expect,
  });
  const t = validToken();
  v1('missing-join', `access_token=${t}`, bad('join_request is required'));
  v1('valid-join-none', `access_token=${t}&join_request=${JOIN_NONE}`, ok);
  v1('valid-join-gzip', `access_token=${t}&join_request=${JOIN_GZIP}`, ok);
  v1('bad-base64', `access_token=${t}&join_request=!!!`, bad('cannot base64 decode wrapped join request'));
  v1('bad-wrapped-proto', `access_token=${t}&join_request=${JOIN_BAD_WRAP}`, bad('cannot unmarshal wrapped join request'));
  v1('bad-inner-proto', `access_token=${t}&join_request=${JOIN_BAD_INNER}`, bad('cannot unmarshal join request'));
  v1('gzip-read-failed', `access_token=${t}&join_request=${JOIN_GZIP_BAD}`, bad('cannot read decompressed join request'));
  v1('gzip-too-large', `access_token=${t}&join_request=${JOIN_GZIP_BIG}`, bad('join request too large'));
  v1('empty-join-first', `access_token=${t}&join_request=&join_request=${JOIN_NONE}`, bad('join_request is required'));
  v1('valid-join-first', `access_token=${t}&join_request=${JOIN_NONE}&join_request=!!!`, ok);

  // G5 connect-ws: real WebSocket boundary on /rtc and /rtc/v1. The gate runs before
  // the 101 upgrade; a valid join upgrades with the deterministic RFC6455 accept.
  const ws = (caseId, leaf, endpoint, rawQuery, expect) => cases.push({
    group: 'connect-ws', leaf, case: caseId, endpoint, method: 'GET',
    path: `${endpoint}?${rawQuery}`, ws: true, expect,
  });
  const wsErr = (status, body) => ({ status, body, cors: null, accept: null });
  const wsOk = { status: 101, body: '', cors: null, accept: WS_ACCEPT };
  // v0 connect
  ws('v0-non-ws', L.rtc0, '/rtc', `access_token=${t}`, { status: 404, body: '', cors: null, accept: null }); // overridden ws:false below
  cases[cases.length - 1].ws = false; // non-WS control on the connect route
  ws('v0-no-token', L.rtc0, '/rtc', '', wsErr(401, NO_PERM));
  ws('v0-garbage-token', L.rtc0, '/rtc', 'access_token=garbage', wsErr(401, 'invalid authorization token'));
  ws('v0-no-room', L.rtc0, '/rtc', `access_token=${jwt({ video: joinVideo('') })}`, wsErr(400, 'no room name'));
  ws('v0-identity-over', L.rtc0, '/rtc', `access_token=${jwt({ sub: 'a'.repeat(ID_LIMIT + 1), video: joinVideo() })}`,
    wsErr(400, `participant identity length exceeds limits: max length ${ID_LIMIT}`));
  ws('v0-valid-upgrade', L.rtc0, '/rtc', `access_token=${validToken('wsroom0')}`, wsOk);
  // v1 connect
  ws('v1-no-token', L.rtc1, '/rtc/v1', '', wsErr(401, NO_PERM));
  ws('v1-missing-join', L.rtc1, '/rtc/v1', `access_token=${t}`, wsErr(400, 'join_request is required'));
  ws('v1-bad-base64', L.rtc1, '/rtc/v1', `access_token=${t}&join_request=!!!`, wsErr(400, 'cannot base64 decode wrapped join request'));
  ws('v1-valid-upgrade', L.rtc1, '/rtc/v1', `access_token=${validToken('wsroom1')}&join_request=${JOIN_NONE}`, wsOk);
  return cases;
}

const CASES = buildCases();

function assertObs(c, obs) {
  const e = c.expect;
  assert.equal(obs.status, e.status, `${c.case}: status (side ${side}) got ${obs.status} body=${obs.body?.slice(0, 120)}`);
  if (e.body !== undefined) assert.equal(obs.body, e.body, `${c.case}: body (side ${side})`);
  if (e.cors !== undefined) assert.equal(obs.cors, e.cors, `${c.case}: cors (side ${side})`);
  if (e.accept !== undefined) assert.equal(obs.accept, e.accept, `${c.case}: sec-websocket-accept (side ${side})`);
}

async function scenario(contract) {
  for (const c of CASES) {
    const obs = await drive(c.method, c.path, { headers: c.headers, ws: c.ws });
    assertObs(c, obs);
    contract.push({
      case: c.case, group: c.group, leaf: c.leaf, endpoint: c.endpoint, method: c.method, ws: c.ws,
      status: obs.status, body: obs.body, cors: obs.cors, accept: obs.accept,
    });
  }
  return contract;
}

try {
  await scenario(report.contract);
  const groups = {};
  for (const r of report.contract) {
    const g = (groups[r.group] ||= { cases: 0, statuses: {} });
    g.cases += 1;
    g.statuses[r.status] = (g.statuses[r.status] || 0) + 1;
  }
  report.diagnostics.groups = groups;
  report.diagnostics.case_count = report.contract.length;
  report.diagnostics.leaf_coverage = Object.fromEntries(
    Object.values(L).map((leaf) => [leaf, report.contract.filter((r) => r.leaf === leaf).map((r) => r.case)]));
  // Anti-vacuous structural sanity:
  //  - method-any must show BOTH the connect 404 and the validate 401 (not a blanket 405).
  const ma = report.contract.filter((r) => r.group === 'method-any');
  assert.ok(ma.some((r) => r.status === 404), 'method-any must exercise a connect 404');
  assert.ok(ma.some((r) => r.status === 401), 'method-any must exercise a validate 401');
  assert.ok(!ma.some((r) => r.status === 405), 'method-any must NEVER 405 (mux.HandleFunc accepts any method)');
  //  - the auth group must contain both accepts (200) and rejects (401).
  const auth = report.contract.filter((r) => r.group === 'v0-validate-auth');
  assert.ok(auth.some((r) => r.status === 200), 'auth group must exercise an acceptance');
  assert.ok(auth.some((r) => r.status === 401), 'auth group must exercise a rejection');
  //  - the params group must contain both 200 and 400 (boundary actually exercised).
  const params = report.contract.filter((r) => r.group === 'v0-validate-params');
  assert.ok(params.some((r) => r.status === 200), 'params group must exercise an acceptance');
  assert.ok(params.some((r) => r.status === 400), 'params group must exercise a rejection');
  //  - the v1 join group must contain both 200 and 400.
  const join = report.contract.filter((r) => r.group === 'v1-validate-join');
  assert.ok(join.some((r) => r.status === 200), 'v1 join group must exercise an acceptance');
  assert.ok(join.some((r) => r.status === 400), 'v1 join group must exercise a rejection');
  //  - connect-ws must contain a real 101 upgrade with the RFC6455 accept AND a pre-upgrade error.
  const cw = report.contract.filter((r) => r.group === 'connect-ws');
  assert.ok(cw.some((r) => r.status === 101 && r.accept === WS_ACCEPT), 'connect-ws must produce a real RFC6455 101 upgrade');
  assert.ok(cw.some((r) => r.status >= 400 && r.status < 500), 'connect-ws must exercise a pre-upgrade rejection');
  //  - every leaf must be exercised.
  for (const leaf of Object.values(L)) {
    assert.ok(report.diagnostics.leaf_coverage[leaf].length > 0, `leaf ${leaf} must have cases`);
  }
  assert.equal(report.contract.length, CASES.length, 'every inventory case must produce a contract record');
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  report.errors.push({ name: error.name, message: String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>') });
  process.exitCode = 1;
} finally {
  await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ status: report.status, side, profile, cases: report.contract.length }));
}