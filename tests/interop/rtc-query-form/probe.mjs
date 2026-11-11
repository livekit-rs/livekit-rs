import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const env = process.env;
const require = createRequire(resolve(env.INTEROP_DIR, 'package.json'));
const { chromium } = require('playwright');
const { AccessToken, RoomServiceClient } = require('livekit-server-sdk');
const { closeOwnedBrowser, observeChildStderr } = await import(pathToFileURL(resolve(env.INTEROP_DIR, 'owned-browser-exit.mjs')));
const sdkPath = require.resolve('livekit-client');
const sdk = await readFile(sdkPath);
const sdkSha = createHash('sha256').update(sdk).digest('hex');
const sdkPackage = JSON.parse(await readFile(resolve(env.INTEROP_DIR, 'node_modules/livekit-client/package.json'), 'utf8'));
assert.equal(sdkPackage.version, '2.23.0');
const api = new RoomServiceClient(env.RTC_URL, env.LIVEKIT_API_KEY, env.LIVEKIT_API_SECRET);
const result = { status: 'RUNNING', sdk_version: sdkPackage.version, sdk_sha256: sdkSha,
  ws_rows: [], http_rows: [], errors: [], cleanup_errors: [] };
const rooms = new Set();
const html = '<!doctype html><script src="/sdk.js"></script>';
const redact = str => String(str).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<fixture-token>');

async function token(room, identity) {
  const grant = new AccessToken(env.LIVEKIT_API_KEY, env.LIVEKIT_API_SECRET, { identity });
  grant.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  return await grant.toJwt();
}

async function verifyHttp(path, params, label, expected) {
  const res = await fetch(env.RTC_URL + path + '?' + params, { signal: AbortSignal.timeout(3000) });
  const body = await res.text();
  result.http_rows.push({ label, path, status: res.status, body: redact(body).slice(0, 250),
    cors: res.headers.get('access-control-allow-origin'), expected, match: expected === res.status });
}

let host, browserServer, browser, context, finishStderr;
try {
  host = createServer((req, res) => {
    res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'application/javascript' : 'text/html' });
    res.end(req.url === '/sdk.js' ? sdk : html);
  });
  await new Promise(done => host.listen(0, '127.0.0.1', done));
  browserServer = await chromium.launchServer({ headless: true, executablePath: env.OWNED_CHROMIUM,
    args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  finishStderr = observeChildStderr(browserServer.process());
  browser = await chromium.connect(browserServer.wsEndpoint());
  context = await browser.newContext();
  await context.addInitScript(() => {
    const Original = window.WebSocket;
    window.signaling = [];
    window.WebSocket = class extends Original {
      constructor(url, protocols) {
        const original = new URL(url);
        let sent = original.toString();
        if (original.pathname === '/rtc' || original.pathname === '/rtc/v1') {
          if (window.prefix) sent = sent.replace('?', '?' + window.prefix);
          if (window.suffix) sent += window.suffix;
          window.signaling.push({ path: original.pathname, original: original.search,
            mutated: Boolean(window.prefix || window.suffix) });
        }
        super(sent, protocols);
      }
    };
  });
  for (const single of [false, true]) {
    const version = single ? 'v1' : 'v0';
    const path = single ? '/rtc/v1' : '/rtc';
    const controlRoom = 'owned-query-control-' + version;
    let originalQuery;
    for (const profile of ['control', 'duplicate-token', 'malformed-token', 'empty-first-token', 'invalid-first-token']) {
      const room = profile === 'control' ? controlRoom : 'owned-query-' + profile + '-' + version;
      const identity = 'owned-query-' + profile;
      const roomInfo = await api.createRoom({ name: room, emptyTimeout: 90, departureTimeout: 90 });
      rooms.add(room);
      const page = await context.newPage();
      const expected = !profile.endsWith('-first-token');
      const row = { version, profile, room_sid: roomInfo.sid, connected: false, expected };
      result.ws_rows.push(row);
      try {
        await page.goto('http://127.0.0.1:' + host.address().port);
        const jwt = await token(room, identity);
        const suffix = { 'duplicate-token': '&access_token=garbage',
          'malformed-token': '&access_token=%zz' }[profile] || '';
        const prefix = { 'empty-first-token': 'access_token=&',
          'invalid-first-token': 'access_token=garbage&' }[profile] || '';
        const observed = await page.evaluate(async ([url, jwt, single, prefix, suffix]) => {
          window.prefix = prefix;
          window.suffix = suffix;
          window.rtcRoom = new LivekitClient.Room({ singlePeerConnection: single });
          let error;
          try {
            await rtcRoom.connect(url, jwt, { maxRetries: 0, websocketTimeout: 7000 });
          } catch (e) { error = e.message; }
          return { error, sid: rtcRoom.localParticipant.sid, state: rtcRoom.state, signals: window.signaling };
        }, [env.RTC_URL.replace(/^http/, 'ws'), jwt, single, prefix, suffix]);
        assert(observed.signals.length >= 1);
        assert(observed.signals.every(s => s.path === path && s.mutated === Boolean(prefix || suffix)));
        assert(observed.signals.every(s => new URLSearchParams(s.original).get('access_token') === jwt),
          'official SDK supplied the actual valid first token');
        row.connect_error = observed.error ? redact(observed.error) : null;
        row.connected = observed.state === 'connected';
        if (row.connected) {
          assert.match(observed.sid, /^PA_/);
          const actual = await api.getParticipant(room, identity);
          assert.equal(actual.sid, observed.sid);
          row.participant_sid = actual.sid;
        } else if (!expected) {
          assert(!(await api.listParticipants(room)).some(participant => participant.identity === identity),
            'rejected access token must not create a participant');
          row.no_participant = true;
        }
        row.match = row.connected === expected && (expected || row.no_participant);
        if (profile === 'control') {
          assert(row.connected, 'ordinary unchanged official SDK control');
          originalQuery = observed.signals[0].original.slice(1);
          const tokenized = new URLSearchParams(originalQuery);
          assert.equal(tokenized.get('access_token'), jwt);
          if (single) assert(tokenized.get('join_request'));
        }
      } finally {
        try { await page.evaluate(() => window.rtcRoom?.disconnect()); }
        catch (error) { result.cleanup_errors.push('SDK disconnect: ' + redact(error.message)); }
        await page.close();
      }
    }
    const q = new URLSearchParams(originalQuery);
    const actualToken = q.get('access_token');
    q.delete('access_token');
    const options = q.toString();
    const first = 'access_token=' + encodeURIComponent(actualToken);
    const validate = path + '/validate';
    await verifyHttp(validate, [first, options].filter(Boolean).join('&'), 'control', 200);
    await verifyHttp(validate, [first, 'access_token=garbage', options].filter(Boolean).join('&'), 'valid-first', 200);
    await verifyHttp(validate, ['access_token=garbage', first, options].filter(Boolean).join('&'), 'invalid-first', 401);
    await verifyHttp(validate, ['access_token=', first, options].filter(Boolean).join('&'), 'empty-first', 401);
    await verifyHttp(validate, [first, 'access_token=%zz', options].filter(Boolean).join('&'), 'malformed-last', 200);
    await verifyHttp(validate, ['access_token=%zz', first, options].filter(Boolean).join('&'), 'malformed-first', 200);
    await verifyHttp(validate, [first, 'x=1;access_token=garbage', options].filter(Boolean).join('&'), 'unescaped-semicolon', 200);
    if (single) {
      await verifyHttp(validate, [first, options, 'join_request=garbage'].join('&'), 'valid-join-first', 200);
      await verifyHttp(validate, [first, 'join_request=', options].join('&'), 'empty-join-first', 400);
      await verifyHttp(validate, [first, 'join_request=%zz', options].join('&'), 'malformed-join-first', 200);
    } else {
      // Go ParseForm drops malformed pairs before FormValue, so an invalid
      // attributes encoding cannot overwrite an explicitly empty first value.
      await verifyHttp(validate, [first, 'attributes=', 'attributes=!', options].join('&'), 'empty-attributes-first', 200);
      await verifyHttp(validate, [first, 'attributes=%zz', options].join('&'), 'malformed-attributes', 200);
    }
  }
} catch (error) {
  result.errors.push(redact(error.stack || error.message).slice(0, 1500));
} finally {
  if (context) try { await context.close(); } catch (error) { result.cleanup_errors.push('context: ' + error.message); }
  if (browserServer) {
    result.browser = await closeOwnedBrowser(browser, browserServer);
    if (result.browser.exit?.code !== 0 || result.browser.contextsAfter !== 0) result.cleanup_errors.push("missing real browser child exit or live context");

    if (result.browser.failures.length) result.cleanup_errors.push(...result.browser.failures);
    if (finishStderr) {
      const stderr = finishStderr();
      result.browser_stderr = stderr.metadata;
      await writeFile(resolve(env.OUTPUT, '..', 'browser.stderr'), stderr.bytes);
    }
  }
  if (host) await new Promise(done => host.close(done));
  for (const room of rooms) try { await api.deleteRoom(room); }
  catch (error) { result.cleanup_errors.push('owned room: ' + redact(error.message)); }
  assert.equal(createHash('sha256').update(await readFile(sdkPath)).digest('hex'), sdkSha);
  result.status = !result.errors.length && !result.cleanup_errors.length
    && result.ws_rows.length === 10 && result.ws_rows.every(r => r.match)
    && result.http_rows.length === 19 && result.http_rows.every(r => r.match) ? 'PASS' : 'FAIL';
  await writeFile(env.OUTPUT, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  process.exitCode = result.status === 'PASS' ? 0 : 1;
}
