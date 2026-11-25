// Post-failure attribution probe for a browser media leg. It never changes the
// leg itself: run.mjs has already exited and its evidence JSON is on disk here.
// This script only reads that record and probes the server/TURN ports from the
// harness side, so a reader can tell apart the candidate causes of a leg that
// fails with "could not establish pc connection":
//   1. signaling never reached the server          -> wsHandshake.ok == false
//   2. TURN TLS listener absent/refused            -> turnTlsTcpConnect / tlsHandshake errors
//   3. TURN TLS certificate rejected by Chromium   -> no relay local-candidate + peerCertificate empty
//                                                    while this probe still completes TLS
//   4. ICE never reached relay state               -> localCandidateTypes without "relay"
//   5. runner environment difference               -> chromium flags plus loopback interface dump
// Output goes to output/<leg>-attribution.json inside the existing artifact path.
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import net from 'node:net';
import tls from 'node:tls';
import https from 'node:https';
import os from 'node:os';
import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const output = process.env.INTEROP_OUTPUT_DIR ? resolve(process.env.INTEROP_OUTPUT_DIR) : join(root, 'output');
const LEG_LABELS = ['rust-turn-tls', 'rust-native-https-rsa']; // step 18 and the RSA WSS/TURN TLS leg
const RUNS = Number(process.env.RUNS || 1);
const SIGNAL_URL = process.env.LIVEKIT_URL || 'ws://127.0.0.1:8080';
const TURN_HOST = process.env.TURN_HOST || '127.0.0.1';
const TURN_TLS_PORT = Number(process.env.TURN_TLS_PORT || 5349);
const CA_FILE = process.env.TLS_CA_FILE || join(output, 'tls', 'root.pem');
const SPKI_FILE = process.env.TLS_SPKI_FILE || join(output, 'tls', 'spki-sha256.txt');
const BROWSER_CHANNEL = process.env.BROWSER_CHANNEL || 'chrome';
const CHROMIUM_LOG = process.env.CHROMIUM_LOG || join(output, 'chromium-netlog.log');
// RFC 1918 ranges are replaced in every string field before writing; the same
// placeholder scheme the committed evidence summaries use (RFC 5737 documentation space).
const REDACTED_PREFIX = '198.51.100.';
const PRIVATE_CIDR = /\b(10|192\.168|172\.(1[6-9]|2\d|3[01]))((\.\d{1,3}){1,3})\b/g;
// One mapping for every RFC 1918 form, keeping only the last octet so the committed
// placeholder stays a valid single address instead of a five-octet string.
function redactAddress(match) {
  return REDACTED_PREFIX + match.split('.').pop();
}

function redact(value) {
  if (typeof value === 'string') {
    return value.replace(PRIVATE_CIDR, redactAddress);
  }
  if (Array.isArray(value)) return value.map(redact);
  if (value && typeof value === 'object') {
    const out = {};
    for (const [key, entry] of Object.entries(value)) out[key] = redact(entry);
    return out;
  }
  return value;
}

function tcpProbe(port, host = TURN_HOST) {
  return new Promise(resolveProbe => {
    const startedAt = Date.now();
    const socket = net.connect({ host, port });
    socket.setTimeout(3000);
    socket.on('connect', () => { socket.destroy(); resolveProbe({ ok: true, elapsedMs: Date.now() - startedAt }); });
    socket.on('timeout', () => { socket.destroy(); resolveProbe({ ok: false, error: 'connect-timeout', elapsedMs: Date.now() - startedAt }); });
    socket.on('error', error => resolveProbe({ ok: false, error: `${error.code || error.name}`, elapsedMs: Date.now() - startedAt }));
  });
}

function tlsProbe() {
  return new Promise(resolveProbe => {
    const startedAt = Date.now();
    const ca = tlsCa;
    const socket = tls.connect({ host: TURN_HOST, port: TURN_TLS_PORT, servername: 'turn.localhost', ca, rejectUnauthorized: true }, () => {
      const cert = socket.getPeerCertificate(true);
      const der = cert.raw ? Buffer.from(cert.raw) : undefined;
      const spkiPin = der ? createHash('sha256').update(spkiOf(der)).digest('base64') : undefined;
      resolveProbe({
        ok: true, elapsedMs: Date.now() - startedAt, tlsVersion: socket.getProtocol(),
        cipher: socket.getCipher()?.standardName, authorized: socket.authorized,
        authorizationError: socket.authorizationError ? String(socket.authorizationError) : undefined,
        subject: cert.subject, issuer: cert.issuer, validFrom: cert.valid_from, validTo: cert.valid_to,
        subjectAltNames: cert.subjectaltname, publicKeyType: cert.asymmetricKeyType,
        fingerprint256: cert.fingerprint256, sha256OfLeafDer: der ? createHash('sha256').update(der).digest('hex') : undefined,
        spkiSha256Base64: spkiPin, spkiMatchesHarnessPin: spkiPin === harnessSpki,
      });
      socket.destroy();
    });
    socket.setTimeout(5000);
    socket.on('timeout', () => { socket.destroy(); resolveProbe({ ok: false, error: 'tls-handshake-timeout' }); });
    socket.on('error', error => resolveProbe({ ok: false, error: `${error.code || error.name}: ${error.message}`, elapsedMs: Date.now() - startedAt }));
  });
}

// SubjectPublicKeyInfo DER is the openssl `pkey -pubout -outform DER` blob that
// generate-tls-fixture.mjs pins, so both sides produce the same base64 SHA-256.
function spkiOf(leafDer) {
  const asn = parseDer(leafDer);
  const tbs = asn?.value?.children?.find(child => child.tag === 0x30)?.value?.children;
  const spki = tbs?.find(child => child.tag === 0x30);
  if (!spki) throw new Error('unable to locate TBSCertificate.SubjectPublicKeyInfo');
  return spki.bytes;
}

function parseDer(buffer) {
  let offset = 0;
  function readLength() {
    const first = buffer[offset++];
    if (first < 0x80) return first;
    const count = first & 0x7f;
    let length = 0;
    for (let i = 0; i < count; i++) length = (length << 8) | buffer[offset++];
    return length;
  }
  function readNode() {
    const tag = buffer[offset++];
    const length = readLength();
    const start = offset;
    const end = offset + length;
    const node = { tag, bytes: buffer.subarray(start, end), value: null, children: [] };
    if ((tag & 0x20) !== 0) {
      while (offset < end) node.children.push(readNode());
    } else {
      node.value = buffer.subarray(start, end);
      offset = end;
    }
    return node;
  }
  return readNode();
}

async function wsHandshakeProbe() {
  const target = new URL(SIGNAL_URL);
  const startedAt = Date.now();
  try {
    const key = createHash('sha1').update(String(Math.random())).digest('base64');
    const socket = net.connect({ host: target.hostname, port: Number(target.port) });
    await new Promise((res, rej) => { socket.once('connect', res); socket.once('error', rej); socket.setTimeout(3000, () => rej(new Error('connect-timeout'))); });
    const httpPath = `/rtc/v1/whip/?probe=${encodeURIComponent(key)}`;
    socket.write(`GET ${httpPath} HTTP/1.1\r\nHost: ${target.host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer invalid-probe-token\r\n\r\n`);
    const head = await new Promise((res, rej) => {
      let data = Buffer.alloc(0);
      socket.on('data', chunk => { data = Buffer.concat([data, chunk]); if (data.includes('\r\n\r\n')) res(data.subarray(0, data.indexOf('\r\n\r\n')).toString()); });
      socket.once('error', rej);
      socket.setTimeout(3000, () => rej(new Error('response-timeout')));
    });
    socket.destroy();
    const statusLine = head.split('\r\n')[0];
    return { ok: true, elapsedMs: Date.now() - startedAt, statusLine, upgradeOffered: /upgrade:\s*websocket/i.test(head), note: 'expected rejection without a valid JWT; a TCP-level refusal here means the signaling listener is gone' };
  } catch (error) {
    return { ok: false, error: `${error.code || error.name || 'Error'}: ${error.message}`, elapsedMs: Date.now() - startedAt };
  }
}

function httpsApiProbe() {
  return new Promise(resolveProbe => {
    const target = new URL(SIGNAL_URL.replace(/^ws:/, 'http:').replace(/^wss:/, 'https:'));
    const request = https.get({ host: target.hostname, port: target.port, path: '/', ca: tlsCa, agent: false, rejectUnauthorized: false }, response => {
      resolveProbe({ reachable: true, status: response.statusCode });
      response.resume();
      response.on('end', () => {});
    });
    request.setTimeout(3000, () => request.destroy(new Error('timeout')));
    request.on('error', error => resolveProbe({ reachable: false, error: `${error.code || error.name}` }));
  });
}

// run.mjs names its artifact `${SERVER_LABEL}-${single|dual}[-video[-codec]][-relay][-extended].json`.
// Enumerate the plausible spellings for the legs this probe is attached to.
function legEvidenceNames() {
  const names = [];
  for (const label of LEG_LABELS) {
    for (const mode of ['single', 'dual']) {
      for (const video of ['', '-video', '-video-vp9', '-video-av1', '-video-vp8']) {
        for (const relay of ['', '-relay']) {
          for (const extended of ['', '-extended']) names.push(`${label}-${mode}${video}${relay}${extended}`);
        }
      }
    }
  }
  return names;
}

function summarizeRun(run) {
  const transports = (run.reports || []).map(report => (report.transports || []).map(t => ({
    dtlsState: t.dtlsState, localCandidateType: t.localCandidateType, relayProtocol: t.relayProtocol,
    turnUrl: t.turnUrl, protocol: t.protocol, bytesReceived: t.bytesReceived,
  })));
  return { room: run.room, status: run.status, transports };
}

function summarizeDiagnostics(diagnostics) {
  return (diagnostics || []).map(row => {
    if (row?.error) return { error: row.error };
    return {
      connection: row.connection, iceConnectionState: row.ice?.[0]?.ice ?? row.peers?.[0]?.ice,
      negotiationErrors: (row.negotiationErrors || []).slice(0, 5),
      peers: (row.peers || []).map(peer => ({
        connection: peer.connection, ice: peer.ice, signaling: peer.signaling,
        statsTypes: countBy((peer.stats || []).map(s => s.type)),
        localCandidates: (peer.stats || []).filter(s => s.type === 'local-candidate').map(s => ({
          candidateType: s.candidateType, relayProtocol: s.relayProtocol, protocol: s.protocol,
          port: s.port, url: s.url, ip: s.ip ? redact(s.ip) : undefined, state: s.state, tlsVersion: s.tlsVersion,
        })),
        remoteCandidates: (peer.stats || []).filter(s => s.type === 'remote-candidate').map(s => ({
          candidateType: s.candidateType, relayProtocol: s.relayProtocol, port: s.port, ip: s.ip ? redact(s.ip) : undefined,
        })),
        candidatePairs: (peer.stats || []).filter(s => s.type === 'candidate-pair').map(s => ({
          state: s.state, nominated: s.nominated, bytesSent: s.bytesSent, bytesReceived: s.bytesReceived,
          availableSendBitrate: s.availableOutgoingBitrate,
        })),
        peerCertificate: (peer.stats || []).filter(s => s.type === 'peer-certificate').map(s => ({
          fingerprint: s.fingerprint, fingerprintAlgorithm: s.fingerprintAlgorithm, dtlsRole: s.dtlsRole,
        })),
        dtlsTransports: (peer.stats || []).filter(s => s.type === 'transport').map(s => ({
          dtlsState: s.dtlsState, dtlsRole: s.dtlsRole, bytesSent: s.bytesSent, bytesReceived: s.bytesReceived,
          tlsVersion: s.tlsVersion, srtpCipher: s.srtpSelectedCipherSuite,
        })),
        beforeCloseSnapshotStats: countBy((peer.beforeClose?.stats || []).map(s => s.type)),
        beforeCloseLocalCandidates: (peer.beforeClose?.stats || []).filter(s => s.type === 'local-candidate').map(s => ({
          candidateType: s.candidateType, relayProtocol: s.relayProtocol, url: s.url, port: s.port, state: s.state,
        })),
      })),
    };
  });
}

function countBy(values) {
  const out = {};
  for (const value of values) out[value] = (out[value] || 0) + 1;
  return out;
}

const report = { scenario: 'step18-turn-tls-attribution', status: 'RUNNING',
  generatedAt: new Date().toISOString(),
  scope: 'read-only post-mortem probe; it adds no assertions to the media leg and cannot turn a failure into a pass',
  environment: {
    ci: process.env.CI || '', runnerOs: process.env.RUNNER_OS || '', arch: process.arch, platform: process.platform,
    node: process.version, chromiumChannel: BROWSER_CHANNEL,
    legEnv: { LIVEKIT_URL: SIGNAL_URL, TURN_TLS_PORT, FORCE_RELAY: process.env.FORCE_RELAY || '', TURN_TLS: process.env.TURN_TLS || '', RUNS },
    chromiumLaunchArgs: ['--autoplay-policy=no-user-gesture-required', ...(process.env.TLS_SPKI ? [`--ignore-certificate-errors-spki-list=${process.env.TLS_SPKI}`] : [])],
    interfaces: Object.fromEntries(Object.entries(os.networkInterfaces()).map(([name, rows]) => [name, (rows || []).map(r => ({
      family: r.family, address: redact(r.address), netmask: r.netmask, internal: r.internal }))])),
    routing: (() => {
      const probe = process.platform === 'darwin'
        ? spawnSync('route', ['-n', 'get', '127.0.0.1'], { encoding: 'utf8' })
        : spawnSync('ip', ['route', 'get', '127.0.0.1'], { encoding: 'utf8' });
      return probe.error ? `unavailable: ${probe.error.message}` : (probe.stdout || '').trim().split('\n').slice(0, 8).map(redact).join('\n');
    })(),
  },
};

let tlsCa, harnessSpki;
try {
  tlsCa = await readFile(CA_FILE);
  harnessSpki = (await readFile(SPKI_FILE, 'utf8')).trim();
  report.certificateMaterial = { caFile: CA_FILE, caSha256: createHash('sha256').update(tlsCa).digest('hex'), spkiFile: SPKI_FILE, harnessSpkiPin: harnessSpki };
} catch (error) {
  report.certificateMaterial = { error: `${error.code || error.name}: ${error.message}`, caFile: CA_FILE, spkiFile: SPKI_FILE };
}

const candidates = legEvidenceNames();
const found = [];
for (const name of candidates) {
  try { found.push({ file: name, json: JSON.parse(await readFile(join(output, `${name}.json`), 'utf8')) }); } catch { /* leg-specific file absent */ }
}
if (!found.length) {
  report.legEvidence = { filesChecked: candidates.map(n => n + '.json'), error: 'no leg evidence JSON found; the harness died before its finally block wrote anything' };
} else {
  report.legEvidence = found.map(({ file, json }) => ({
    file, label: json.label, status: json.status, browser: json.browser, sdk: json.sdk,
    startedAt: json.startedAt, finishedAt: json.finishedAt,
    durationMsRequested: json.duration, runsCompleted: (json.runs || []).length, runsExpected: RUNS,
    error: json.error ? redact(String(json.error)) : undefined,
    timings: { wallClockMs: json.startedAt && json.finishedAt ? new Date(json.finishedAt) - new Date(json.startedAt) : undefined },
    runSummaries: (json.runs || []).map(summarizeRun),
    diagnostics: summarizeDiagnostics(json.diagnostics),
    failedProfiles: json.failedProfiles,
    signalDirections: countBy((json.failedSignals || []).map(s => s.direction)),
    offerAnswerSeen: (json.failedSignals || []).filter(s => s.message?.offer || s.message?.answer).map(s => ({ client: s.client, direction: s.direction, kind: s.message?.offer ? 'offer' : 'answer' })),
    trickleCandidates: (json.failedSignals || []).filter(s => s.message?.trickle).map(s => ({ client: s.client, count: (s.message.trickle.candidates || []).length })),
  }));
}

report.harnessSideProbes = {
  signalTcp: await tcpProbe(Number(new URL(SIGNAL_URL).port) || 80, new URL(SIGNAL_URL).hostname),
  signalWsHandshake: await wsHandshakeProbe(),
  signalHttpApi: await httpsApiProbe(),
  turnTlsTcpConnect: await tcpProbe(TURN_TLS_PORT),
  turnTlsHandshake: harnessSpki && tlsCa ? await tlsProbe() : { skipped: 'no CA or SPKI material readable' },
};

try {
  const log = await readFile(CHROMIUM_LOG, 'utf8');
  const lines = log.split('\n');
  const grep = pattern => lines.filter(line => pattern.test(line)).slice(0, 60).map(redact);
  report.chromiumLog = { file: CHROMIUM_LOG, totalLines: lines.length,
    note: 'present only if the leg was launched with CHROMIUM_LOG set; run.mjs does not request a netlog by default',
    turn: grep(/\bturn\b|\bturns\b/i), stun: grep(/stun/i), tls: grep(/tls|certificate|ssl_error|handshake/i),
    relay: grep(/relay/i), networkChanged: grep(/NetworkChangeNotifier|network changed/i) };
} catch (error) {
  report.chromiumLog = { file: CHROMIUM_LOG, unavailable: `${error.code || error.name}: ${error.message}` };
}

report.status = 'COMPLETE';
report.finishedAt = new Date().toISOString();
await writeFile(join(output, 'step18-turn-tls-attribution.json'), JSON.stringify(redact(report), null, 2));
console.log(`Attribution evidence written to output/step18-turn-tls-attribution.json (leg records: ${found.length}, TURN TLS TCP: ${report.harnessSideProbes.turnTlsTcpConnect.ok}, TURN TLS handshake: ${report.harnessSideProbes.turnTlsHandshake.ok ?? report.harnessSideProbes.turnTlsHandshake.skipped})`);
