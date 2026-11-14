/** SPEC-0077 C23 — launched-MAIN embedded TURN consumer stimulus (raw sockets).
 *
 * Drives ONE launched MAIN (side under test) over the real embedded TURN
 * listeners with raw STUN/PROXY wire bytes — no media, no TLS certificate
 * (turn.external_tls=true makes the TLS-port listener plain TCP, the listener
 * intended for a TLS terminator), and NO change to the official SDK.
 *
 * Observables (all deterministic):
 *  TCP test A — connect to the TURN TLS-port and send a bare STUN Binding
 *    Request (no PROXY header).
 *      proxy_protocol=false           -> Binding Success Response.
 *      proxy_protocol=true + trusted  -> REQUIRE reads the first STUN bytes as
 *                                        a PROXY header, fails, closes: no reply.
 *      proxy_protocol=true + untrusted-> REJECT closes immediately: no reply.
 *  TCP test B — connect, send a PROXY v1 header naming source PROXY_SRC_IP:port
 *    followed by a STUN Binding Request.
 *      proxy_protocol=false           -> "PROXY..." is not a STUN frame: no reply.
 *      proxy_protocol=true + trusted  -> header parsed; Binding Success Response
 *                                        whose XOR-MAPPED-ADDRESS == PROXY_SRC_IP:port
 *                                        (proves the forwarded source was used).
 *      proxy_protocol=true + untrusted-> REJECT: no reply.
 *  UDP test — send a STUN Binding Request datagram to the TURN UDP port.
 *      enabled=true  -> Binding Success Response.
 *      enabled=false -> no listener: no reply.
 *
 * The probe ONLY emits the stimulus and records wire observations. The launcher
 * asserts each side independently against its profile contract AND compares the
 * two sides. The XOR-MAPPED-ADDRESS for a direct (non-PROXY) Binding legitimately
 * reflects the ephemeral loopback source port, so for non-PROXY replies the
 * launcher compares only the IP family / response presence, not the ephemeral
 * port (explicit scope boundary; the PROXY reply port IS compared because it is
 * the fixed forwarded source port, not an ephemeral one).
 */
import assert from 'node:assert/strict';
import net from 'node:net';
import dgram from 'node:dgram';
import { randomBytes } from 'node:crypto';
import { writeFile } from 'node:fs/promises';

const side = process.env.SIDE;
const profile = process.env.PROFILE;
const outPath = process.env.OUTPUT;
const HOST = process.env.TURN_HOST || '127.0.0.1';
const TCP_PORT = process.env.TURN_TCP_PORT ? Number(process.env.TURN_TCP_PORT) : 0;
const UDP_PORT = process.env.TURN_UDP_PORT ? Number(process.env.TURN_UDP_PORT) : 0;
const PROXY_SRC_IP = process.env.PROXY_SRC_IP || '203.0.113.9';
const PROXY_SRC_PORT = Number(process.env.PROXY_SRC_PORT || '55000');
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS || '3000');
assert.ok(side && profile && outPath, 'SIDE/PROFILE/OUTPUT required');

const COOKIE = 0x2112a442;
const cookieBytes = Buffer.from([0x21, 0x12, 0xa4, 0x42]);

function bindingRequest() {
  const b = Buffer.alloc(20);
  b.writeUInt16BE(0x0001, 0); // Binding Request
  b.writeUInt16BE(0, 2); // length 0
  cookieBytes.copy(b, 4);
  randomBytes(12).copy(b, 8);
  return b;
}

function proxyV1Header() {
  // HAProxy v1 text line: PROXY TCP4 <src> <dst> <sport> <dport>\r\n
  return Buffer.from(
    `PROXY TCP4 ${PROXY_SRC_IP} 198.51.100.1 ${PROXY_SRC_PORT} 443\r\n`, 'ascii');
}

// Parse a STUN Binding Success Response; return {type, mapped} or null.
function parseResponse(buf) {
  if (buf.length < 20) return null;
  const type = buf.readUInt16BE(0);
  const len = buf.readUInt16BE(2);
  if (buf.readUInt32BE(4) !== COOKIE) return null;
  if (buf.length < 20 + len) return null;
  let mapped = null;
  let off = 20;
  const end = 20 + len;
  while (off + 4 <= end) {
    const atype = buf.readUInt16BE(off);
    const alen = buf.readUInt16BE(off + 2);
    const val = buf.subarray(off + 4, off + 4 + alen);
    if (atype === 0x0020 && alen >= 8) { // XOR-MAPPED-ADDRESS
      const family = val[1];
      const port = val.readUInt16BE(2) ^ (COOKIE >>> 16);
      if (family === 0x01) {
        const ip = Buffer.from([0, 1, 2, 3].map((i) => val[4 + i] ^ cookieBytes[i]));
        mapped = `${ip.join('.')}:${port}`;
      } else if (family === 0x02 && alen >= 20) {
        const x = Buffer.alloc(16);
        cookieBytes.copy(x, 0);
        buf.subarray(8, 20).copy(x, 4); // cookie || txid
        const ip = Buffer.from(Array.from({ length: 16 }, (_, i) => val[4 + i] ^ x[i]));
        const groups = [];
        for (let i = 0; i < 16; i += 2) groups.push(ip.readUInt16BE(i).toString(16));
        mapped = `[${groups.join(':')}]:${port}`;
      }
    }
    off += 4 + alen;
    off = (off + 3) & ~3; // 4-byte padding
  }
  return { type, mapped, mapped_ip: mapped ? mapped.replace(/:\d+$/, '').replace(/^\[|\]$/g, '') : null };
}

// One TCP attempt: optionally prefix a PROXY header, then a Binding Request.
function tcpAttempt(sendProxy) {
  return new Promise((resolve) => {
    const obs = { transport: 'tcp', proxy_sent: sendProxy, got_response: false, mapped: null, mapped_ip: null, closed: false, timeout: false, error: null };
    const sock = net.connect({ host: HOST, port: TCP_PORT });
    let buf = Buffer.alloc(0);
    let settled = false;
    const done = () => { if (!settled) { settled = true; sock.destroy(); resolve(obs); } };
    const timer = setTimeout(() => { obs.timeout = true; done(); }, TIMEOUT_MS);
    sock.on('connect', () => {
      const req = bindingRequest();
      sock.write(sendProxy ? Buffer.concat([proxyV1Header(), req]) : req);
    });
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      const r = parseResponse(buf);
      if (r && r.type === 0x0101) {
        obs.got_response = true; obs.mapped = r.mapped; obs.mapped_ip = r.mapped_ip;
        clearTimeout(timer); done();
      }
    });
    sock.on('close', () => { obs.closed = true; clearTimeout(timer); done(); });
    sock.on('error', (e) => { obs.error = String(e.code || e.message); clearTimeout(timer); done(); });
  });
}

function udpAttempt() {
  return new Promise((resolve) => {
    const obs = { transport: 'udp', got_response: false, mapped: null, mapped_ip: null, timeout: false, error: null };
    const sock = dgram.createSocket('udp4');
    let settled = false;
    const done = () => { if (!settled) { settled = true; try { sock.close(); } catch {} resolve(obs); } };
    const timer = setTimeout(() => { obs.timeout = true; done(); }, TIMEOUT_MS);
    sock.on('message', (msg) => {
      const r = parseResponse(msg);
      if (r && r.type === 0x0101) { obs.got_response = true; obs.mapped = r.mapped; obs.mapped_ip = r.mapped_ip; }
      clearTimeout(timer); done();
    });
    sock.on('error', (e) => { obs.error = String(e.code || e.message); clearTimeout(timer); done(); });
    sock.send(bindingRequest(), UDP_PORT, HOST, (err) => { if (err) { obs.error = String(err.code || err.message); clearTimeout(timer); done(); } });
  });
}

const report = { status: 'RUNNING', side, profile, host: HOST, tcp_port: TCP_PORT, udp_port: UDP_PORT, proxy_src: `${PROXY_SRC_IP}:${PROXY_SRC_PORT}`, observations: {}, errors: [] };
try {
  if (TCP_PORT) {
    report.observations.tcp_raw = await tcpAttempt(false);
    report.observations.tcp_proxy = await tcpAttempt(true);
  }
  if (UDP_PORT) {
    report.observations.udp = await udpAttempt();
  }
  assert.ok(Object.keys(report.observations).length > 0, 'no TURN port provided; stimulus did not fire');
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL';
  report.errors.push({ name: error.name, message: String(error.message) });
  process.exitCode = 1;
} finally {
  await writeFile(outPath, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ status: report.status, side, profile, observations: report.observations }));
}