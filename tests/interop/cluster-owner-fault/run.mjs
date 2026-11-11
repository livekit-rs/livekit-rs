import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { createHash, randomInt } from 'node:crypto';
import { open, mkdir, readFile, writeFile } from 'node:fs/promises';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { createServer as createTcpServer } from 'node:net';
import { createSocket } from 'node:dgram';
import { chromium } from 'playwright';
import { AccessToken, RoomServiceClient } from 'livekit-server-sdk';
import { DataPacket, SignalResponse } from '@livekit/protocol';

const { values: opts } = parseArgs({ options: {
  binary: { type: 'string' }, mode: { type: 'string' }, output: { type: 'string' },
  single: { type: 'boolean' }, redis: { type: 'string', default: '/opt/homebrew/bin/redis-server' },
  'reliable-trace': { type: 'boolean' },
} });
assert.ok(opts.binary && opts.output && ['drain', 'kill'].includes(opts.mode));
const out = resolve(opts.output);
const interop = dirname(dirname(fileURLToPath(import.meta.url)));
const require = createRequire(import.meta.url);
const sdkPath = require.resolve('livekit-client');
const sdk = await readFile(sdkPath);
const html = await readFile(join(interop, 'client.html'));
const exposeRoom = 'room = new Room({ adaptiveStream, dynacast: true, singlePeerConnection: single });';
assert.ok(!opts['reliable-trace'] || html.includes(exposeRoom), 'fixture must expose the real SDK room');
const servedHtml = opts['reliable-trace'] ? html.toString().replace(exposeRoom, `${exposeRoom}
    window.interopTraceRoom = room;
    room.on(RoomEvent.Reconnected, () => window.interopRecord('roomReconnected', {
      participantSid: room.localParticipant.sid, attemptingReconnect: room.engine.attemptingReconnect,
    }));
`) : html;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const binary = resolve(opts.binary);
const ports = new Set();
while (ports.size < 7) ports.add(randomInt(30000, 44000));
const [redisPort, ownerHttp, ownerTcp, ownerUdp, survivorHttp, survivorTcp, survivorUdp] = [...ports];
const survivorUrl = 'http://127.0.0.1:' + survivorHttp;
const api = new RoomServiceClient(survivorUrl, 'devkey', 'secret');
const roomName = 'owner-fault-' + Date.now();
const receipt = { mode: opts.mode, single: !!opts.single, room: roomName, status: 'FAIL',
  binary, binarySha256: hash(await readFile(binary)), sdkSha256: hash(sdk), fixtureSha256: hash(html),
  servedFixtureSha256: hash(servedHtml), runnerSha256: hash(await readFile(fileURLToPath(import.meta.url))), processes: {} };
const childProcesses = [];
const sleep = ms => new Promise(r => setTimeout(r, ms));
const exec = promisify(execFile);
await mkdir(out, { mode: 0o700 });

async function redis(...args) {
  const { stdout } = await exec('redis-cli', ['--json', '-p', String(redisPort), ...args]);
  return JSON.parse(stdout);
}
async function until(callback, label, ms = 12000) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await callback()) return;
    await sleep(100);
  }
  throw Error('deadline: ' + label);
}
async function start(name, program, args) {
  const log = await open(join(out, name + '.log'), 'w');
  const child = spawn(program, args, { stdio: ['ignore', log.fd, log.fd] });
  const record = receipt.processes[name] = { pid: child.pid };
  const done = new Promise(r => {
    child.once('error', error => { record.error = error.message; r(); });
    child.once('exit', (code, signal) => { Object.assign(record, { code, signal }); r(); });
  });
  const running = () => child.exitCode === null && child.signalCode === null && !record.error;
  childProcesses.push({ child, record, done, running });
  await log.close();
  return { child, record, done, running };
}
async function portReleased(port, udp) {
  const listener = udp ? createSocket('udp4') : createTcpServer();
  return new Promise(resolve => {
    listener.once('error', () => resolve(false));
    const bound = () => listener.close(() => resolve(true));
    if (udp) listener.bind(port, '127.0.0.1', bound);
    else listener.listen(port, '127.0.0.1', bound);
  });
}
async function startServer(name, http, tcp, udp) {
  const configPath = join(out, name + '.json');
  const config = JSON.stringify({ port: http, bind_addresses: ['127.0.0.1'],
    redis: { address: '127.0.0.1:' + redisPort }, keys: { devkey: 'secret' }, turn: { enabled: false },
    room: { empty_timeout: 120, departure_timeout: 5 },
    rtc: { node_ip: '127.0.0.1', tcp_port: tcp, udp_port: udp, enable_loopback_candidate: true,
      use_external_ip: false, ips: { includes: ['127.0.0.1/32'] } }, logging: { level: 'info' } });
  await writeFile(configPath, config);
  const existing = new Set(await redis('HKEYS', 'nodes'));
  const p = await start(name, binary, ['--config', configPath]);
  p.record.configSha256 = hash(config);
  await until(async () => {
    assert.ok(p.running(), name + ' exited before ready');
    return (await fetch('http://127.0.0.1:' + http + '/').catch(() => null))?.ok;
  }, name + ' readiness');
  const added = (await redis('HKEYS', 'nodes')).filter(id => !existing.has(id));
  assert.equal(added.length, 1);
  p.record.nodeId = added[0];
  return p;
}
function nodeState(hex) {
  let at = 0;
  const raw = Buffer.from(hex, 'hex');
  const read = () => {
    let value = 0n;
    for (let n = 0n; n <= 63n; n += 7n) {
      assert.ok(at < raw.length, 'truncated node protobuf');
      const byte = raw[at++];
      value |= BigInt(byte & 127) << n;
      if (byte < 128) return value;
    }
    throw Error('oversized protobuf varint');
  };
  while (at < raw.length) {
    const tag = Number(read()), wire = tag & 7;
    if (wire === 0) {
      const value = read();
      if (tag >> 3 === 6) return Number(value);
    } else if (wire === 2) {
      const length = Number(read());
      at += length;
    } else if (wire === 1) at += 8;
    else if (wire === 5) at += 4;
    else throw Error('unexpected node protobuf wire type');
  }
  return 0;
}
let host, browser;
let pages = [];
async function joinPeers() {
  pages = [];
  for (let i = 0; i < 2; i++) {
    const page = await browser.newPage();
    pages.push(page);
    await page.addInitScript(trace => {
      window.interopPeerConnections = [];
      window.interopSignalSockets = [];
      window.interopDataChannels = [];
      window.interopWire = [];
      const encoded = data => {
        const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) :
          ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : null;
        return bytes && bytes.length <= 32768 ? Array.from(bytes) : null;
      };
      const stamp = (kind, details) => {
        if (window.interopWire.length >= 10000) {
          window.interopTraceOverflow = true;
          return;
        }
        window.interopWire.push({ kind, at: performance.now(), ...details });
      };
      if (trace) window.interopRecord = stamp;
      const watch = (dc, pc, role) => {
        const details = { index: window.interopDataChannels.length, pc, role, label: dc.label };
        window.interopDataChannels.push({ ...details, dc });
        const info = () => ({ ...details, id: dc.id, state: dc.readyState });
        stamp('channel', info());
        for (const event of ['open', 'close', 'closing', 'error']) {
          dc.addEventListener(event, () => stamp(event, info()));
        }
        dc.addEventListener('message', async ({ data }) => {
          stamp('message', { ...info(), bytes: encoded(data instanceof Blob ? await data.arrayBuffer() : data) });
        });
      };
      const PC = window.RTCPeerConnection;
      window.RTCPeerConnection = class extends PC {
        constructor(...args) {
          super(...args);
          const index = window.interopPeerConnections.length;
          window.interopPeerConnections.push(this);
          if (trace) this.addEventListener('datachannel', ({ channel }) => watch(channel, index, 'remote'));
        }
        createDataChannel(...args) {
          const channel = super.createDataChannel(...args);
          if (trace) watch(channel, window.interopPeerConnections.indexOf(this), 'local');
          return channel;
        }
      };
      if (trace) {
        const nativeSend = RTCDataChannel.prototype.send;
        RTCDataChannel.prototype.send = function (data) {
          const result = nativeSend.call(this, data);
          const row = window.interopDataChannels.find(entry => entry.dc === this);
          if (row) stamp('send', { index: row.index, id: this.id, state: this.readyState, bytes: encoded(data) });
          return result;
        };
      }
      const WS = window.WebSocket;
      window.WebSocket = class extends WS {
        constructor(...args) {
          super(...args);
          const index = window.interopSignalSockets.length;
          window.interopSignalSockets.push(this);
          if (trace) this.addEventListener('message', async ({ data }) => {
            stamp('signal', { index, bytes: encoded(data instanceof Blob ? await data.arrayBuffer() : data) });
          });
        }
      };
    }, !!opts['reliable-trace']);
    await page.goto('http://127.0.0.1:' + host.address().port);
    const token = new AccessToken('devkey', 'secret', { identity: 'peer-' + i });
    token.addGrant({ roomJoin: true, room: roomName, canPublish: true, canSubscribe: true, canPublishData: true });
    const connected = await page.evaluate(args => interop.connect(args), {
      url: survivorUrl, token: await token.toJwt(), single: !!opts.single, frequency: i ? 880 : 440,
      adaptiveStream: false,
    });
    assert.ok(connected.sid);
    await page.evaluate(() => interop.video({}));
  }
}
async function trace(label) {
  if (!opts['reliable-trace']) return;
  const rows = await Promise.all(pages.map(page => page.evaluate(() => {
    const room = window.interopTraceRoom;
    const engine = room.engine;
    const manager = engine.dataChannels;
    const reliable = manager?.reliable;
    const channels = window.interopDataChannels.map(({ dc, ...details }) => ({
      ...details, id: dc.id, state: dc.readyState, onmessage: typeof dc.onmessage === 'function',
      sdkReliablePublisher: dc === reliable?.channelHandle, sdkReliableSubscriber: dc === manager?.reliableSub,
    }));
    return { participantSid: room.localParticipant.sid, roomState: room.state,
      attemptingReconnect: engine.attemptingReconnect, publisherReadyMemoized: !!engine.publisherConnectionPromise,
      unsentReliable: reliable?.messageBuffer?.getUnsent?.().length,
      transportMode: engine.pcManager?.mode,
      channels, wire: window.interopWire, overflow: window.interopTraceOverflow ?? false };
  })));
  const decode = entry => {
    const { bytes, ...event } = entry;
    if (!bytes?.length) return event;
    try {
      if (entry.kind === 'signal') {
        const response = SignalResponse.fromBinary(Uint8Array.from(bytes)).message;
        if (response.case === 'join') return { ...event, signal: response.case, participantSid: response.value.participant?.sid };
        if (response.case === 'reconnect') return { ...event, signal: response.case, lastMessageSeq: response.value.lastMessageSeq };
        if (response.case === 'leave') return { ...event, signal: response.case, action: response.value.action, reason: response.value.reason };
        return { ...event, signal: response.case };
      }
      const packet = DataPacket.fromBinary(Uint8Array.from(bytes));
      const user = packet.value.case === 'user' ? packet.value.value : undefined;
      let payload;
      if (user?.payload?.length) {
        try { payload = JSON.parse(new TextDecoder().decode(user.payload)); } catch {}
      }
      return { ...event, packet: packet.value.case, seq: packet.sequence, participantSid: packet.participantSid,
        topic: user?.topic, fixtureSeq: Number.isInteger(payload?.seq) ? payload.seq : undefined };
    } catch { return { ...event, decodeError: true }; }
  };
  for (const row of rows) {
    assert.equal(row.overflow, false, 'trace event capacity exceeded');
    row.wire = row.wire.map(decode);
  }
  (receipt.reliableTrace ??= {})[label] = rows;
}
async function diagnosticData(label, reliable, useServer) {
  if (!opts['reliable-trace']) return;
  const topic = label + '-' + Date.now();
  if (useServer) {
    await api.sendData(roomName, new TextEncoder().encode(JSON.stringify({ seq: 0, topic })), 0, { topic });
  } else {
    for (const page of pages) {
      await page.evaluate(({ topic, reliable }) => interop.data({ count: 5, topic, reliable }), { topic, reliable });
    }
  }
  const received = () => Promise.all(pages.map(page => page.evaluate(topic =>
    interop.report().received.filter(row => row.topic === topic).map(row => row.seq), topic)));
  let rows;
  try {
    await until(async () => {
      rows = await received();
      return rows.every(row => row.join(',') === (useServer ? '0' : '0,1,2,3,4'));
    }, label, 3000);
  } catch { /* retain failed data deliveries independently from the original gate */ }
  (receipt.reliableDiagnostics ??= {})[label] = { topic, received: rows };
  await trace(label);
}
async function reports() {
  return Promise.all(pages.map(page => page.evaluate(async () => {
    const report = interop.report();
    const ports = [];
    for (const pc of window.interopPeerConnections) {
      if (pc.connectionState !== 'connected') continue;
      const stats = await pc.getStats();
      for (const row of stats.values()) {
        const pair = row.type === 'transport' && stats.get(row.selectedCandidatePairId);
        const remote = pair && stats.get(pair.remoteCandidateId);
        if (remote) ports.push(remote.port);
      }
    }
    return { videoFrames: report.videoFrames, audibleSamples: report.audibleSamples,
      tones: report.toneMeasurements.filter(t => t.rms > .01).slice(-20),
      events: report.events, connection: report.connection, remotePorts: ports,
      signalSockets: window.interopSignalSockets.length, errors: report.errors };
  })));
}
async function media(label, expectedPorts, previous = [], ms = 15000) {
  let latest;
  try {
    await until(async () => {
      latest = await reports();
      return latest.every((row, i) => row.connection === 'connected' && row.remotePorts.length > 0 &&
        row.remotePorts.every(port => expectedPorts.includes(port)) &&
        row.videoFrames - (previous[i]?.videoFrames ?? 0) >= 8 &&
        row.audibleSamples - (previous[i]?.audibleSamples ?? 0) >= 5 &&
        row.tones.some(t => t.sampleIndex > (previous[i]?.tones.at(-1)?.sampleIndex ?? 0) &&
          t.identity === 'peer-' + (1-i) && Math.abs(t.frequencyHz - (i ? 440 : 880)) < 30));
    }, label, ms);
    const topic = label + '-' + Date.now();
    await trace(label + 'BeforeData');
    for (const page of pages) await page.evaluate(topic => interop.data({ count: 5, topic }), topic);
    await trace(label + 'AfterData');
    await until(async () => {
      const received = await Promise.all(pages.map((page, i) => page.evaluate(({ topic, i }) =>
        interop.report().received.filter(row => row.topic === topic && row.from === 'peer-' + (1-i)).map(row => row.seq),
        { topic, i })));
      receipt[label + 'ReliableData'] = received;
      return received.every(rows => rows.join(',') === '0,1,2,3,4');
    }, label + ' reliable data');
    return latest;
  } finally {
    receipt[label] = latest;
    await trace(label + 'Settled');
  }
}
async function closePeers() {
  const cleanup = [];
  for (const page of pages) {
    const row = await page.evaluate(async () => {
      await interop.close();
      const report = interop.report();
      return { pc: window.interopPeerConnections.map(pc => pc.connectionState),
        context: report.audioContext, timers: report.activeSamplingTimers };
    });
    cleanup.push(row);
    await page.context().close();
    assert.ok(row.pc.length && row.pc.every(state => state === 'closed'));
    assert.equal(row.context, 'closed');
    assert.equal(row.timers, 0);
  }
  pages = [];
  return cleanup;
}
try {
  const broker = await start('redis', opts.redis, ['--bind', '127.0.0.1', '--port', String(redisPort), '--save', '', '--appendonly', 'no', '--dir', out]);
  await until(async () => broker.running() && await redis('PING').catch(() => null) === 'PONG', 'Redis readiness');
  const owner = await startServer('owner', ownerHttp, ownerTcp, ownerUdp);
  const survivor = await startServer('survivor', survivorHttp, survivorTcp, survivorUdp);
  host = createServer((req, res) => {
    res.writeHead(200, { 'content-type': req.url === '/sdk.js' ? 'text/javascript' : 'text/html' });
    res.end(req.url === '/sdk.js' ? sdk : servedHtml);
  });
  await new Promise(r => host.listen(0, '127.0.0.1', r));
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  receipt.browser = browser.version();
  const original = await api.createRoom({ name: roomName, nodeId: owner.record.nodeId, emptyTimeout: 120, departureTimeout: 5 });
  receipt.originalSid = original.sid;
  await joinPeers();
  assert.equal(await redis('HGET', 'room_node_map', roomName), owner.record.nodeId);
  const baseline = await media('beforeFault', [ownerTcp, ownerUdp]);
  const started = Date.now();
  owner.child.kill(opts.mode === 'drain' ? 'SIGTERM' : 'SIGKILL');
  if (opts.mode === 'drain') {
    await until(async () => {
      assert.ok(owner.running(), 'owner exited with participants still connected');
      const hex = await redis('EVAL', "local s=redis.call('HGET',KEYS[1],ARGV[1]); if not s then return '' end; return (string.gsub(s,'.', function(c) return string.format('%02x',string.byte(c)) end))", '1', 'nodes', owner.record.nodeId);
      return nodeState(hex) === 2;
    }, 'owner SHUTTING_DOWN registry state');
    receipt.drainOwnerRetained = await redis('HGET', 'room_node_map', roomName);
    assert.equal(receipt.drainOwnerRetained, owner.record.nodeId);
    await Promise.all(pages.map(page => page.evaluate(() => interop.reconnect())));
    await until(async () => (await reports()).every((row,i) => row.signalSockets > baseline[i].signalSockets &&
      row.events.slice(baseline[i].events.length).some(event => event[0] === 'state' &&
        ['signalReconnecting', 'reconnecting'].includes(event[1]))),
      'official SDK reconnect during owner drain');
    await media('duringDrain', [ownerTcp, ownerUdp], baseline);
    receipt.drainClientCleanup = await closePeers();
    await until(() => !owner.running(), 'graceful owner exit after SDK disconnect', 25000);
    assert.equal(owner.record.code, 0);
    assert.equal(await redis('HEXISTS', 'nodes', owner.record.nodeId), 0);
    assert.equal(await redis('HGET', 'room_node_map', roomName), null);
    receipt.afterDrainCreateSid = (await api.createRoom({ name: roomName, nodeId: survivor.record.nodeId })).sid;
    await joinPeers();
    await media('afterDrainRejoin', [survivorTcp, survivorUdp]);
  } else {
    await owner.done;
    assert.equal(owner.record.signal, 'SIGKILL');
    try {
      await media('automaticRecovery', [survivorTcp, survivorUdp], baseline, 40000);
      assert.ok(receipt.automaticRecovery.every((row, i) => row.signalSockets > baseline[i].signalSockets));
      receipt.automaticReconnect = true;
    } catch (error) {
      receipt.automaticReconnect = false;
      receipt.reconnectError = error.message;
      if (receipt.reconnectError === 'deadline: automaticRecovery reliable data') {
        await diagnosticData('oldSDKLateReliable', true, false);
        await diagnosticData('oldSDKLateLossy', false, false);
        await diagnosticData('oldSDKServerReliable', true, true);
      }
      receipt.lostClientCleanup = await closePeers();
      await joinPeers();
      await media('freshSDKRecovery', [survivorTcp, survivorUdp]);
    }
  }
  receipt.recoveryElapsedMs = Date.now() - started;
  assert.equal(await redis('HGET', 'room_node_map', roomName), survivor.record.nodeId);
  receipt.afterFaultOwner = survivor.record.nodeId;
  receipt.afterFaultRoomSid = (await api.listRooms([roomName]))[0]?.sid;
  assert.ok(receipt.afterFaultRoomSid);
  const active = await api.listParticipants(roomName);
  receipt.afterFaultParticipants = active.map(p => p.identity).sort();
  assert.deepEqual(receipt.afterFaultParticipants, ['peer-0', 'peer-1']);
  receipt.clientCleanup = await closePeers();
  await until(async () => (await api.listParticipants(roomName)).length === 0, 'participant cleanup');
  await api.deleteRoom(roomName);
  receipt.redisReadback = {
    mapping: await redis('HEXISTS', 'room_node_map', roomName),
    room: await redis('HEXISTS', 'rooms', roomName),
    participants: await redis('EXISTS', 'room_participants:' + roomName),
    staleOwner: await redis('HEXISTS', 'nodes', owner.record.nodeId),
  };
  assert.equal(receipt.redisReadback.mapping + receipt.redisReadback.room + receipt.redisReadback.participants, 0);
  receipt.status = receipt.automaticReconnect === false ? 'FAIL_AUTOMATIC_RECONNECT' : 'PASS';
} catch (error) {
  receipt.error = String(error.stack ?? error);
  receipt.lastReports = await reports().catch(() => []);
} finally {
  await browser?.close();
  if (host) await new Promise(r => host.close(r));
  for (const p of childProcesses.reverse()) {
    if (p.running()) p.child.kill('SIGTERM');
    try { await until(() => !p.running(), 'fixture process exit', 15000); }
    catch { p.record.forcedCleanup = true; p.child.kill('SIGKILL'); }
    await p.done;
    if (p.record.forcedCleanup || p.record.error || (p.record.code !== 0 &&
        !(opts.mode === 'kill' && p.record === receipt.processes.owner && p.record.signal === 'SIGKILL'))) {
      receipt.status = 'FAIL';
    }
  }
  const tcpPorts = [redisPort, ownerHttp, ownerTcp, survivorHttp, survivorTcp];
  receipt.portsReleased = (await Promise.all([
    ...tcpPorts.map(port => portReleased(port, false)),
    ...[ownerUdp, survivorUdp].map(port => portReleased(port, true)),
  ])).every(Boolean);
  if (!receipt.portsReleased) receipt.status = 'FAIL';
  await writeFile(join(out, 'execution.json'), JSON.stringify(receipt, null, 2) + '\n');
  console.log(JSON.stringify({ status: receipt.status, error: receipt.error, reconnectError: receipt.reconnectError }));
  process.exitCode = receipt.status === 'PASS' ? 0 : 1;
}
