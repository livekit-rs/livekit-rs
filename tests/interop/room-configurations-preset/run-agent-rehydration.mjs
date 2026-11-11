#!/usr/bin/env node
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, openSync, readFileSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer, createConnection } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { isDeepStrictEqual } from 'node:util';
import { AccessToken, AgentDispatchClient, RoomServiceClient } from 'livekit-server-sdk';
import { Room } from '@livekit/protocol';

// Use the internal protobuf descriptor shipped by the locked official package.
const { RoomInternal } = await import(new URL('../src/gen/livekit_internal_pb.js', import.meta.resolve('@livekit/protocol')));
assert.equal(process.argv.length, 5, 'usage: run-agent-rehydration.mjs GO_SERVER RUST_SERVER NEW_OUTPUT_DIR');
const [go, rust, output] = process.argv.slice(2).map(path => resolve(path));
await mkdir(output);
const cases = [
  { name: 'no-preset', request: {}, expected: ['seeded'] },
  { name: 'omitted-agents', request: { roomPreset: 'omitted' }, expected: [] },
  { name: 'empty-agents', request: { roomPreset: 'empty' }, expected: [] },
  { name: 'configured-agents', request: { roomPreset: 'configured' }, expected: ['preset-agent'] },
  { name: 'explicit-wins', request: { roomPreset: 'configured', agents: [{ agentName: 'explicit-agent' }] }, expected: ['explicit-agent'] },
];

function hash(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}
function launch(program, args, logPath) {
  const fd = openSync(logPath, 'w');
  const process = spawn(program, args, { stdio: ['ignore', fd, fd] });
  closeSync(fd);
  const done = new Promise(resolve => {
    process.once('error', error => resolve({ error: error.message }));
    process.once('exit', (code, signal) => resolve({ code, signal }));
  });
  return { process, done };
}
async function stop({ process, done }, signal) {
  if (process.exitCode === null && process.signalCode === null) process.kill(signal);
  const exit = await Promise.race([done, sleep(12_000, null, { ref: false })]);
  if (exit) return exit;
  process.kill('SIGKILL');
  return { ...await done, forced: true };
}
async function reserve() {
  const socket = createServer();
  await new Promise((accept, reject) => {
    socket.once('error', reject);
    socket.listen(0, '127.0.0.1', accept);
  });
  return socket;
}
const close = socket => new Promise((accept, reject) => socket.close(error => error ? reject(error) : accept()));
async function wait(probe, process) {
  for (let i = 0; i < 120; i++) {
    assert.equal(process.exitCode, null, 'process exited before readiness');
    if (await probe()) return;
    await sleep(50);
  }
  throw new Error('readiness deadline');
}
function redis(port, args, input, check = true) {
  const result = spawnSync('redis-cli', ['--raw', '-h', '127.0.0.1', '-p', port, ...args], { input, timeout: 2500 });
  if (check) {
    assert.equal(result.status, 0, result.stderr.toString());
    assert.equal(result.stdout.toString().startsWith('ERR '), false, result.stdout.toString());
  }
  return result.stdout ?? Buffer.alloc(0);
}
async function run(binary, label) {
  const dir = await mkdtemp(join(tmpdir(), 'lk-c09-owned-'));
  const reserved = [await reserve(), await reserve()];
  const [redisPort, httpPort] = reserved.map(socket => socket.address().port);
  const cfg = join(dir, 'config.json');
  await writeFile(cfg, JSON.stringify({
    port: httpPort, bind_addresses: ['127.0.0.1'], keys: { devkey: 'owned-c09-fixture-secret' },
    redis: { address: `127.0.0.1:${redisPort}` },
    rtc: { node_ip: '127.0.0.1', use_external_ip: false, tcp_port: 0, port_range_start: 30000, port_range_end: 30005 },
    turn: { enabled: false },
    room: { room_configurations: { omitted: { metadata: 'uses-omitted' }, empty: { agents: [] }, configured: { agents: [{ agent_name: 'preset-agent' }] } } }
  }));
  const report = { binary_sha256: hash(binary), config_sha256: hash(cfg), cases: [] };
  let broker, server;
  try {
    await close(reserved[0]);
    broker = launch('redis-server', ['--bind', '127.0.0.1', '--port', redisPort, '--save', '', '--appendonly', 'no', '--dir', dir], join(output, `${label}-redis.log`));
    await wait(() => redis(redisPort, ['PING'], undefined, false).toString().trim() === 'PONG', broker.process);
    await close(reserved[1]);
    server = launch(binary, ['--config', cfg], join(output, `${label}-server.log`));
    const url = `http://127.0.0.1:${httpPort}`;
    await wait(async () => {
      try { return (await fetch(url, { signal: AbortSignal.timeout(500) })).ok; } catch { return false; }
    }, server.process);
    await sleep(200);
    const access = new AccessToken('devkey', 'owned-c09-fixture-secret', { identity: 'c09-fixture' });
    access.addGrant({ roomCreate: true, roomList: true });
    const token = await access.toJwt();
    const client = new RoomServiceClient(url, 'devkey', 'owned-c09-fixture-secret');
    const dispatcher = new AgentDispatchClient(url, 'devkey', 'owned-c09-fixture-secret');
    for (const entry of cases) {
      const sid = `RM_${entry.name}`;
      redis(redisPort, ['-x', 'HSET', 'rooms', entry.name], new Room({
        sid, name: entry.name, emptyTimeout: 60, departureTimeout: 20,
        creationTime: BigInt(Math.floor(Date.now() / 1000)), creationTimeMs: BigInt(Date.now()),
      }).toBinary());
      redis(redisPort, ['-x', 'HSET', 'room_internal', entry.name], new RoomInternal({
        agentDispatches: [{ agentName: 'seeded', metadata: 'seeded-policy' }],
      }).toBinary());
      const response = await fetch(`${url}/twirp/livekit.RoomService/CreateRoom`, {
        method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({ name: entry.name, ...entry.request }), signal: AbortSignal.timeout(8000),
      });
      const result = await response.json();
      const internal = redis(redisPort, ['HGET', 'room_internal', entry.name]);
      const agents = RoomInternal.fromBinary(internal.subarray(0, -1)).agentDispatches.map(value => value.agentName);
      const dispatched = (await dispatcher.listDispatch(entry.name)).map(value => value.agentName);
      const expectedDispatches = entry.expected.length ? entry.expected : [''];
      await client.deleteRoom(entry.name);
      const removed = ['rooms', 'room_internal', 'room_node_map'].every(key =>
        redis(redisPort, ['HEXISTS', key, entry.name]).toString().trim() === '0');
      report.cases.push({ name: entry.name, expected: entry.expected, status: response.status,
        same_sid: result.sid === sid, agents, dispatched, removed,
        accepted: response.ok && result.sid === sid && isDeepStrictEqual(agents, entry.expected)
          && isDeepStrictEqual(dispatched, expectedDispatches) && removed });
    }
  } catch (error) {
    report.error = error.message;
  } finally {
    for (const socket of reserved) if (socket.listening) await close(socket);
    if (server) report.server_exit = await stop(server, 'SIGINT');
    if (broker) report.redis_exit = await stop(broker, 'SIGTERM');
    const bindable = port => new Promise(resolve => {
      const client = createConnection({ port, host: '127.0.0.1' });
      client.once('connect', () => { client.destroy(); resolve(false); });
      client.once('error', error => resolve(error.code === 'ECONNREFUSED'));
    });
    report.listeners_released = (await Promise.all([bindable(redisPort), bindable(httpPort)])).every(Boolean);
    await rm(dir, { recursive: true, force: true });
  }
  report.pass = report.cases.length === cases.length && report.cases.every(value => value.accepted)
    && report.server_exit?.code === 0 && report.redis_exit?.code === 0 && report.listeners_released;
  return report;
}
const results = { go: await run(go, 'go'), rust: await run(rust, 'rust') };
await writeFile(join(output, 'execution.json'), JSON.stringify(results, null, 2) + '\n');
process.exitCode = results.go.pass && results.rust.pass ? 0 : 1;
