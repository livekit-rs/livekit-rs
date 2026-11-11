// Run with the pinned Go and candidate binary paths, in that order.
// Add --cluster to exercise the independent commands on different masters.
// Both daemons use fresh owned Redis. All Twirp calls use the official SDK.
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createServer } from 'node:net';
import { request } from 'node:http';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { AccessToken, RoomServiceClient } from 'livekit-server-sdk';
import { JoinRequest, WrappedJoinRequest } from '@livekit/protocol';

const key = 'publicfixturekey';
const secret = 'public-room-catalog-fixture-0123456789';
const sha = path => createHash('sha256').update(readFileSync(path)).digest('hex');
const [go, rust, mode] = process.argv.slice(2);
const cluster = mode === '--cluster';
assert.ok(cluster || !mode);
assert.equal(sha(go), '4bc2378e636740b29093061036249ffc9ebbef91f4ece1de12177c8dfee1e7dc');

async function ports(length) {
  const held = await Promise.all(Array.from({ length }, () => new Promise(resolve => {
    const s = createServer().listen(0, '127.0.0.1', () => resolve(s));
  })));
  const numbers = held.map(s => s.address().port);
  await Promise.all(held.map(s => new Promise(resolve => s.close(resolve))));
  return numbers;
}

async function observe(binary) {
  const directory = mkdtempSync(join(tmpdir(), 'lk-catalog-'));
  const [redis, http, tcp, udp, ...extra] = await ports(cluster ? 12 : 7);
  const [otherHttp, otherTcp, otherUdp] = extra.slice(-3);
  const brokers = cluster ? [redis, ...extra.slice(0, 2)] : [redis];
  const addresses = brokers.map(port => `127.0.0.1:${port}`);
  const url = `http://127.0.0.1:${http}`;
  const server = new RoomServiceClient(url, key, secret);
  const processes = [];
  const processLogs = new Map();
  const stop = [];
  const cliOn = (port, ...args) => execFileSync('redis-cli', ['--raw', '-c', '-p', port, ...args], {
    encoding: 'utf8', timeout: 2500, stdio: ['ignore', 'pipe', 'ignore'],
  }).trim();
  const cli = (...args) => cliOn(redis, ...args);
  function start(command, args) {
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    for (const stream of [child.stdout, child.stderr]) {
      stream.on('data', chunk => {
        processLogs.set(child, ((processLogs.get(child) ?? '') + chunk).slice(-4096));
      });
    }
    processes.push(child);
    return child;
  }
  async function waitFor(child, check) {
    for (let i = 0; i < 300; i++) {
      assert.equal(child.exitCode, null, `owned process exited before readiness: ${processLogs.get(child) ?? ''}`);
      try { if (await check()) return; } catch { /* listener not up yet */ }
      await sleep(100);
    }
    throw Error('owned process did not become ready');
  }
  const joinToken = async (room, extras) => {
    const token = new AccessToken(key, secret, extras.token ?? { identity: 'catalog-join' });
    token.addGrant({
      room, roomJoin: true, roomCreate: extras.roomCreate,
      canUpdateOwnMetadata: extras.canUpdateOwnMetadata,
    });
    return token.toJwt();
  };
  function joinEndpoint(single, validate, extras) {
    const path = single ? '/rtc/v1' : '/rtc';
    const endpoint = new URL(validate ? `${path}/validate` : path, extras.baseUrl ?? url);
    endpoint.searchParams.set('protocol', '17');
    endpoint.searchParams.set('sdk', 'js');
    const { metadata, attributes, encodedAttributes } = extras;
    const paddedUrlBase64 = bytes => Buffer.from(bytes).toString('base64')
      .replaceAll('+', '-').replaceAll('/', '_');
    if (single) {
      const joinRequest = new JoinRequest({
        clientInfo: { protocol: 17 }, metadata, participantAttributes: attributes,
      }).toBinary();
      const wrapped = new WrappedJoinRequest({ joinRequest }).toBinary();
      endpoint.searchParams.set('join_request', paddedUrlBase64(wrapped));
    } else if (encodedAttributes !== undefined) {
      endpoint.searchParams.set('attributes', encodedAttributes);
    } else if (attributes !== undefined) {
      endpoint.searchParams.set('attributes', paddedUrlBase64(JSON.stringify(attributes)));
    }
    return endpoint;
  }
  async function validate(room, single = false, extras = {}) {
    const endpoint = joinEndpoint(single, true, extras);
    const token = await joinToken(room, extras);
    const response = await fetch(endpoint, {
      headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(3000),
    });
    return { status: response.status, body: await response.text() };
  }
  async function connect(room, single, extras = {}) {
    const endpoint = joinEndpoint(single, false, extras);
    const token = await joinToken(room, extras);
    return new Promise((resolve, reject) => {
      const req = request(endpoint, {
        headers: {
          authorization: `Bearer ${token}`,
          connection: 'Upgrade',
          upgrade: 'websocket',
          'sec-websocket-key': 'dGhlIHNhbXBsZSBub25jZQ==',
          'sec-websocket-version': '13',
        },
        signal: AbortSignal.timeout(10000),
      }, res => {
        res.setEncoding('utf8');
        let body = '';
        res.on('data', chunk => { body += chunk; });
        res.on('error', reject);
        res.on('end', () => resolve({ status: res.statusCode, body }));
      });
      req.on('error', reject);
      req.on('upgrade', (res, socket) => {
        if (extras.token) resolve({ status: res.statusCode, socket });
        else {
          socket.destroy();
          reject(Error('unexpected upgrade'));
        }
      });
      req.end();
    });
  }
  const connectBoth = room => Promise.all([false, true].map(single => connect(room, single)));
  const overrideProfiles = [
    [false, { attributes: { fixture: 'legacy-attributes' } }],
    [true, { metadata: 'v1-metadata' }],
    [true, { attributes: { fixture: 'v1-attributes' } }],
  ];
  const deniedOverrides = room => Promise.all(overrideProfiles.flatMap(([single, extras]) => [
    validate(room, single, extras),
    connect(room, single, extras),
  ]));
  const permittedOverrides = room => Promise.all(overrideProfiles.map(([single, extras]) =>
    validate(room, single, { ...extras, canUpdateOwnMetadata: true })));
  async function joinWithOverrides(room, single) {
    const identity = single ? 'catalog-v1-overrides' : 'catalog-v0-overrides';
    const { status, socket } = await connect(room, single, {
      token: {
        identity,
        metadata: 'original-metadata',
        attributes: { keep: 'original', change: 'original' },
      },
      canUpdateOwnMetadata: true,
      metadata: single ? 'override-metadata' : undefined,
      attributes: { keep: '', change: 'override' },
    });
    assert.equal(status, 101);
    assert.ok(socket);
    try {
      const participant = await server.getParticipant(room, identity);
      return { status, metadata: participant.metadata, attributes: participant.attributes };
    } finally {
      try {
        await server.removeParticipant(room, identity);
      } finally {
        socket.destroy();
      }
    }
  }
  const observation = { binarySha256: sha(binary), stop };
  try {
    for (const [index, port] of brokers.entries()) {
      const clusterArgs = cluster
        ? ['--cluster-enabled', 'yes', '--cluster-port', extra[index + 2],
          '--cluster-config-file', `nodes-${index}.conf`]
        : [];
      const broker = start('redis-server', ['--bind', '127.0.0.1', '--port', port,
        '--save', '', '--appendonly', 'no', '--dir', directory, ...clusterArgs]);
      await waitFor(broker, () => cliOn(port, 'PING') === 'PONG');
    }
    if (cluster) {
      execFileSync('redis-cli', ['--cluster', 'create', ...addresses, '--cluster-yes'],
        { timeout: 15000, stdio: 'pipe' });
      for (const [index, port] of brokers.entries()) {
        await waitFor(processes[index], () => {
          const info = cliOn(port, 'CLUSTER', 'INFO');
          return info.includes('cluster_state:ok') && info.includes('cluster_slots_ok:16384');
        });
      }
      const ranges = JSON.parse(execFileSync('redis-cli',
        ['--json', '-p', redis, 'CLUSTER', 'SLOTS'], { encoding: 'utf8', timeout: 2500 }));
      const slots = ['rooms', 'room_internal'].map(key => Number(cli('CLUSTER', 'KEYSLOT', key)));
      const owners = slots.map(slot => ranges.find(([first, last]) => slot >= first && slot <= last)[2][1]);
      assert.notEqual(owners[0], owners[1], 'keys must reside on separate masters');
      observation.slots = slots;
    }
    const config = join(directory, 'config.yaml');
    const redisConfig = cluster ? `cluster_addresses: [${addresses.map(a => "'"+a+"'").join(', ')}]`
      : `address: '${addresses[0]}'`;
    const configText = (http, tcp, udp, metadataLimit, attributesLimit) => `port: ${http}
bind_addresses: [127.0.0.1]
keys: {${key}: ${secret}}
rtc: {node_ip: 127.0.0.1, tcp_port: ${tcp}, udp_port: ${udp}, enable_loopback_candidate: true}
redis: {${redisConfig}}
room: {auto_create: false}
limit: {num_tracks: 1, max_metadata_size: ${metadataLimit}, max_attributes_size: ${attributesLimit}}
logging: {level: error}
`;
    writeFileSync(config, configText(http, tcp, udp, 32, 64));
    const daemon = start(binary, ['--dev', '--config', config]);
    await waitFor(daemon, async () => (await fetch(url, { signal: AbortSignal.timeout(500) })).ok);
    const name = 'catalog-public', absent = 'catalog-absent';
    const room = await server.createRoom({ name, emptyTimeout: 60 });
    assert.equal((await server.listRooms([name]))[0]?.sid, room.sid);
    observation.valid = await validate(name);
    observation.healthyDeniedOverrides = await deniedOverrides(name);
    observation.healthyPermittedOverrides = await permittedOverrides(name);
    observation.participantOverrides = [
      await joinWithOverrides(name, false),
      await joinWithOverrides(name, true),
    ];
    const tooManyAttributes = { attributes: { fixture: 'X'.repeat(128) } };
    const tooMuchMetadata = { metadata: 'X'.repeat(128) };
    observation.exceededLimits = [];
    for (const [single, override] of [
      [false, tooManyAttributes], [true, tooManyAttributes], [true, tooMuchMetadata],
    ]) {
      const request = { ...override, canUpdateOwnMetadata: true };
      observation.exceededLimits.push(
        await validate(name, single, request), await connect(name, single, request),
      );
    }
    const uncreated = 'catalog-profile-limit-no-allocation';
    observation.ownerLimitPreventsAllocation = {
      response: await connect(uncreated, true, {
        ...tooMuchMetadata, canUpdateOwnMetadata: true, roomCreate: true,
      }),
      publicStored: Number(cli('HEXISTS', 'rooms', uncreated)),
      privateStored: Number(cli('HEXISTS', 'room_internal', uncreated)),
    };

    const strictNode = cli('HGET', 'room_node_map', name);
    const otherUrl = `http://127.0.0.1:${otherHttp}`;
    const otherConfig = join(directory, 'permissive-config.yaml');
    writeFileSync(otherConfig, configText(otherHttp, otherTcp, otherUdp, 512, 512));
    const other = start(binary, ['--dev', '--config', otherConfig]);
    await waitFor(other, async () => (await fetch(otherUrl, { signal: AbortSignal.timeout(500) })).ok);
    const otherNodes = cli('HKEYS', 'nodes').split('\n').filter(node => node !== strictNode);
    assert.equal(otherNodes.length, 1, 'both physical nodes registered');
    const [permissiveNode] = otherNodes;
    const permissiveRoom = 'catalog-permissive-owner';
    await server.createRoom({ name: permissiveRoom, nodeId: permissiveNode, emptyTimeout: 60 });
    assert.equal(cli('HGET', 'room_node_map', permissiveRoom), permissiveNode);
    observation.permissiveEntryStrictOwner = await connect(name, true, {
      ...tooMuchMetadata, baseUrl: otherUrl, canUpdateOwnMetadata: true,
    });
    const identity = 'catalog-remote-profile';
    const { socket, ...remote } = await connect(permissiveRoom, true, {
      ...tooMuchMetadata, canUpdateOwnMetadata: true, token: { identity },
    });
    observation.strictEntryPermissiveOwner = remote;
    if (socket) {
      try {
        const participant = await server.getParticipant(permissiveRoom, identity);
        observation.strictEntryPermissiveOwner.metadata = participant.metadata;
      } finally {
        try {
          await server.removeParticipant(permissiveRoom, identity);
        } finally {
          socket.destroy();
        }
      }
    }
    assert.equal(cli('HGET', 'room_node_map', name), strictNode);
    observation.missingDeniedOverrides = await deniedOverrides(absent);
    const malformed = { encodedAttributes: '!' };
    observation.malformedBeforeAdmission = [
      await validate(absent, false, malformed),
      await connect(absent, false, malformed),
    ];
    assert.equal(cli('HSET', 'room_internal', name, 'invalid'), '0');
    assert.equal(cli('HSET', 'room_internal', absent, 'invalid'), '1');
    observation.malformedPrivateExisting = await validate(name);
    observation.malformedPrivateAbsent = await validate(absent);
    const owner = cli('HGET', 'room_node_map', name);
    const full = 'catalog-full-node';
    // Node.stats (4), NodeStats.updated_at (2, future), num_tracks_in (5, one).
    // Keep this fixture alive across the startup/periodic stale-node sweep.
    const fullNode = Buffer.from([0x22, 0x0c, 0x10, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x7f, 0x28, 1]);
    execFileSync('redis-cli', ['-c', '-p', redis, '-x', 'HSET', 'nodes', full], {
      input: fullNode, timeout: 2500, stdio: ['pipe', 'ignore', 'ignore'],
    });
    try {
      assert.equal(cli('HSET', 'room_node_map', absent, full), '1');
      assert.equal(cli('HSET', 'room_node_map', name, full), '0');
      observation.fullOwnerAbsentConnect = await connectBoth(absent);
      observation.fullOwnerExistingConnect = await connectBoth(name);
      observation.fullOwnerDeniedOverrides = await deniedOverrides(name);
    } finally {
      assert.equal(cli('HSET', 'room_node_map', name, owner), '0');
      cli('HDEL', 'room_node_map', absent);
      cli('HDEL', 'nodes', full);
    }
    const blocked = 'catalog-acl-blocked';
    for (const port of brokers) {
      assert.equal(cliOn(port, 'ACL', 'SETUSER', 'default', '-hset',
        '(+hset %W~nodes %W~room_internal %W~room_node_map)'), 'OK');
    }
    try {
      await assert.rejects(server.createRoom({ name: blocked, syncStreams: true }), error => {
        observation.failedCreate = { code: error.code, status: error.status, message: error.message };
        return true;
      });
      observation.publicAfterFailedCreate = Number(cli('HEXISTS', 'rooms', blocked));
      observation.privateAfterFailedCreate = Number(cli('HEXISTS', 'room_internal', blocked));
    } finally {
      for (const port of brokers) {
        assert.equal(cliOn(port, 'ACL', 'SETUSER', 'default', '+hset', 'clearselectors'), 'OK');
      }
    }
    cli('DEL', 'rooms');
    assert.equal(cli('SET', 'rooms', 'wrong-type'), 'OK');
    observation.wrongTypeValidate = await validate(name);
    observation.wrongTypeConnect = await connectBoth(name);
    observation.wrongTypeDeniedOverrides = await deniedOverrides(name);
    const originalFetch = globalThis.fetch;
    for (const [label, names] of [['listAll', undefined], ['listNamed', [name]]]) {
      globalThis.fetch = async (...args) => {
        const response = await originalFetch(...args);
        observation[label] = { status: response.status, body: await response.clone().json() };
        return response;
      };
      try {
        await assert.rejects(server.listRooms(names));
      } finally {
        globalThis.fetch = originalFetch;
      }
    }
  } finally {
    for (const child of processes.reverse()) {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGTERM');
        const timeout = setTimeout(() => child.kill('SIGKILL'), 10000);
        await new Promise(resolve => child.once('exit', resolve));
        clearTimeout(timeout);
      }
      stop.push({ code: child.exitCode, signal: child.signalCode });
    }
    rmSync(directory, { force: true, recursive: true });
  }
  return observation;
}

const reference = await observe(go);
const candidate = await observe(rust ?? go);
console.log(JSON.stringify({ reference, candidate }, null, 2));
const surface = ({ binarySha256, ...observed }) => observed;
assert.deepEqual(surface(candidate), surface(reference));
assert.deepEqual(reference.stop, Array(cluster ? 5 : 3).fill({ code: 0, signal: null }));
assert.equal(reference.malformedPrivateExisting.status, 200);
assert.equal(reference.malformedPrivateAbsent.status, 404);
assert.equal(reference.wrongTypeValidate.status, 500);
assert.deepEqual(reference.wrongTypeConnect.map(({ status }) => status), [500, 500]);
assert.deepEqual(reference.fullOwnerAbsentConnect.map(({ status }) => status), [404, 404]);
assert.deepEqual(reference.fullOwnerExistingConnect.map(({ status }) => status), [503, 503]);
const statuses = key => reference[key].map(({ status }) => status);
assert.deepEqual(statuses('healthyDeniedOverrides'), Array(6).fill(401));
assert.deepEqual(statuses('healthyPermittedOverrides'), Array(3).fill(200));
assert.deepEqual(statuses('missingDeniedOverrides'), Array(6).fill(404));
assert.deepEqual(statuses('fullOwnerDeniedOverrides'), Array(6).fill(503));
assert.deepEqual(statuses('wrongTypeDeniedOverrides'), Array(6).fill(500));
assert.deepEqual(statuses('malformedBeforeAdmission'), [400, 404]);
assert.deepEqual(reference.participantOverrides, ['original-metadata', 'override-metadata'].map(metadata => ({
  status: 101, metadata, attributes: { keep: 'original', change: 'override' },
})));
const mediaClosed = { status: 500, body: 'connection closed by media' };
assert.deepEqual(reference.exceededLimits, Array.from({ length: 3 }, () => [
  { status: 200, body: 'success' }, mediaClosed,
]).flat());
assert.deepEqual(reference.ownerLimitPreventsAllocation, {
  response: mediaClosed, publicStored: 0, privateStored: 0,
});
assert.deepEqual(reference.permissiveEntryStrictOwner, mediaClosed);
assert.deepEqual(reference.strictEntryPermissiveOwner, { status: 101, metadata: 'X'.repeat(128) });
assert.equal(reference.listAll.body.code, 'unknown');
assert.equal(reference.listNamed.body.code, 'unknown');
assert.equal(reference.publicAfterFailedCreate, 0);
assert.equal(reference.privateAfterFailedCreate, 1);
