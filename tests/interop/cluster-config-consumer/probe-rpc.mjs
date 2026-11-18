// C44 psrpc.compression config-consumer driver. No browser, no media: the
// official server-sdk RoomService client creates the room on the owner node and
// then sends a large, highly-compressible UpdateRoomMetadata through the FRONT
// node. roomservice.go:344-351 checks RoomExists in Redis (true for a created
// empty room) and routes roomClient.UpdateRoomMetadata to the owner over the
// PSRPC bus, so the request envelope crosses Redis and the runner's MONITOR tap
// observes its internal.Msg.compression flag. The returned Room.metadata proves
// the owner actually decompressed and applied the cross-node request.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {writeFile} from 'node:fs/promises';
import {randomBytes} from 'node:crypto';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(here, '../package.json'));
const {AccessToken, RoomServiceClient} = require('livekit-server-sdk');

const url = process.env.LIVEKIT_URL;
const key = process.env.LIVEKIT_API_KEY;
const secret = process.env.LIVEKIT_API_SECRET;
const ownerUrl = process.env.WHIP_OWNER_URL;
const ownerNode = process.env.WHIP_OWNER_NODE_ID;
const out = process.env.CONFIG_PROBE_OUTPUT;
const metadataSize = Number(process.env.METADATA_SIZE || 8192);
assert.ok(url && key && secret && ownerUrl && ownerNode && out);

const roomName = `room-cfg-${randomBytes(6).toString('hex')}`;
const frontApi = new RoomServiceClient(url.replace(/^ws/, 'http'), key, secret);
const ownerApi = new RoomServiceClient(ownerUrl, key, secret);
// Highly compressible: gzip shrinks this far below the input, so the
// smaller-than-input decline never masks the quality/threshold gating.
const metadata = 'A'.repeat(metadataSize);
const evidence = {status: 'RUNNING', roomName, metadataSize, steps: []};
let created = false;

function clean(value) {
  return String(value).replaceAll(secret, '<secret>')
    .replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<token>');
}

try {
  // Pin ownership to node b so the front (node a) is never the owner and the
  // UpdateRoomMetadata request must cross the Redis bus.
  await ownerApi.createRoom({name: roomName, nodeId: ownerNode, emptyTimeout: 30});
  created = true;
  evidence.steps.push('room-created-on-owner');

  let outcome = 'applied';
  let returnedMetadataLength = null;
  let rpcError = null;
  try {
    const room = await frontApi.updateRoomMetadata(roomName, metadata);
    returnedMetadataLength = (room.metadata || '').length;
    assert.equal(returnedMetadataLength, metadataSize,
      'owner did not apply the full cross-node metadata');
    evidence.steps.push('front-update-room-metadata-applied-by-owner');
  } catch (error) {
    outcome = 'rejected';
    rpcError = {name: error.name, status: error.status, code: error.code, message: clean(error.message)};
    evidence.steps.push('front-update-room-metadata-rejected');
  }
  evidence.outcome = outcome;
  evidence.returnedMetadataLength = returnedMetadataLength;
  evidence.rpcError = rpcError;

  // The runner correlates this room's topic channel in the MONITOR tap.
  evidence.status = 'PASS';
} catch (error) {
  evidence.status = 'FAIL';
  evidence.failure = {name: error.name, message: clean(error.message)};
  process.exitCode = 1;
} finally {
  if (created) {
    try { await ownerApi.deleteRoom(roomName); evidence.steps.push('room-deleted'); }
    catch (error) { evidence.cleanupError = clean(error.message); evidence.status = 'FAIL'; process.exitCode = 1; }
  }
  await writeFile(out, JSON.stringify(evidence, null, 2) + '\n');
}