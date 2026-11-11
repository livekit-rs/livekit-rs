import assert from 'node:assert/strict';
import {createRequire} from 'node:module';

const require = createRequire(import.meta.url);
const {RoomServiceClient} = require('livekit-server-sdk');
const [operation, front, room, nodeId] = process.argv.slice(2);
const {LIVEKIT_API_KEY: key, LIVEKIT_API_SECRET: secret} = process.env;
const client = new RoomServiceClient(front, key, secret);

if (operation === 'create') {
  const options = {name:room,nodeId,emptyTimeout:60,metadata:'concurrent-fault-create'};
  const originalFetch = globalThis.fetch;
  let pending = 0;
  let overlap = 0;
  globalThis.fetch = async (url, init) => {
    assert.match(new URL(url).pathname,/\/CreateRoom$/);
    overlap = Math.max(overlap,++pending);
    try {
      return await originalFetch(url, init);
    } finally {
      pending--;
    }
  };
  let created;
  try {
    created = await Promise.all([client.createRoom(options), client.createRoom(options)]);
  } finally {
    globalThis.fetch = originalFetch;
  }
  const listed = await client.listRooms([room]);
  console.log(JSON.stringify({
    create_http_overlap:overlap,
    created:created.map(({sid,name,metadata})=>({sid,name,metadata})),
    listed:listed.map(({sid,name,metadata})=>({sid,name,metadata})),
  }));
} else {
  assert.equal(operation,'delete');
  await client.deleteRoom(room);
  console.log(JSON.stringify({listed:(await client.listRooms([room])).map(({sid})=>sid)}));
}
