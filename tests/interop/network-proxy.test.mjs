import test from 'node:test';
import assert from 'node:assert/strict';
import dgram from 'node:dgram';
import { TokenBucket, TurnUdpProxy } from './network-proxy.mjs';

test('bottleneck applies wire-byte budget and cannot accumulate an unlimited burst', () => {
  const bucket = new TokenBucket(8000, 0, 1000);
  assert.equal(bucket.take(1000, 0), true);
  assert.equal(bucket.take(1, 0), false);
  assert.equal(bucket.take(500, 500), true);
  assert.equal(bucket.take(1001, 100000), false);
  assert.equal(bucket.take(1000, 100000), true);
});
test('rate changes settle old capacity before applying the new rate', () => {
  const bucket = new TokenBucket(8000, 0, 1000);
  bucket.take(1000, 0); bucket.setRate(800, 100);
  assert.equal(bucket.take(200, 1100), true);
  assert.equal(bucket.take(1, 1100), false);
});
test('UDP proxy preserves payloads and releases sockets at shutdown', async () => {
  const server = dgram.createSocket('udp4'), client = dgram.createSocket('udp4');
  await new Promise(resolve => server.bind(0, '127.0.0.1', resolve));
  server.on('message', (bytes, peer) => server.send(bytes, peer.port, peer.address));
  const proxy = await new TurnUdpProxy({ upstreamPort: server.address().port }).start();
  try {
    const response = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('proxy echo timed out')), 2000);
      client.once('message', bytes => { clearTimeout(timer); resolve(bytes); });
    });
    const packet = Buffer.from([0x40, 0x01, 0x00, 0x04, 0x12, 0x34, 0x56, 0x78]);
    client.send(packet, proxy.port, '127.0.0.1');
    assert.deepEqual(await response, packet);
    assert.equal(proxy.stats().forwardedPackets, 1);
    assert.equal(proxy.stats().perClient[0].forwardedPackets, 1);
    assert.equal(proxy.stats().perClient[0].expiredPackets, 0);
    assert.ok(proxy.stats().flushTiming.calls >= 1);
    assert.ok(Number.isFinite(proxy.stats().perClient[0].maxQueueAgeMs));
  } finally {
    await proxy.close(); client.close(); server.close();
  }
});

test('controlled scheduler exposes the exact throughput lost beyond the finite burst window', () => {
  const saturate = intervalMs => {
    const bucket = new TokenBucket(4000000, 0);
    // Remove the initial burst so this measures sustained service only.
    assert.equal(bucket.take(4500, 0), true);
    let forwarded = 0;
    for (let now = intervalMs; now <= 1000; now += intervalMs) {
      while (bucket.take(1000, now)) forwarded += 1000;
    }
    return { forwarded, discarded: bucket.discardedRefillBytes };
  };
  assert.deepEqual(saturate(5), { forwarded: 500000, discarded: 0 });
  assert.deepEqual(saturate(10), { forwarded: 400000, discarded: 99500 });
  assert.deepEqual(saturate(20), { forwarded: 200000, discarded: 299500 });
  // This is diagnostic evidence, not a justification for enlarging the burst
  // or accepting a weaker capacity/decoded-media test.
});
