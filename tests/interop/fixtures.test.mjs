import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

// Validate recorded message associations; this is not server replay coverage.
for (const server of ['official', 'rust']) {
  test(`${server} golden capture retains join/publication CID relationships`, async () => {
    const fixture = JSON.parse(await readFile(new URL(`../fixtures/upstream/${server}-single-golden.json`, import.meta.url)));
    assert.equal(fixture.status, 'PASS');
    for (const client of [0, 1]) {
      const messages = fixture.run.signals.filter(s => s.client === client);
      assert.ok(messages.every(s => !s.decodeError));
      const join = messages.findIndex(s => s.direction === 'server-to-client' && s.message.join);
      const published = messages.findIndex(s => s.direction === 'server-to-client' && s.message.trackPublished);
      assert.ok(join >= 0 && published > join);
      const requested = new Set(messages.filter(s => s.message.addTrack).map(s => s.message.addTrack.cid));
      for (const message of messages.filter(s => s.message.trackPublished)) {
        assert.ok(requested.has(message.message.trackPublished.cid));
        assert.ok(message.message.trackPublished.track.sid);
      }
      assert.equal(fixture.run.profiles.find(p => p.client === client).info.protocol, 17);
      assert.equal(fixture.run.profiles.find(p => p.client === client).info.clientProtocol, 2);
    }
    const raw = JSON.stringify(fixture);
    assert.ok(!/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/.test(raw), 'JWT leaked');
    assert.ok(!/a=ice-pwd:(?!<redacted>)/.test(raw), 'ICE password leaked');
  });
}
