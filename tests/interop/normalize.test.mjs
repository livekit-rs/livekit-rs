import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizer } from './normalize.mjs';

test('fixtures redact credentials and addresses but preserve identifiers consistently', () => {
  const normalize = normalizer();
  const result = normalize({
    refreshToken: 'eyJ.secret.signature', protocol: 17, requestToken: 4,
    participant: { sid: 'PA_example', address: '203.0.113.4' },
    sdp: 'o=- 123 4 IN IP4 203.0.113.4\r\na=msid:PA_example|TR_example cid\r\na=ice-ufrag:private\r\na=ice-pwd:password\r\na=fingerprint:sha-256 11:22\r\na=candidate:123 1 udp 1234 203.0.113.4 12345 typ srflx raddr 192.168.0.1 rport 23456 ufrag private\r\n',
    candidate: JSON.stringify({ candidate: 'candidate:456 1 udp 1234 203.0.113.4 12345 typ host ufrag private' })
  });
  const encoded = JSON.stringify(result);
  for (const secret of ['eyJ.secret.signature', '203.0.113.4', '192.168.0.1', 'password', 'private']) assert.ok(!encoded.includes(secret), secret);
  assert.equal(result.protocol, 17); assert.equal(result.requestToken, 4);
  assert.ok(result.sdp.includes(result.participant.sid + '|'));
  assert.equal(normalize('PA_example'), result.participant.sid);
  assert.ok(result.sdp.includes('typ srflx'));
});
