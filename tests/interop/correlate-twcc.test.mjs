import test from 'node:test';
import assert from 'node:assert/strict';
import { correlate } from './correlate-twcc.mjs';
const epoch = Date.parse('2026-10-24T00:00:00Z');
const raw = { status: 'FAIL', sdk: 'untouched', networkTimeline: [{ run: 4, atMonotonicMs: 1000, phase: 'recovery', clients: [{ peers: [{ connection: 'connected', stats: [
  { type: 'transport', selectedCandidatePairId: 'SECRET_SELECTED' },
  { type: 'candidate-pair', id: 'SECRET_SELECTED', availableOutgoingBitrate: 300000, timestamp: epoch + 1001 },
  { type: 'candidate-pair', id: 'SECRET_OLD', availableOutgoingBitrate: 99000000 },
] }] }] }], proxyClassTrace: { runs: [{ run: 4, coverage: 'OBSERVED_HEADER_INFERENCE', eventOverflow: 2, eventHistoryComplete: false, events: [
  { event: 'rtcp_send_call_returned', atMs: 1005, controlPt: 205, countOrFmt: 15, queueAgeMs: 80, address: 'SECRET_ADDRESS' },
  { event: 'rtcp_send_call_returned', atMs: 1010, controlPt: 200, countOrFmt: 0 },
] }] } };
const line = '2026-10-24T00:00:01.004Z DEBUG transport=SECRET_TRANSPORT observation=BweIncomingTwccObservation { sender_ssrc: 999999, base_seq: 99999, received_packets: 25, missing_packets: 1, actual_span: 15.75ms, encoded_span: 15.75ms, max_encoding_error: 100µs, report_age: Some(201ms), max_report_age: 201ms, max_pending_age: 5ms, max_receiver_gap: 180ms } incoming RTP TWCC metadata (serialization poll, not kernel send)';
test('offline correlation requires explicit clock bridge and never guesses startedAt', () => {
  const result = correlate({ ...raw, startedAt: new Date(epoch).toISOString() }, line, '', 4);
  assert.equal(result.clock.status, 'INSUFFICIENT');
  assert.equal(result.events[0].nearestServerSnapshot, undefined);
  assert.equal(result.serverSnapshots[0].relativeMs, undefined);
});
test('explicit same-process clock correlates safe fields; old selected pair and secrets excluded', () => {
  const result = correlate(raw, line, `OBS_NODE_TIME_ORIGIN_MS=${epoch}\n`, 4);
  assert.equal(result.events.length, 1);
  assert.equal(result.events[0].nearestPublisherSamples[0].sample.availableOutgoingBitrate, 300000);
  assert.equal(result.events[0].nearestServerSnapshot.deltaMs, -1);
  assert.equal(result.serverSnapshots[0].max_encoding_errorMs, 0.1);
  assert.equal(result.serverSnapshots[0].report_ageMs, 201);
  assert.equal(result.coverage.eventHistoryComplete, false);
  assert.ok(!JSON.stringify(result).includes('SECRET'));
  assert.ok(!JSON.stringify(result).includes('999999'));
});
test('ambiguous anchors and nonexistent runs cannot manufacture correlation', () => {
  assert.equal(correlate(raw, line, `OBS_NODE_TIME_ORIGIN_MS=${epoch}\nOBS_NODE_TIME_ORIGIN_MS=${epoch}\n`, 4).clock.status, 'INSUFFICIENT');
  assert.throws(() => correlate(raw, line, '', 3), /no network samples/);
  assert.throws(() => correlate(raw, line, '', -1), /nonnegative/);
});
