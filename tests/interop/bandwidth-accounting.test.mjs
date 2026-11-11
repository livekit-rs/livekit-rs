import test from 'node:test';
import { TokenBucket } from './network-proxy.mjs';
import assert from 'node:assert/strict';
import { integratedRate, wireEnvelope, auditTokenLedger, sustainedBudgetGate, wireConservation, replayProbe, allocationHorizonInvariant } from './bandwidth-accounting.mjs';

const rates = [{ atUs: 0, bps: 4000000 }];
test('burst envelope is derived from bytes and time, not a chosen percentage', () => {
  assert.equal(wireEnvelope({ rateChanges: rates, startUs: 0, endUs: 2000, burstBytes: 4500, wireBytes: 5500 }).status, 'PASS');
  assert.equal(wireEnvelope({ rateChanges: rates, startUs: 0, endUs: 2000, burstBytes: 4500, wireBytes: 5501 }).status, 'FAIL');
  assert.equal(wireEnvelope({ rateChanges: rates, startUs: 0, endUs: 1000000, burstBytes: 4500, wireBytes: 504500 }).status, 'PASS');
});
test('variable-rate integration includes the true transition and no extra new burst', () => {
  const changes = [{ atUs: 0, bps: 4000000 }, { atUs: 500000, bps: 120000 }];
  assert.equal(integratedRate(changes, 0, 1000000), 2060000000000n);
  assert.equal(wireEnvelope({ rateChanges: changes, startUs: 0, endUs: 1000000, burstBytes: 4500, wireBytes: 262000 }).status, 'PASS');
  assert.equal(wireEnvelope({ rateChanges: changes, startUs: 0, endUs: 1000000, burstBytes: 4500, wireBytes: 262001 }).status, 'FAIL');
});
test('token ledger rejects an extra packet even at identical timestamps', () => {
  assert.equal(auditTokenLedger({ initialBps: 4000000, burstBytes: 4500, events: [{ type: 'service', atUs: 0, wireBytes: 4500 }, { type: 'service', atUs: 0, wireBytes: 1 }] }).status, 'FAIL');
  assert.equal(auditTokenLedger({ initialBps: 4000000, burstBytes: 4500, events: [{ type: 'service', atUs: 0, wireBytes: 4500 }, { type: 'rate', atUs: 1000000, bps: 120000 }, { type: 'service', atUs: 1000000, wireBytes: 4500 }, { type: 'service', atUs: 2000000, wireBytes: 4500 }] }).status, 'PASS');
});
test('18.331 Mbps short probe cannot become a one-second sustained 4 Mbps budget', () => {
  const input = { rateChanges: rates, startUs: 1000000, endUs: 2000000, burstBytes: 4500, budgetBps: 18331200, minimumWindowUs: 1000000, completeObservationWindow: true, byteDomain: 'native-byte-subset-of-wire' };
  const result = sustainedBudgetGate(input);
  assert.equal(result.status, 'FAIL'); assert.equal(result.maximumBudgetBps.value, 4036000);
  assert.equal(sustainedBudgetGate({ ...input, budgetBps: 4036000 }).status, 'PASS');
  assert.equal(sustainedBudgetGate({ ...input, budgetBps: 4036001 }).status, 'FAIL');
  assert.equal(sustainedBudgetGate({ ...input, endUs: 1002000 }).status, 'INSUFFICIENT');
  assert.equal(sustainedBudgetGate({ ...input, completeObservationWindow: false }).status, 'INSUFFICIENT');
});
test('same formula handles other capacities, low rate and explicit timing uncertainty', () => {
  const common = { startUs: 1000000, endUs: 2000000, burstBytes: 4500, minimumWindowUs: 1000000, completeObservationWindow: true, byteDomain: 'wire' };
  assert.equal(sustainedBudgetGate({ ...common, rateChanges: [{ atUs: 0, bps: 120000 }], budgetBps: 156000 }).status, 'PASS');
  assert.equal(sustainedBudgetGate({ ...common, rateChanges: [{ atUs: 0, bps: 120000 }], budgetBps: 156001 }).status, 'FAIL');
  assert.equal(sustainedBudgetGate({ ...common, rateChanges: [{ atUs: 0, bps: 9000000 }], budgetBps: 9036000 }).status, 'PASS');
  const result = sustainedBudgetGate({ ...common, rateChanges: rates, budgetBps: 4038000, timingErrorUs: 500 });
  assert.equal(result.status, 'PASS'); assert.equal(result.maximumBudgetBps.value, 4040000000000 / 999000);
  assert.equal(sustainedBudgetGate({ ...common, rateChanges: rates, budgetBps: 4000000, byteDomain: 'unknown' }).status, 'INSUFFICIENT');
});
test('queue wire accounting exposes every dropped, failed and reset byte', () => {
  const input = { offeredWireBytes: 10000, queueStartWireBytes: 2000, servedWireBytes: 8000, droppedWireBytes: 1000, sendFailedWireBytes: 500, resetDiscardedWireBytes: 500, queueEndWireBytes: 2000 };
  assert.equal(wireConservation(input).status, 'PASS');
  assert.equal(wireConservation({ ...input, droppedWireBytes: 999 }).status, 'FAIL');
});
test('native probe replay excludes real endpoint sizes without granting capacity', () => {
  const p = { totalBytes: 6000, excludedLastSendBytes: 1200, excludedFirstRecvBytes: 1200, sendSpanUs: 2000, recvSpanUs: 2000, receivedPackets: 5 };
  assert.equal(replayProbe(p).nativeEstimateBps.value, 19200000);
  assert.equal(replayProbe({ ...p, recvSpanUs: 10000 }).nativeEstimateBps.value, 3648000);
  assert.equal(replayProbe({ ...p, recvSpanUs: 500 }).status, 'INVALID_SAMPLE');
  assert.equal(replayProbe({ ...p, receivedPackets: 3 }).status, 'INVALID_SAMPLE');
});

test('allocation horizon invariant is explicit policy, not forged observed feedback', () => {
  const input = { configuredRateBps: 8000000, horizonUs: 1000000, burstBytes: 9000, budgetBps: 18331200, snapshotIdentityVerified: true, byteDomain: 'native-byte-subset-of-wire' };
  assert.equal(allocationHorizonInvariant(input).status, 'FAIL');
  assert.equal(allocationHorizonInvariant({ ...input, budgetBps: 8072000 }).status, 'PASS');
  assert.equal(allocationHorizonInvariant({ ...input, snapshotIdentityVerified: false }).status, 'INSUFFICIENT');
  assert.equal(allocationHorizonInvariant({ ...input, horizonUs: 1000 }).maximumBudgetBps.value, 80000000);
});

test('independent rational ledger audits REAL proxy TokenBucket accepted trace', () => {
  const bucket = new TokenBucket(4000000, 0, 4500), events = [];
  let seed = 0x12345678, atMs = 0, accepted = 0;
  const random = () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed; };
  for (let i = 0; i < 20000; i++) {
    atMs += random() % 20;
    if (i % 71 === 0) {
      const bps = [120000, 4000000, 9000000][random() % 3];
      bucket.setRate(bps, atMs); events.push({ type: 'rate', atUs: atMs * 1000, bps });
    }
    for (let p = 0; p < 3; p++) {
      const wireBytes = 28 + random() % 1400;
      if (bucket.take(wireBytes, atMs)) { events.push({ type: 'service', atUs: atMs * 1000, wireBytes }); accepted++; }
    }
  }
  assert.ok(accepted > 20000);
  assert.equal(auditTokenLedger({ initialBps: 4000000, burstBytes: 4500, events }).status, 'PASS');
  assert.equal(auditTokenLedger({ initialBps: 4000000, burstBytes: 4500, events: [...events, { type: 'service', atUs: atMs * 1000, wireBytes: 4501 }] }).status, 'FAIL');
});

test('actual 405345/403426 candidate trains replay but cannot qualify one-second validation', async () => {
  const { readFile } = await import('node:fs/promises');
  const fixture = JSON.parse(await readFile(new URL('./short-probe-observations.json', import.meta.url), 'utf8'));
  for (const observation of fixture.observations) {
    const correctedSend = observation.totalBytes - observation.excludedLastSendBytes;
    const correctedRecv = observation.totalBytes - observation.excludedFirstRecvBytes;
    const sendRate = correctedSend * 8e9 / observation.sendSpanNs;
    const recvRate = correctedRecv * 8e9 / observation.recvSpanNs;
    const exact = recvRate < sendRate * 0.9 ? recvRate * 0.95 : Math.min(sendRate, recvRate);
    // Native and JS expression order can differ by one IEEE-754 ULP.
    // Check the production integer-bps representation, not a percentage band.
    assert.equal(Math.round(exact), Math.round(observation.nativeEstimateBps));
    assert.ok(observation.sendSpanNs < 1000000000 && observation.recvSpanNs < 1000000000);
    const replay = replayProbe({ ...observation, sendSpanUs: observation.sendSpanUsTruncated, recvSpanUs: observation.recvSpanUsTruncated });
    assert.equal(replay.status, 'REPLAYED');
    // as_micros logs truncate sub-microsecond send time. This exact interval
    // accounts for the difference; it is not a chosen percentage tolerance.
    const sendLow = correctedSend * 8e6 / (observation.sendSpanUsTruncated + 1);
    const sendHigh = correctedSend * 8e6 / observation.sendSpanUsTruncated;
    assert.ok(sendRate >= sendLow && sendRate <= sendHigh);
    assert.equal(sustainedBudgetGate({ rateChanges: rates, startUs: 1000000,
      endUs: 1000000 + Math.max(observation.sendSpanUsTruncated, observation.recvSpanUsTruncated),
      burstBytes: 4500, budgetBps: Math.round(observation.nativeEstimateBps), minimumWindowUs: 1000000,
      completeObservationWindow: true, byteDomain: 'native-byte-subset-of-wire' }).status, 'INSUFFICIENT');
  }
});
