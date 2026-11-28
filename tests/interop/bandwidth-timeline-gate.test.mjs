import test from 'node:test';
import assert from 'node:assert/strict';
import { accuracyVerdict, evaluateBudgetTimeline, evaluateRecoveryProgress, evaluateSeedResponse, SEED_CAPACITY_BPS } from './bandwidth-timeline-gate.mjs';
const rateChanges = [{ atUs: 0, settledByUs: 0, bpsPerClient: 4000000 }, { atUs: 20000000, settledByUs: 20000010, bpsPerClient: 120000 }, { atUs: 40000000, settledByUs: 40000010, bpsPerClient: 4000000 }];
const sample = (at, bps, budget) => ({ metricsReadStartedUs: at, metricsReadCompletedUs: at + 1000, metrics: { livekit_estimated_capacity_bps: budget }, proxy: { clients: 2, bpsPerClient: bps } });
const valid = [sample(6000000, 4000000, 8072000), sample(26000010, 120000, 312000), sample(46000010, 4000000, 8072000)];
test('all three stable epochs use exact per-path rate and burst, no fixed 4 Mbps clamp', () => {
  assert.equal(evaluateBudgetTimeline({ samples: valid, rateChanges, expectedClients: 2 }).status, 'PASS');
  const result = evaluateBudgetTimeline({ samples: [sample(6000000, 9000000, 18072000)], rateChanges: [{ atUs: 0, settledByUs: 0, bpsPerClient: 9000000 }], expectedClients: 2 });
  assert.equal(result.status, 'PASS');
});
test('old sustained 18.331 Mbps and low-phase 516 kbps fail without clipping gauge', () => {
  assert.equal(evaluateBudgetTimeline({ samples: [sample(6000000, 4000000, 18331200), ...valid.slice(1)], rateChanges, expectedClients: 2 }).status, 'FAIL');
  assert.equal(evaluateBudgetTimeline({ samples: [valid[0], sample(26000010, 120000, 516946), valid[2]], rateChanges, expectedClients: 2 }).status, 'FAIL');
});
test('predeclared five-second history plus one-second window retains transition spikes', () => {
  const result = evaluateBudgetTimeline({ samples: [sample(25999999, 120000, 22000000), ...valid], rateChanges, expectedClients: 2 });
  assert.equal(result.status, 'PASS'); assert.equal(result.samples[0].status, 'WARMUP');
  assert.equal(evaluateBudgetTimeline({ samples: [valid[0], valid[2]], rateChanges, expectedClients: 2 }).status, 'INSUFFICIENT');
});
test('missing original gauge, path identity or sample timestamp cannot pass', () => {
  for (const broken of [{ ...valid[1], metrics: {} }, { ...valid[1], proxy: { clients: 3, bpsPerClient: 120000 } }, { ...valid[1], metricsReadStartedUs: undefined }]) {
    assert.equal(evaluateBudgetTimeline({ samples: [valid[0], broken, valid[2]], rateChanges, expectedClients: 2 }).status, 'INSUFFICIENT');
  }
});
// Per-path proxy byte counters: esample(atMs, startUs, bps, budget, cumulativeBytesPerClient). The
// proxy carries its own reading timestamp (tests/interop/network-proxy.mjs:106), and only differences
// between consecutive readings matter.
const rates2 = [{ atUs: 0, settledByUs: 0, bpsPerClient: 120000 }];
const esample = (atMs, startUs, bps, budget, bytes) => ({ ...sample(startUs, bps, budget), atMonotonicMs: atMs,
  proxy: { atMonotonicMs: atMs, clients: 2, bpsPerClient: bps,
    perClient: (bytes ?? [0, 0]).map(forwardedIpv4Bytes => ({ forwardedIpv4Bytes,
      expiredPackets: 0, queueLimitDrops: 0, oversizedDrops: 0 })) } });
const lossy = s => ({ ...s, proxy: { ...s.proxy, perClient: s.proxy.perClient.map(c => ({ ...c, expiredPackets: 40 })) } });
test('an aggregate budget still inside the per-path capacity prior is scored, not credited', () => {
  // 100 Mbps aggregate is the untouched 50 Mbps-per-path prior band, so it cannot certify a budget.
  const prior = esample(26000, 26000010, 120000, 100000000, [127500, 127500]);
  const quiet = [esample(25490, 25500000, 120000, 100000000, [0, 0]), prior];
  assert.equal(evaluateBudgetTimeline({ samples: quiet, rateChanges: rates2, expectedClients: 2 }).status, 'INSUFFICIENT');
  assert.equal(evaluateBudgetTimeline({ samples: [prior], rateChanges: rates2, expectedClients: 2 }).epochs[0].priorOnly, true);
  assert.equal(evaluateBudgetTimeline({ samples: quiet, rateChanges: rates2, expectedClients: 2 }).epochs[0].provenUnimpacted, true);
  assert.equal(evaluateBudgetTimeline({ samples: [quiet[0], lossy(prior)], rateChanges: rates2, expectedClients: 2 }).status, 'INSUFFICIENT');
});
test('an epoch whose paths demonstrably lost packets has to produce an observed budget', () => {
  const rates3 = [{ atUs: 0, settledByUs: 0, bpsPerClient: 4000000 }, { atUs: 20000000, settledByUs: 20000010, bpsPerClient: 120000 }];
  const warm = esample(6000, 6000000, 4000000, 8072000, [0, 0]);
  const priorQuiet = esample(26000, 26000010, 120000, 100000000, [500000, 500000]);
  assert.equal(evaluateBudgetTimeline({ samples: [warm, priorQuiet], rateChanges: rates3, expectedClients: 2 }).status, 'PASS');
  assert.equal(evaluateBudgetTimeline({ samples: [warm, lossy(priorQuiet)], rateChanges: rates3, expectedClients: 2 }).status, 'INSUFFICIENT');
});
test('budget is capped by the highest rate the paths delivered, not only the configured rate', () => {
  // The two paths already carried 4 Mbps aggregate between 25.5s and 26.0s, so a 3.1 Mbps budget in
  // the 120 kbps epoch is a stale belief both implementations hold, while 4.2 Mbps is invented.
  const stale = esample(26000, 26000010, 120000, 3100000, [127500, 127500]);
  const invented = esample(26000, 26000010, 120000, 4200000, [127500, 127500]);
  const lead = esample(25490, 25500000, 120000, 100000000, [0, 0]);
  const staleResult = evaluateBudgetTimeline({ samples: [lead, stale], rateChanges: rates2, expectedClients: 2 });
  assert.equal(staleResult.status, 'PASS');
  assert.equal(staleResult.samples[1].ceilingSource, 'delivered-peak');
  assert.equal(staleResult.samples[1].demonstratedPeakBps, 4000000);
  assert.equal(evaluateBudgetTimeline({ samples: [lead, invented], rateChanges: rates2, expectedClients: 2 }).status, 'FAIL');
  // Without byte-level path evidence the ceiling stays the strict configured rate.
  assert.equal(evaluateBudgetTimeline({ samples: [sample(25500000, 120000, 100000000), sample(26000010, 120000, 3100000)], rateChanges: rates2, expectedClients: 2 }).status, 'FAIL');
});
const rec = (key, atMs, requestedBps, targetRank, capacityBps) => ({ key, atMs, requestedBps, targetRank, capacityBps });
const window = { wallAtMs: 10000, restoreAtMs: 28000, windowMs: 95000 };
test('recovery progress needs a per-path dip under the wall and a raise after it', () => {
  const responded = [rec('p1', 5000, 1800000, 200, 100000000), rec('p1', 12000, 127529, 0, 39434),
    rec('p1', 40000, 1980827, 200, 362104)];
  assert.equal(evaluateRecoveryProgress({ records: responded, ...window }).status, 'PASS');
  // A layer climb that only adds bytes without returning any managed layer is not recovery.
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200), rec('p1', 12000, 127529, -100),
    rec('p1', 40000, 200000, -100)], ...window }).status, 'FAIL');
  // Never cut: the request stays at the pre-wall level, so the dip clause cannot credit a reaction.
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200), rec('p1', 40000, 1750000, 200)], ...window }).status, 'FAIL');
  // Cut and pinned: no raise above its own floor after the wall lifted.
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200), rec('p1', 12000, 127529, 0),
    rec('p1', 40000, 127529, 0)], ...window }).status, 'FAIL');
});
test('recovery dip also counts as a granted-layer drop at unchanged bandwidth', () => {
  const result = evaluateRecoveryProgress({ records: [rec('p1', 5000, 700000, 200), rec('p1', 12000, 690000, 100),
    rec('p1', 40000, 800000, 200)], ...window });
  assert.equal(result.status, 'PASS');
  assert.equal(result.paths[0].dipped, true);
});
test('recovery is scored per path, so an untouched neighbour does not void a responding one', () => {
  const inert = [rec('p2', 5000, 2100000, 200, 100000000), rec('p2', 40000, 2100000, 200, 100000000)];
  const responded = [rec('p1', 5000, 1800000, 200, 100000000), rec('p1', 12000, 127529, 0, 39434),
    rec('p1', 40000, 1980827, 200, 362104)];
  const result = evaluateRecoveryProgress({ records: [...inert, ...responded], ...window });
  assert.equal(result.status, 'PASS');
  assert.deepEqual(result.paths.map(p => [p.key, p.passed]), [['p2', false], ['p1', true]]);
  assert.deepEqual(result.paths.find(p => p.key === 'p1').capacityMinBps, 39434);
});
test('recovery without records, window or a post-restore sample proves nothing', () => {
  assert.equal(evaluateRecoveryProgress({ records: [], ...window }).status, 'INSUFFICIENT');
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200)], ...window }).status, 'INSUFFICIENT');
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200), rec('p1', 200000, 100, 0)], ...window }).status, 'INSUFFICIENT');
  assert.equal(evaluateRecoveryProgress({ records: [rec('p1', 5000, 1800000, 200)], wallAtMs: 10000 }).status, 'INSUFFICIENT');
});
test('a bandwidth dip on a path the server never touched is not a recovery response', () => {
  // Measured 2026-11-28 on the pair14rust leg: transport-1 requested 1,800,000 -> 1,219,798 (-32%)
  // with its granted layer held at rank 200, committed capacity pinned at the seed and 0 deficient
  // records, i.e. encoder bitrate wobble on a path that never reacted. The raise clause is satisfied,
  // so only the dip clause can discriminate.
  const wobble = [{ key: 'p1', atMs: 5000, requestedBps: 1800000, targetRank: 200, capacityBps: SEED_CAPACITY_BPS },
    { key: 'p1', atMs: 12000, requestedBps: 1219798, targetRank: 200, capacityBps: SEED_CAPACITY_BPS },
    { key: 'p1', atMs: 40000, requestedBps: 2209741, targetRank: 300, capacityBps: SEED_CAPACITY_BPS }];
  const inert = evaluateRecoveryProgress({ records: wobble, ...window });
  assert.equal(inert.status, 'FAIL');
  assert.equal(inert.paths[0].dipped, false);
  assert.equal(inert.paths[0].raised, true);
  // The same trace is a response once the server reports the path deficient under the wall.
  assert.equal(evaluateRecoveryProgress({ records: wobble.map((r, i) => ({ ...r, deficient: i === 1 })), ...window }).status, 'PASS');
  // ...or once its own committed capacity moved off the seed.
  assert.equal(evaluateRecoveryProgress({ records: wobble.map((r, i) => ({ ...r, capacityBps: i === 1 ? 39434 : SEED_CAPACITY_BPS })), ...window }).status, 'PASS');
});

// Per-path receiver-response certification. Rows are `remote feedback sample` records
// (crates/rtc-media/src/feedback.rs) already bound to a browser by forwarded SSRC.
const fr = (key, atMs, estimateBps, capacityBps, observation) =>
  ({ key, pathBound: true, atMs, estimateBps, capacityBps, observation });
const seedWindow = { wallAtMs: 10000, restoreAtMs: 28000 };
test('a receiver whose estimate collapsed has to move the committed capacity off the seed', () => {
  // pair14rust client-0 / transport-2, measured values.
  const responded = [fr('client-0', 5000, 597516, SEED_CAPACITY_BPS), fr('client-0', 12000, 17862, 17862),
    fr('client-0', 40000, 622816, 2179520)];
  const result = evaluateSeedResponse({ records: responded, ...seedWindow });
  assert.equal(result.status, 'PASS');
  assert.equal(result.paths[0].verdict, 'RESPONDED');
  assert.equal(result.paths[0].underWallCapacityMinBps, 17862);
  assert.equal(result.paths[0].afterWallCapacityMaxBps, 2179520);
  // A server holding the seed while its receiver reports 17,862bps is exactly the failure this gates.
  assert.equal(evaluateSeedResponse({ records: [fr('client-0', 5000, 2000000, SEED_CAPACITY_BPS),
    fr('client-0', 12000, 17862, SEED_CAPACITY_BPS)], ...seedWindow }).status, 'FAIL');
  // At the seed, not below it: the comparison is strict.
  assert.equal(evaluateSeedResponse({ records: [fr('client-0', 5000, 2000000, SEED_CAPACITY_BPS),
    fr('client-0', 12000, 17862, SEED_CAPACITY_BPS - 1)], ...seedWindow }).status, 'PASS');
});
test('a congested observer trend obligates a path even when its estimate never collapsed', () => {
  // pair12rust client-0/1: receiver estimates stayed near their pre-wall values (the publisher encoder
  // backed off first), so only the server's own Congesting/Loss trend makes the path accountable.
  const inertServer = [fr('client-0', 5000, 446600, SEED_CAPACITY_BPS),
    fr('client-0', 12000, 1856368, SEED_CAPACITY_BPS, 'Congesting/Loss nack_ratio=1.000')];
  const result = evaluateSeedResponse({ records: inertServer, ...seedWindow });
  assert.equal(result.status, 'FAIL');
  assert.equal(result.paths[0].estimateCollapsed, false);
  assert.equal(result.paths[0].congestionReported, true);
  assert.equal(result.paths[0].verdict, 'OBLIGATED_NOT_RESPONDED');
  // The same obligation discharged by a commit below the seed (pair12rust client-1 reached 291,943).
  assert.equal(evaluateSeedResponse({ records: inertServer.map((r, i) =>
    ({ ...r, capacityBps: i === 1 ? 291943 : r.capacityBps })), ...seedWindow }).status, 'PASS');
});
test('an unresponsive neighbour is disclosed, not scored, against a path that did respond', () => {
  // pair14rust, measured: client-1's receiver estimate never fell (pre 187,560-281,078, under
  // 286,406-309,318) and its observer never reported congestion, so its seed is not a failure.
  // client-0 collapsed 597,516 -> 17,862 and committed 100,000,000 -> 17,862 -> RESPONDED.
  const quiet = [fr('client-1', 5000, 281078, SEED_CAPACITY_BPS), fr('client-1', 12000, 286406, SEED_CAPACITY_BPS),
    fr('client-1', 40000, 309318, SEED_CAPACITY_BPS)];
  const responded = [fr('client-0', 5000, 597516, SEED_CAPACITY_BPS), fr('client-0', 12000, 17862, 17862)];
  const result = evaluateSeedResponse({ records: [...quiet, ...responded], ...seedWindow });
  assert.equal(result.status, 'PASS');
  assert.deepEqual(result.paths.map(p => [p.key, p.verdict]), [['client-1', 'SEED_RETAINED'], ['client-0', 'RESPONDED']]);
  assert.match(result.reason, /1 path\(s\) held it with no receiver congestion/);
  // Same quiet neighbour, now with a collapsed estimate: the leg fails instead of hiding behind p1.
  assert.equal(evaluateSeedResponse({ records: [fr('client-1', 5000, 2100000, SEED_CAPACITY_BPS),
    ...quiet.slice(1), ...responded], ...seedWindow }).status, 'FAIL');
});
test('unbound, capacity-less or window-less receiver rows certify nothing', () => {
  const collapsed = { atMs: 12000, estimateBps: 17862 };
  assert.equal(evaluateSeedResponse({ records: [{ ...collapsed, key: 'ssrc:8,9', pathBound: false }], ...seedWindow }).status, 'INSUFFICIENT');
  // Go exports no committed capacity, so every bound path is UNSUPPORTED rather than silently clean.
  const goShaped = evaluateSeedResponse({ records: [{ ...collapsed, key: 'client-0', pathBound: true, capacityBps: undefined }], ...seedWindow });
  assert.equal(goShaped.status, 'INSUFFICIENT');
  assert.equal(goShaped.paths[0].verdict, 'UNSUPPORTED');
  assert.equal(evaluateSeedResponse({ records: [], ...seedWindow }).status, 'INSUFFICIENT');
  assert.equal(evaluateSeedResponse({ records: [fr('client-0', 12000, 17862, 17862)], wallAtMs: 10000 }).status, 'INSUFFICIENT');
  assert.equal(evaluateSeedResponse({ records: [fr('client-0', 12000, 17862, 17862)], wallAtMs: 28000, restoreAtMs: 10000 }).status, 'INSUFFICIENT');
});

// The roll-up is where a completed leg used to overwrite the accounting verdict with PASS, so the
// ordering it enforces has to be discriminable on its own.
test('a finished leg cannot promote the accuracy verdict', () => {
  assert.equal(accuracyVerdict([]), 'INSUFFICIENT');
  assert.equal(accuracyVerdict([{ runStatus: 'PASS' }]), 'PASS');
  assert.equal(accuracyVerdict([{ runStatus: 'PASS_PER_PATH' }]), 'PASS_PER_PATH');
  assert.equal(accuracyVerdict([{ runStatus: 'PASS' }, { runStatus: 'PASS_PER_PATH' }]), 'PASS_PER_PATH');
  assert.equal(accuracyVerdict([{ runStatus: 'PASS' }, { runStatus: 'INSUFFICIENT' }]), 'INSUFFICIENT');
  assert.equal(accuracyVerdict([{ runStatus: 'PASS' }, { runStatus: 'FAIL' }]), 'FAIL');
});

