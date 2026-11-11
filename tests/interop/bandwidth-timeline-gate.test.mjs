import test from 'node:test';
import assert from 'node:assert/strict';
import { evaluateBudgetTimeline } from './bandwidth-timeline-gate.mjs';
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
