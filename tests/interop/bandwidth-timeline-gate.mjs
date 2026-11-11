import { allocationHorizonInvariant } from './bandwidth-accounting.mjs';

// Native Robust ACK: max sample window 5 s, min sustained window 1 s.
// Probe-finalization history can also reach 5 s. Skip a predeclared 5 s after
// every rate transition, then require 1 s of same-rate qualification history.
// The gate itself is a hypothetical 1 s allocation horizon, NOT measured ACK throughput.
// Transition samples stay in evidence; this is not a reaction-time test.
export const ACCURACY_SETTLE_US = 5000000;
export const ACCURACY_WINDOW_US = 1000000;
export const PROXY_BURST_BYTES = 4500;

export function evaluateBudgetTimeline({ samples, rateChanges, expectedClients, pathIdentityVerifiedAtUs = 0 }) {
  if (samples.length > 10000 || rateChanges.length > 1000 || !Number.isSafeInteger(pathIdentityVerifiedAtUs)) return { status: 'INSUFFICIENT', reason: 'bounded metadata or path verification identity unavailable', samples: [] };
  if (!Number.isSafeInteger(expectedClients) || expectedClients <= 0 || !rateChanges.length) {
    return { status: 'INSUFFICIENT', reason: 'active nominated transport count and rate trace required', samples: [] };
  }
  const rates = rateChanges.map((r, index) => ({ ...r, index }));
  for (let i = 0; i < rates.length; i++) {
    const r = rates[i];
    if (![r.atUs, r.settledByUs, r.bpsPerClient].every(Number.isSafeInteger) || r.atUs < 0 || r.settledByUs < r.atUs || r.bpsPerClient <= 0 || (i && r.atUs <= rates[i - 1].atUs)) {
      return { status: 'INSUFFICIENT', reason: 'invalid rate transition identity', samples: [] };
    }
  }
  const results = samples.map((sample, sampleIndex) => {
    const start = sample.metricsReadStartedUs, end = sample.metricsReadCompletedUs;
    if (![start, end].every(Number.isSafeInteger) || start < 0 || end < start) return { sampleIndex, status: 'INSUFFICIENT', reason: 'metric request interval missing' };
    if (start < pathIdentityVerifiedAtUs) return { sampleIndex, status: 'WARMUP', reason: 'before nominated relay path identity verified' };
    const epoch = rates.findLast(r => r.atUs <= end);
    if (!epoch) return { sampleIndex, status: 'WARMUP', reason: 'before first rate epoch' };
    // Only same-epoch windows are scored. Rate-read overlap is retained as
    // transition evidence, never averaged into a generous steady-state cap.
    const windowStart = start - ACCURACY_WINDOW_US;
    if (windowStart < epoch.settledByUs + ACCURACY_SETTLE_US) return { sampleIndex, epoch: epoch.index, status: 'WARMUP', reason: 'predeclared transition/history window' };
    const otherEpoch = rates.findLast(r => r.atUs <= start);
    if (otherEpoch?.index !== epoch.index || rates[epoch.index + 1]?.atUs <= end) return { sampleIndex, epoch: epoch.index, status: 'WARMUP', reason: 'metric read crosses rate transition' };
    const budget = sample.metrics?.livekit_estimated_capacity_bps;
    if (!Number.isSafeInteger(budget) || budget < 0 || sample.proxy?.clients !== expectedClients || sample.proxy?.bpsPerClient !== epoch.bpsPerClient) {
      return { sampleIndex, epoch: epoch.index, status: 'INSUFFICIENT', reason: 'original budget gauge or exact active path identity missing', budgetBps: budget };
    }
    const aggregateRate = epoch.bpsPerClient * expectedClients;
    const gate = allocationHorizonInvariant({ configuredRateBps: aggregateRate, horizonUs: ACCURACY_WINDOW_US,
      burstBytes: PROXY_BURST_BYTES * expectedClients, budgetBps: budget,
      snapshotIdentityVerified: true, byteDomain: 'native-byte-subset-of-wire' });
    return { sampleIndex, epoch: epoch.index, phase: sample.phase, metricReadStartedUs: start, metricReadCompletedUs: end,
      configuredBpsPerClient: epoch.bpsPerClient, expectedClients, ...gate };
  });
  const epochs = rates.map(r => ({ epoch: r.index, configuredBpsPerClient: r.bpsPerClient,
    stableSamples: results.filter(s => s.epoch === r.index && s.status !== 'WARMUP').length,
    failures: results.filter(s => s.epoch === r.index && s.status === 'FAIL').length,
    insufficient: results.filter(s => s.epoch === r.index && s.status === 'INSUFFICIENT').length }));
  for (const epoch of epochs) {
    const points = results.filter(s => s.epoch === epoch.epoch && s.metricReadStartedUs !== undefined);
    epoch.sampledBudgetMinBps = points.length ? Math.min(...points.map(s => Number(s.budgetBps))) : undefined;
    epoch.sampledBudgetMaxBps = points.length ? Math.max(...points.map(s => Number(s.budgetBps))) : undefined;
    epoch.maximumInterSnapshotGapUs = points.length > 1 ? Math.max(...points.slice(1).map((s, i) => s.metricReadStartedUs - points[i].metricReadCompletedUs)) : undefined;
    epoch.coverage = 'Discrete budget snapshots only; unsampled intervals do not prove absence of overshoot.';
  }
  const missingEpoch = epochs.some(r => r.stableSamples === 0);
  return { status: results.some(s => s.status === 'FAIL') ? 'FAIL' : missingEpoch || results.some(s => s.status === 'INSUFFICIENT') ? 'INSUFFICIENT' : 'PASS',
    settleUs: ACCURACY_SETTLE_US, allocationHorizonUs: ACCURACY_WINDOW_US, sameRateQualificationHistoryUs: ACCURACY_WINDOW_US, burstBytesPerClient: PROXY_BURST_BYTES,
    expectedClients, rateChanges, epochs, samples: results,
    scope: 'One-second allocation-horizon policy invariant on original aggregate budget snapshots after stable rate qualification; NOT an observed sustained throughput/TWCC window, full capacity discovery, continuous overshoot coverage or reaction-time test.' };
}
