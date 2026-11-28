import { allocationHorizonInvariant } from './bandwidth-accounting.mjs';

// Native Robust ACK: max sample window 5 s, min sustained window 1 s.
// Probe-finalization history can also reach 5 s. Skip a predeclared 5 s after
// every rate transition, then require 1 s of same-rate qualification history.
// The gate itself is a hypothetical 1 s allocation horizon, NOT measured ACK throughput.
// Transition samples stay in evidence; this is not a reaction-time test.
export const ACCURACY_SETTLE_US = 5000000;
export const ACCURACY_WINDOW_US = 1000000;
export const PROXY_BURST_BYTES = 4500;
// Both implementations seed the channel capacity at 100 Mbps per path and move it
// only on a congested trend or a probe credit (Go remote_bwe.go Reset()/
// estimateAvailableChannelCapacity, Rust feedback.rs committed seed). An
// unqualified seed is a prior, not an observation of this path, so it is filed
// separately instead of scored. Half the seed is the band edge: an aggregate
// budget at or above it still contains an unobserved path prior.
export const UNQUALIFIED_PRIOR_BPS_PER_CLIENT = 50000000;
// Both engines seed a subscriber path at 100 Mbps and move it only on a congested trend or a probe
// credit, so this is the value a path holds when its receiver never reported congestion
// (Go remote_bwe.go Reset(), Rust remote_bwe/mod.rs RemoteController::new).
export const SEED_CAPACITY_BPS = 100000000;

// Roll-up of the per-run accuracy verdicts. A leg that reached its end is not itself a verdict: the
// aggregate gate can stay INSUFFICIENT while only the per-path receiver arm certified anything, and
// `PASS_PER_PATH` must not be laundered into `PASS` by the surrounding run completing. Any run that
// failed or certified nothing dominates, so one weak run cannot be hidden behind a strong one.
export function accuracyVerdict(runs) {
  if (!Array.isArray(runs) || !runs.length) return 'INSUFFICIENT';
  for (const bad of ['FAIL', 'INSUFFICIENT']) if (runs.some(run => run.runStatus === bad)) return bad;
  return runs.every(run => run.runStatus === 'PASS') ? 'PASS' : 'PASS_PER_PATH';
}

// Delivered rate and losses per snapshot, derived from the bottleneck proxy's own
// per-path counters between consecutive samples. This is external ground truth:
// the highest rate the path has already carried bounds what the SFU may still
// believe, while a rate above anything delivered is an invented budget.
function derivePathEvidence(samples, expectedClients) {
  let demonstratedPeakBps = 0;
  return samples.map((sample, index) => {
    const current = sample.proxy, previous = index ? samples[index - 1].proxy : undefined;
    if (!current?.perClient || current.perClient.length !== expectedClients) return { demonstratedPeakBps };
    if (previous?.perClient?.length !== expectedClients) return { demonstratedPeakBps };
    const spanMs = current.atMonotonicMs - previous.atMonotonicMs;
    if (!Number.isFinite(spanMs) || spanMs <= 0) return { demonstratedPeakBps };
    let deliveredBytes = 0, droppedPackets = 0;
    for (let path = 0; path < expectedClients; path++) {
      const now = current.perClient[path], before = previous.perClient[path];
      if (!Number.isSafeInteger(now?.forwardedIpv4Bytes) || !Number.isSafeInteger(before?.forwardedIpv4Bytes)) return { demonstratedPeakBps };
      deliveredBytes += Math.max(0, now.forwardedIpv4Bytes - before.forwardedIpv4Bytes);
      for (const counter of ['expiredPackets', 'queueLimitDrops', 'oversizedDrops']) {
        if ((now?.[counter] ?? 0) < (before?.[counter] ?? 0)) return { demonstratedPeakBps };
        droppedPackets += (now?.[counter] ?? 0) - (before?.[counter] ?? 0);
      }
    }
    const deliveredBps = Math.round(deliveredBytes * 8000 / spanMs);
    demonstratedPeakBps = Math.max(demonstratedPeakBps, deliveredBps);
    return { deliveredBps, droppedPackets, demonstratedPeakBps };
  });
}

// Recovery progress, scored per nominated path on the allocator's own records.
//
// The aggregate capacity gauge cannot carry this claim: it sums one committed
// capacity per subscriber connection (crates/rtc-runtime/src/manager.rs), so in a
// two-path leg where one connection collapses to tens of kbps and the other never
// congests, the sum stays pinned near the untouched 100,000,000 seed and no
// relative staircase on it is satisfiable. Measured on the 2026-11-27 C108 legs:
// transport-1 committed 100,000,000 -> 39,434 then climbed 68,525/83,694/129,481/
// 163,244/205,713/362,104 while transport-2 held its seed, and the summed gauge
// moved 0.03-0.10% per step, i.e. 0 of 26 qualified samples ever at or below the
// 8,000,000 open-path aggregate and no 5% step. The claim therefore has to be
// made per path, on a record source both engines have: the allocation decision
// itself (`stream allocation:` in the pinned Go, `alloc cost sample` here).
//
// A path that responded to the wall and then recovered must show both a dip below
// its own pre-wall level (in bandwidth or in granted layer rank) and a raise above
// its own constrained floor after the wall lifted. Requiring the raise in bandwidth
// *and* layer rank keeps a server that pauses a track forever (floor 0, rank -100,
// top 0, rank -100) from passing on noise, and a server that never cut from passing
// on its own seed.
export function evaluateRecoveryProgress({ records, wallAtMs, restoreAtMs, windowMs, cutRatio = 0.7 }) {
  if (!Array.isArray(records) || !records.length
    || ![wallAtMs, restoreAtMs, windowMs].every(Number.isFinite) || windowMs <= 0) {
    return { status: 'INSUFFICIENT', reason: 'no allocator records or recovery window', cutRatio, paths: [] };
  }
  const windowEndMs = restoreAtMs + windowMs;
  const groups = new Map();
  for (const record of records) {
    if (!Number.isFinite(record?.atMs) || typeof record.key !== 'string' || record.atMs > windowEndMs) continue;
    const bucket = record.atMs < wallAtMs ? 'before' : record.atMs <= restoreAtMs ? 'under' : 'after';
    const row = groups.get(record.key) ?? { key: record.key, before: [], under: [], after: [] };
    row[bucket].push({ atMs: record.atMs, bps: record.requestedBps ?? 0,
      rank: record.targetRank ?? -Infinity, capacityBps: record.capacityBps, deficient: record.deficient === true });
    groups.set(record.key, row);
  }
  const paths = [...groups.values()].map(row => {
    const peakBeforeBps = Math.max(0, ...row.before.map(s => s.bps));
    const peakBeforeRank = row.before.length ? Math.max(...row.before.map(s => s.rank)) : -Infinity;
    const span = [...row.under, ...row.after];
    const floorBps = span.length ? Math.min(...span.map(s => s.bps)) : undefined;
    const floorRank = span.length ? Math.min(...span.map(s => s.rank)) : undefined;
    const topBps = row.after.length ? Math.max(...row.after.map(s => s.bps)) : undefined;
    const topRank = row.after.length ? Math.max(...row.after.map(s => s.rank)) : undefined;
    const capacities = span.map(s => s.capacityBps).filter(Number.isFinite);
    const observed = row.before.length > 0 && row.after.length > 0;
    // A requested-bandwidth dip is only credited when the server itself cut the path: a granted
    // layer that fell below its own pre-wall level is the server's own decision, otherwise the demand
    // has to have fallen while the SFU reported the path deficient or moved its committed capacity off
    // the seed. Encoder bitrate wobble on a path that never reacted (measured 2026-11-28: requested
    // 1,800,000 -> 1,219,798 with committed capacity pinned at 100,000,000 and 0 deficient records) is
    // not a response to the wall.
    const rankDipped = floorRank < peakBeforeRank;
    const sfuSignal = rankDipped || span.some(s => s.deficient)
      || (capacities.length > 0 && Math.min(...capacities) < SEED_CAPACITY_BPS);
    const dipped = observed && (rankDipped
      || (floorBps <= peakBeforeBps * cutRatio && sfuSignal));
    const raised = observed && topBps > floorBps && topRank > floorRank;
    return { key: row.key, observed, beforeRecords: row.before.length, underRecords: row.under.length,
      afterRecords: row.after.length, peakBeforeBps: observed ? peakBeforeBps : undefined,
      peakBeforeRank: observed ? peakBeforeRank : undefined, floorBps, floorRank, topBps, topRank,
      dipped, raised, passed: dipped && raised,
      capacityMinBps: capacities.length ? Math.min(...capacities) : undefined,
      capacityMaxBps: capacities.length ? Math.max(...capacities) : undefined,
      capacityUpSteps: capacities.length
        ? capacities.filter((v, i) => i && v > capacities[i - 1] * 1.05).length : undefined };
  });
  const observedPaths = paths.filter(p => p.observed);
  const pass = observedPaths.filter(p => p.passed);
  return {
    status: !observedPaths.length ? 'INSUFFICIENT' : pass.length ? 'PASS' : 'FAIL',
    reason: !observedPaths.length ? 'no path has both pre-wall and post-restore allocator records'
      : pass.length ? `${pass.length}/${observedPaths.length} nominated paths dipped under the wall and raised after it`
      : `no nominated path both dipped and raised inside ${windowMs}ms of the wall lifting`,
    cutRatio, windowMs, windowEndMs, paths: paths.map(({ observed, ...rest }) => ({ ...rest, observed })),
    scope: 'Per-path allocator dip-and-raise inside the recovery window. NOT a claim that the estimate '
      + 'rediscovered its pre-wall capacity: the decrease-only commit path plus the probe-regulator '
      + 'backoff (references/livekit-stable pkg/sfu/bwe/remotebwe/remote_bwe.go:205-212, '
      + 'pkg/sfu/ccutils/probe_regulator.go:80-98) bounds how fast a cleared wall can be re-earned.',
  };
}

// Per-path receiver-response certification.
//
// `evaluateBudgetTimeline` scores the aggregate `livekit_estimated_capacity_bps` gauge, which is a
// sum of one committed capacity per subscriber connection (crates/rtc-runtime/src/manager.rs). In a
// two-path leg it therefore cannot separate "the SFU ignored its receiver" from "this receiver
// never reported congestion": one path holding its seed keeps the sum inside the
// UNQUALIFIED_PRIOR_BPS_PER_CLIENT band and the aggregate claim degrades to PRIOR/INSUFFICIENT no
// matter how hard the other path reacts. Measured on the 2026-11-28 legs: the responding transport
// committed 100,000,000 -> 17,862 within 4.9s of the wall (its browser's own
// availableIncomingBitrate collapsed to the same 17,862), while the second transport's receiver
// estimate never fell below 286,406 and its committed capacity stayed at the seed across 362
// samples, so the summed gauge never left 100,000,000 + x.
//
// This evaluator makes the same claim per path on the server's own per-transport record
// (`remote feedback sample`, crates/rtc-media/src/feedback.rs), which carries the receiver estimate,
// the committed capacity and the forwarded stream SSRCs. The SSRCs are the subscriber's inbound-rtp
// SSRCs, so a record binds to a browser without any server-side identity plumbing.
//
// A path is *obligated* when its own receiver estimate collapsed (at or below `collapseRatio` of
// that path's pre-wall maximum) or when the server's channel observer reported a congested trend
// for it. An obligated path that keeps its seed is the failure this leg is about - a server that
// ignores the estimate it was handed. A path whose receiver never reported anything is disclosed as
// SEED_RETAINED rather than scored: neither engine can act on a signal its receiver never sent,
// and the pinned Go oracle is the weaker case - it emits no per-path committed value at all (only
// `remote bwe: congestion state change`, references/livekit-stable pkg/sfu/bwe/remotebwe/remote_bwe.go:229),
// so on Go logs there are no records here and the arm returns INSUFFICIENT instead of pretending.
export function evaluateSeedResponse({ records, wallAtMs, restoreAtMs, seedBps = SEED_CAPACITY_BPS, collapseRatio = 0.5 }) {
  if (!Array.isArray(records) || !records.length
    || ![wallAtMs, restoreAtMs].every(Number.isFinite) || restoreAtMs <= wallAtMs) {
    return { status: 'INSUFFICIENT', reason: 'no receiver-estimate records or impairment window',
      seedBps, collapseRatio, paths: [] };
  }
  const groups = new Map();
  for (const record of records) {
    if (!Number.isFinite(record?.atMs)) continue;
    const key = typeof record.key === 'string' ? record.key : 'unbound';
    if (!groups.has(key)) groups.set(key, { key, pathBound: record.pathBound === true, pre: [], under: [], after: [] });
    groups.get(key)[record.atMs < wallAtMs ? 'pre' : record.atMs <= restoreAtMs ? 'under' : 'after']
      .push({ atMs: record.atMs, estimateBps: record.estimateBps, capacityBps: record.capacityBps,
        congested: typeof record.observation === 'string' && record.observation.startsWith('Congesting') });
  }
  const paths = [...groups.values()].map(group => {
    const estimates = group.under.map(s => s.estimateBps).filter(Number.isFinite);
    const preEstimates = group.pre.map(s => s.estimateBps).filter(Number.isFinite);
    const capacities = group.under.map(s => s.capacityBps).filter(Number.isFinite);
    const estimateCollapsed = estimates.length > 0 && preEstimates.length > 0
      && Math.min(...estimates) <= Math.max(...preEstimates) * collapseRatio;
    const congestionReported = group.under.some(s => s.congested);
    const obligated = estimateCollapsed || congestionReported;
    const responded = capacities.length > 0 && Math.min(...capacities) < seedBps;
    return {
      key: group.key, pathBound: group.pathBound, preRecords: group.pre.length, underRecords: group.under.length,
      afterRecords: group.after.length, capacityRecords: capacities.length,
      preWallEstimateMaxBps: preEstimates.length ? Math.max(...preEstimates) : undefined,
      underWallEstimateMinBps: estimates.length ? Math.min(...estimates) : undefined,
      underWallCapacityMinBps: capacities.length ? Math.min(...capacities) : undefined,
      afterWallCapacityMaxBps: group.after.map(s => s.capacityBps).filter(Number.isFinite).length
        ? Math.max(...group.after.map(s => s.capacityBps).filter(Number.isFinite)) : undefined,
      congestedSamples: group.under.filter(s => s.congested).length,
      estimateCollapsed, congestionReported, obligated, responded,
      verdict: !group.pathBound ? 'UNBOUND' : !capacities.length || !group.under.length ? 'UNSUPPORTED'
        : obligated && !responded ? 'OBLIGATED_NOT_RESPONDED' : responded ? 'RESPONDED' : 'SEED_RETAINED',
    };
  });
  const scored = paths.filter(p => p.verdict !== 'UNBOUND' && p.verdict !== 'UNSUPPORTED');
  const ignored = scored.filter(p => p.verdict === 'OBLIGATED_NOT_RESPONDED');
  const responded = scored.filter(p => p.verdict === 'RESPONDED');
  return {
    status: ignored.length ? 'FAIL' : !responded.length ? 'INSUFFICIENT' : 'PASS',
    reason: ignored.length
      ? `${ignored.map(p => p.key).join(', ')} kept its ${seedBps}bps seed while its own receiver reported congestion`
      : !responded.length
        ? 'no bound path committed below the seed inside the wall, so the leg never exercised a capacity cut'
        : `${responded.length} bound path(s) committed below the ${seedBps}bps seed`
          + (scored.length > responded.length ? `; ${scored.length - responded.length} path(s) held it with no receiver congestion` : ''),
    seedBps, collapseRatio, wallAtMs, restoreAtMs, paths,
    scope: 'Per-subscriber-path receiver response, bound by forwarded SSRC to the browser whose '
      + 'inbound-rtp estimate produced it. NOT a per-path throughput claim: paths whose receiver never '
      + 'reported congestion are disclosed, not scored, because neither engine acts on an absent signal '
      + '(references/livekit-stable pkg/sfu/bwe/remotebwe/remote_bwe.go estimateAvailableChannelCapacity).',
  };
}

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
  const pathEvidence = derivePathEvidence(samples, expectedClients);
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
    const path = pathEvidence[sampleIndex];
    if (budget >= UNQUALIFIED_PRIOR_BPS_PER_CLIENT * expectedClients) {
      return { sampleIndex, epoch: epoch.index, phase: sample.phase, status: 'PRIOR', budgetBps: String(budget),
        demonstratedPeakBps: path.demonstratedPeakBps, droppedPackets: path.droppedPackets, metricReadStartedUs: start, metricReadCompletedUs: end,
        reason: 'aggregate budget still contains an unobserved per-path channel-capacity prior' };
    }
    // The configured rate is the ceiling only while nothing better has been
    // measured. A budget above the current rate but within the highest rate this
    // path has already delivered is a stale belief, which both implementations
    // hold until a fresh congested trend; a budget above anything delivered is
    // invented capacity. Go commits to demand-derived values under a LOSS trend
    // (remote_bwe.go estimateAvailableChannelCapacity), so the nominal rate alone
    // is not a claim the reference implementation makes.
    const aggregateRate = Math.max(epoch.bpsPerClient * expectedClients, path.demonstratedPeakBps);
    const gate = allocationHorizonInvariant({ configuredRateBps: aggregateRate, horizonUs: ACCURACY_WINDOW_US,
      burstBytes: PROXY_BURST_BYTES * expectedClients, budgetBps: budget,
      snapshotIdentityVerified: true, byteDomain: 'native-byte-subset-of-wire' });
    return { sampleIndex, epoch: epoch.index, phase: sample.phase, metricReadStartedUs: start, metricReadCompletedUs: end,
      configuredBpsPerClient: epoch.bpsPerClient, expectedClients, ceilingSource: aggregateRate > epoch.bpsPerClient * expectedClients ? 'delivered-peak' : 'configured-rate',
      demonstratedPeakBps: path.demonstratedPeakBps, droppedPackets: path.droppedPackets, ...gate };
  });
  const epochs = rates.map(r => ({ epoch: r.index, configuredBpsPerClient: r.bpsPerClient,
    stableSamples: results.filter(s => s.epoch === r.index && s.status !== 'WARMUP').length,
    qualifiedSamples: results.filter(s => s.epoch === r.index && s.status !== 'WARMUP' && s.status !== 'PRIOR').length,
    priorSamples: results.filter(s => s.epoch === r.index && s.status === 'PRIOR').length,
    failures: results.filter(s => s.epoch === r.index && s.status === 'FAIL').length,
    insufficient: results.filter(s => s.epoch === r.index && s.status === 'INSUFFICIENT').length }));
  for (const epoch of epochs) {
    const points = results.filter(s => s.epoch === epoch.epoch && s.metricReadStartedUs !== undefined);
    epoch.sampledBudgetMinBps = points.length ? Math.min(...points.map(s => Number(s.budgetBps))) : undefined;
    epoch.sampledBudgetMaxBps = points.length ? Math.max(...points.map(s => Number(s.budgetBps))) : undefined;
    epoch.maximumInterSnapshotGapUs = points.length > 1 ? Math.max(...points.slice(1).map((s, i) => s.metricReadStartedUs - points[i].metricReadCompletedUs)) : undefined;
    epoch.demonstratedPeakBps = Math.max(0, ...points.map(s => Number(s.demonstratedPeakBps ?? 0)));
    const drops = results.filter(s => s.epoch === epoch.epoch && s.droppedPackets !== undefined).map(s => s.droppedPackets);
    epoch.droppedPackets = drops.length ? drops.reduce((sum, n) => sum + n, 0) : undefined;
    // An epoch that never qualified and never lost a packet on the path gave the
    // estimator no observation to make, so the prior is the only honest result.
    epoch.priorOnly = epoch.qualifiedSamples === 0 && epoch.stableSamples > 0;
    epoch.provenUnimpacted = epoch.priorOnly ? (epoch.droppedPackets === undefined ? null : epoch.droppedPackets === 0) : undefined;
    epoch.coverage = 'Discrete budget snapshots only; unsampled intervals do not prove absence of overshoot.';
  }
  const missingEpoch = epochs.some(r => r.stableSamples === 0);
  // Missing proxy loss evidence cannot certify an unqualified epoch as
  // "nothing to observe", and a leg where no epoch ever qualified proves nothing.
  const priorOnlyUnproven = epochs.some(r => r.priorOnly && r.provenUnimpacted !== true);
  const neverQualified = epochs.every(r => r.qualifiedSamples === 0);
  return { status: results.some(s => s.status === 'FAIL') ? 'FAIL'
      : missingEpoch || priorOnlyUnproven || neverQualified || results.some(s => s.status === 'INSUFFICIENT') ? 'INSUFFICIENT' : 'PASS',
    settleUs: ACCURACY_SETTLE_US, allocationHorizonUs: ACCURACY_WINDOW_US, sameRateQualificationHistoryUs: ACCURACY_WINDOW_US, burstBytesPerClient: PROXY_BURST_BYTES,
    unqualifiedPriorBpsPerClient: UNQUALIFIED_PRIOR_BPS_PER_CLIENT,
    expectedClients, rateChanges, epochs, samples: results,
    scope: 'One-second allocation-horizon policy invariant on original aggregate budget snapshots after stable rate qualification, capped by the highest rate the nominated paths have already delivered. Budgets still inside the unobserved per-path capacity prior are filed as PRIOR and only accepted in an epoch whose paths demonstrably lost no packets. NOT an observed sustained throughput/TWCC window, full capacity discovery, continuous overshoot coverage or reaction-time test.' };
}
