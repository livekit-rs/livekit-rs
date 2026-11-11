// Independent, test-only measurement oracles. No SDK, server, proxy or network
// settings are changed. Integer microseconds and rational arithmetic avoid a
// percentage chosen after observing a failure.
const SCALE = 8000000n; // wire bytes * SCALE == bitrate * microseconds
const integer = (value, name) => {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) throw new Error(`${name}: safe integer required`);
  const n = BigInt(value);
  if (n < 0n) throw new Error(`${name}: negative value`);
  return n;
};
const minimum = (a, b) => a < b ? a : b;
const fraction = (numerator, denominator) => ({ numerator: numerator.toString(), denominator: denominator.toString(), value: Number(numerator) / Number(denominator) });

export function integratedRate(rateChanges, startUs, endUs) {
  const start = integer(startUs, 'startUs'), end = integer(endUs, 'endUs');
  if (end < start || !rateChanges.length) throw new Error('invalid rate window');
  const rates = rateChanges.map(r => ({ at: integer(r.atUs, 'rate time'), bps: integer(r.bps, 'rate') }));
  if (rates[0].at > start) throw new Error('rate history does not cover window start');
  for (let i = 1; i < rates.length; i++) if (rates[i].at <= rates[i - 1].at) throw new Error('rate times must increase');
  let rate = rates[0].bps, at = start, bitsUs = 0n;
  for (const next of rates.slice(1)) {
    if (next.at <= start) { rate = next.bps; continue; }
    if (next.at >= end) break;
    bitsUs += rate * (next.at - at); at = next.at; rate = next.bps;
  }
  return bitsUs + rate * (end - at);
}

export function wireEnvelope({ rateChanges, startUs, endUs, burstBytes, wireBytes }) {
  const integral = integratedRate(rateChanges, startUs, endUs);
  const maximum = integer(burstBytes, 'burstBytes') * SCALE + integral;
  const observed = integer(wireBytes, 'wireBytes') * SCALE;
  return { status: observed <= maximum ? 'PASS' : 'FAIL', observedWireBytes: String(wireBytes),
    allowedWireBytes: fraction(maximum, SCALE), excessScaledBytes: (observed > maximum ? observed - maximum : 0n).toString() };
}

export function auditTokenLedger({ initialBps, startUs = 0, burstBytes, events }) {
  const cap = integer(burstBytes, 'burstBytes') * SCALE;
  let tokens = cap, bps = integer(initialBps, 'initialBps'), at = integer(startUs, 'startUs');
  const violations = [];
  for (const [index, event] of events.entries()) {
    const now = integer(event.atUs, 'event time');
    if (now < at) throw new Error('ledger must preserve monotonic event order');
    tokens = minimum(cap, tokens + bps * (now - at)); at = now;
    if (event.type === 'rate') bps = integer(event.bps, 'new rate');
    else if (event.type === 'service') {
      const cost = integer(event.wireBytes, 'wireBytes') * SCALE;
      if (cost > tokens) violations.push({ index, atUs: event.atUs, reason: 'service exceeded available tokens', shortfallScaledBytes: (cost - tokens).toString() });
      // Continue without inventing negative tokens or credit after a violation.
      tokens = cost <= tokens ? tokens - cost : 0n;
    } else throw new Error('unknown ledger event');
  }
  return { status: violations.length ? 'FAIL' : 'PASS', events: events.length, violations,
    finalTokensBytes: fraction(tokens, SCALE) };
}

// This gate is an upper-bound test, not proof of complete path discovery. An
// ALR sender cannot reveal unused capacity. Inputs must identify the actual
// observation window; a short cluster cannot be relabelled a sustained window.
export function sustainedBudgetGate({ rateChanges, startUs, endUs, burstBytes, budgetBps,
  minimumWindowUs, completeObservationWindow, byteDomain, timingErrorUs = 0 }) {
  const start = integer(startUs, 'startUs'), end = integer(endUs, 'endUs');
  if (end <= start) throw new Error('positive window required');
  const span = end - start, uncertainty = integer(timingErrorUs, 'timingErrorUs');
  if (!completeObservationWindow || minimumWindowUs === undefined || span < integer(minimumWindowUs, 'minimumWindowUs')) {
    return { status: 'INSUFFICIENT', reason: 'complete qualifying observation window required' };
  }
  if (!['wire', 'native-byte-subset-of-wire'].includes(byteDomain)) {
    return { status: 'INSUFFICIENT', reason: 'wire-compatible byte domain must be demonstrated' };
  }
  if (2n * uncertainty >= span || start < uncertainty) return { status: 'INSUFFICIENT', reason: 'time uncertainty dominates window' };
  // Declared uncertainty is a per-endpoint error bound, not an arbitrary
  // percentage. The rate history must cover the expanded endpoints.
  const integral = integratedRate(rateChanges, start - uncertainty, end + uncertainty);
  const maximum = integer(burstBytes, 'burstBytes') * SCALE + integral;
  const denominator = span - 2n * uncertainty;
  const budget = integer(budgetBps, 'budgetBps');
  return { status: budget * denominator <= maximum ? 'PASS' : 'FAIL', budgetBps: String(budgetBps),
    spanUs: span.toString(), timingErrorUs: uncertainty.toString(), maximumBudgetBps: fraction(maximum, denominator),
    interpretation: byteDomain === 'native-byte-subset-of-wire' ? 'Conservative wire upper bound; protocol overhead was not invented as a percentage.' : 'Wire-domain upper bound.' };
}

export function wireConservation({ offeredWireBytes, servedWireBytes, droppedWireBytes,
  queueStartWireBytes, queueEndWireBytes, sendFailedWireBytes = 0, resetDiscardedWireBytes = 0 }) {
  const input = integer(offeredWireBytes, 'offered') + integer(queueStartWireBytes, 'queueStart');
  const output = integer(servedWireBytes, 'served') + integer(droppedWireBytes, 'dropped')
    + integer(queueEndWireBytes, 'queueEnd') + integer(sendFailedWireBytes, 'sendFailed') + integer(resetDiscardedWireBytes, 'resetDiscarded');
  return { status: input === output ? 'PASS' : 'FAIL', unaccountedWireBytes: (input - output).toString() };
}

// Replays the native probe formula from already deduplicated received packets.
// It checks formula identity; it deliberately does NOT certify sustained capacity.
export function replayProbe({ totalBytes, excludedLastSendBytes, excludedFirstRecvBytes,
  sendSpanUs, recvSpanUs, receivedPackets }) {
  const sendSpan = integer(sendSpanUs, 'sendSpanUs'), recvSpan = integer(recvSpanUs, 'recvSpanUs');
  const total = integer(totalBytes, 'totalBytes'), last = integer(excludedLastSendBytes, 'lastSend'), first = integer(excludedFirstRecvBytes, 'firstRecv');
  if (receivedPackets < 4 || !sendSpan || !recvSpan || sendSpan > 1000000n || recvSpan > 1000000n || total <= last || total <= first) {
    return { status: 'INVALID_SAMPLE' };
  }
  const sent = (total - last) * SCALE, received = (total - first) * SCALE;
  if (received * sendSpan > 2n * sent * recvSpan) return { status: 'INVALID_SAMPLE', reason: 'receive/send ratio exceeds native limit' };
  let numerator, denominator;
  if (10n * received * sendSpan < 9n * sent * recvSpan) { numerator = received * 19n; denominator = recvSpan * 20n; }
  else if (sent * recvSpan < received * sendSpan) { numerator = sent; denominator = sendSpan; }
  else { numerator = received; denominator = recvSpan; }
  return { status: 'REPLAYED', nativeEstimateBps: fraction(numerator, denominator),
    classification: 'short-packet-train-result; sustained budget requires a separate qualifying window' };
}

// A policy safety invariant, not an observed throughput measurement. With the
// currently configured rate hypothetically held for the declared future horizon,
// the sampled allocation may not authorize more than its token service envelope.
export function allocationHorizonInvariant({ configuredRateBps, horizonUs, burstBytes, budgetBps, snapshotIdentityVerified, byteDomain }) {
  if (!snapshotIdentityVerified || !['wire', 'native-byte-subset-of-wire'].includes(byteDomain)) return { status: 'INSUFFICIENT', reason: 'verified original gauge/path/byte identity required' };
  const horizon = integer(horizonUs, 'horizonUs');
  if (!horizon) throw new Error('positive allocation horizon required');
  const maximum = integer(configuredRateBps, 'configuredRateBps') * horizon + integer(burstBytes, 'burstBytes') * SCALE;
  const budget = integer(budgetBps, 'budgetBps');
  return { status: budget * horizon <= maximum ? 'PASS' : 'FAIL', budgetBps: String(budgetBps),
    allocationHorizonUs: horizon.toString(), maximumBudgetBps: fraction(maximum, horizon),
    interpretation: 'Hypothetical authorized service over the declared horizon at the current configured rate; NOT an observed throughput/TWCC window.' };
}
