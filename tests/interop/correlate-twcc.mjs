// Offline, allowlisted observations. Does not import or execute the browser harness.
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const hash = text => createHash('sha256').update(text).digest('hex');
const finite = value => typeof value === 'number' && Number.isFinite(value) ? value : undefined;
const durationMs = value => {
  const m = /^(?:Some\()?([\d.]+)(ns|µs|us|ms|s)\)?$/.exec(value || '');
  return m ? Number(m[1]) * ({ ns: 1e-6, 'µs': 1e-3, us: 1e-3, ms: 1, s: 1000 })[m[2]] : undefined;
};
function pick(object, fields) { return Object.fromEntries(fields.filter(k => object[k] !== undefined).map(k => [k, object[k]])); }
function nearest(items, at, timeKey) {
  let found;
  for (const item of items) if (!found || Math.abs(item[timeKey] - at) < Math.abs(found[timeKey] - at)) found = item;
  return found ? { deltaMs: found[timeKey] - at, sample: found } : undefined;
}
export function correlate(raw, serverLog, browserLog, run) {
  if (!Number.isSafeInteger(run) || run < 0) throw new Error('run must be a nonnegative integer');
  const anchors = [...browserLog.matchAll(/^OBS_NODE_TIME_ORIGIN_MS=(\d+(?:\.\d+)?)\s*$/gm)].map(m => Number(m[1]));
  const timeOriginMs = anchors.length === 1 ? anchors[0] : undefined;
  const samples = (raw.networkTimeline || []).filter(s => s.run === run && finite(s.atMonotonicMs) !== undefined);
  if (!samples.length) throw new Error('requested run has no network samples');
  const origin = Math.min(...samples.map(s => s.atMonotonicMs));
  const last = Math.max(...samples.map(s => s.atMonotonicMs));
  const trace = raw.proxyClassTrace?.runs?.find(t => t.run === run);
  const publishers = samples.flatMap(s => (s.clients || []).flatMap((client, clientOrdinal) =>
    (client.peers || []).flatMap((peer, peerOrdinal) => {
      const stats = peer.stats || [];
      const selected = new Set(stats.filter(e => e.type === 'transport').map(e => e.selectedCandidatePairId).filter(Boolean));
      // Require an actual selected pair; never take the maximum of stale candidates.
      return stats.filter(e => e.type === 'candidate-pair' && selected.has(e.id)).map(pair => ({
        atMs: s.atMonotonicMs, relativeMs: s.atMonotonicMs - origin, phase: s.phase, clientOrdinal, peerOrdinal,
        availableOutgoingBitrate: finite(pair.availableOutgoingBitrate), currentRoundTripTimeSeconds: finite(pair.currentRoundTripTime),
        statsTimestampUtcMs: finite(pair.timestamp), peerConnection: peer.connection,
      }));
    })));
  const server = [];
  const transportOrdinals = new Map();
  for (const rawLine of serverLog.split('\n')) {
    const line = rawLine.replace(/\x1b\[[0-9;]*m/g, '');
    if (!line.includes('incoming RTP TWCC metadata') && !line.includes('incoming TWCC metadata anomaly')) continue;
    const timestamp = /^(\d{4}-\d\d-\d\dT[\d:.]+Z)/.exec(line);
    const utcMs = timestamp && Date.parse(timestamp[1]);
    if (!Number.isFinite(utcMs)) continue;
    const atMs = timeOriginMs === undefined ? undefined : utcMs - timeOriginMs;
    if (atMs !== undefined && (atMs < origin - 1000 || atMs > last + 1000)) continue;
    const get = name => new RegExp(`\\b${name}: (Some\\([^)]*\\)|[^,}]+)`).exec(line)?.[1]?.trim();
    const transport = /\btransport=([^\s]+)/.exec(line)?.[1];
    if (transport !== undefined && !transportOrdinals.has(transport)) transportOrdinals.set(transport, transportOrdinals.size);
    const entry = { logUtcMs: utcMs, atMs, relativeMs: atMs === undefined ? undefined : atMs - origin,
      transportOrdinal: transportOrdinals.get(transport), kind: line.includes('metadata anomaly') ? 'anomaly' : 'periodic_snapshot' };
    for (const key of ['status_count', 'received_packets', 'missing_packets']) {
      const value = Number(get(key)); if (Number.isFinite(value)) entry[key] = value;
    }
    for (const key of ['actual_span', 'encoded_span', 'max_encoding_error', 'report_age', 'max_report_age', 'max_pending_age', 'max_receiver_gap']) {
      const value = durationMs(get(key)); if (value !== undefined) entry[`${key}Ms`] = value;
    }
    server.push(entry);
  }
  const events = (trace?.events || []).filter(e => e.event === 'flush_return_gap' || (e.controlPt === 205 && e.countOrFmt === 15)).map(e => {
    const safe = pick(e, ['event', 'atMs', 'controlPt', 'countOrFmt', 'datagramBytes', 'queueAgeMs', 'aheadPackets', 'aheadBytes', 'headKind', 'headAgeMs', 'aheadPacketsAtReceive', 'aheadBytesAtReceive', 'headKindAtReceive', 'intervalMs']);
    safe.relativeMs = e.atMs - origin;
    if (e.queuedHeads) safe.queuedHeads = e.queuedHeads.map(h => pick(h, ['queuedPackets', 'queuedBytes', 'headKind', 'headAgeMs']));
    safe.nearestPublisherSamples = [...new Set(publishers.map(p => `${p.clientOrdinal}:${p.peerOrdinal}`))].map(key => nearest(publishers.filter(p => `${p.clientOrdinal}:${p.peerOrdinal}` === key), e.atMs, 'atMs'));
    if (timeOriginMs !== undefined) safe.nearestServerSnapshot = nearest(server, e.atMs, 'atMs');
    return safe;
  });
  return {
    schema: 'offline-twcc-clock-correlation-v1', run, rawStatus: raw.status,
    clock: { status: timeOriginMs === undefined ? 'INSUFFICIENT' : 'EXPLICIT_SAME_PROCESS_TIME_ORIGIN', timeOriginMs, anchorCount: anchors.length, relativeOriginMonotonicMs: origin, lastSampleMonotonicMs: last },
    coverage: trace ? pick(trace, ['coverage', 'diagnosticErrors', 'eventOverflow', 'eventHistoryComplete', 'selfTimeMs', 'unmatchedSendCalls']) : { status: 'INSUFFICIENT_NO_PROXY_TRACE' },
    scope: [
      'Relative zero is first network sample of this run, not run start. Nearest associations carry signed deltas and do not imply causality or packet identity.',
      'Selected candidate-pair availableOutgoingBitrate is browser outbound (publisher direction); a dual-PC peer may not send media. No candidate/transport IDs are retained.',
      'Proxy PT205/FMT15 is first-clear-header inference, not authenticated TWCC decoding; compound contents are opaque. Post-send-call observation is not kernel send or browser receipt.',
      'Server UTC is log callback time, not exact TWCC emission time; periodic snapshots may repeat an old report. Durations describe the last report and cumulative maxima, not same-interval deltas.',
      'Without exactly one explicit Node timeOrigin anchor, server snapshots cannot be assigned to a run; retained only as separately timed allowlisted metadata. No guessed startedAt offset.',
      'Chrome stats are asynchronously read after network sample time; statsTimestampUtcMs is retained for auditing this sampling skew. Event ring overflow means missing history, not absence of delay.',
    ],
    publisherSamples: publishers, serverSnapshots: server, events,
  };
}
async function main() {
  const args = process.argv.slice(2), options = {};
  for (let i = 0; i < args.length; i += 2) { if (!args[i]?.startsWith('--') || args[i + 1] === undefined) throw new Error('expected --name value'); options[args[i].slice(2)] = args[i + 1]; }
  if (!options.input || !options['server-log'] || !options.output) throw new Error('required: --input raw.json --server-log server.log --output safe.json [--browser-log campaign.log] [--run 4]');
  const [rawText, serverLog, browserLog] = await Promise.all([readFile(options.input, 'utf8'), readFile(options['server-log'], 'utf8'), options['browser-log'] ? readFile(options['browser-log'], 'utf8') : '']);
  const result = correlate(JSON.parse(rawText), serverLog, browserLog, Number(options.run ?? 4));
  result.inputHashes = { rawSha256: hash(rawText), serverLogSha256: hash(serverLog), browserLogSha256: hash(browserLog) };
  await writeFile(options.output, `${JSON.stringify(result, null, 2)}\n`);
  console.log(JSON.stringify({ run: result.run, clock: result.clock.status, events: result.events.length, publisherSamples: result.publisherSamples.length, serverSnapshots: result.serverSnapshots.length }));
}
if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) await main();
