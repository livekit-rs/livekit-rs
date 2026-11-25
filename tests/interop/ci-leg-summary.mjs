// Aggregate the recorded interoperability leg conclusions of the browser job.
//
// Each media leg runs through ci-leg.mjs with continue-on-error, so a red leg neither
// aborts the legs behind it nor reddens the job by itself. This step re-applies the
// gate at the end of the job and fails on any leg that is not recorded `success`,
// which also catches a leg that never ran because the job aborted earlier. Lifecycle
// steps (server start, teardown, evidence upload, TLS removal) are deliberately not
// listed here: they are ordinary non-continue-on-error steps, so if one of them fails
// the job is already red and there is nothing for this aggregator to re-assert.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const path = resolve(process.env.CI_STEP_RESULTS_FILE || resolve(root, 'output/ci-leg-results.json'));

// Order matches the workflow. A leg that is absent from the recording is reported as
// "never recorded", which is how a masked-by-abort leg shows up.
const LEGS = [
  ['fixtures', 'Recorded fixture consistency and redaction'],
  ['netproxy', 'Network impairment proxy regression tests'],
  ['single-pc', 'Single-PC audio and reliable data, two independent runs'],
  ['reconnect', 'Reconnect, republish, mute, metadata and RPC'],
  ['vp8', 'VP8 video with extended operations, audio and reliable data'],
  ['vp9', 'VP9 L3T3_KEY spatial and temporal subscription switching'],
  ['dual-pc', 'Dual-PC audio and reliable data'],
  ['turn-tls', 'Original SDK audio and reliable data through authenticated TURN TLS'],
  ['webhook', 'HTTPS Webhook signature, certificate trust and retry through production binary'],
  ['https-ec-probe', 'EC native HTTPS CA trust, wrong hostname, untrusted CA and disabled plaintext'],
  ['https-rsa-probe', 'RSA PKCS1 native HTTPS trust and disabled plaintext'],
  ['native-ec-av1', 'Original SDK native EC WSS, HTTPS API, TURN TLS and AV1 L3T3_KEY switching'],
  ['native-rsa', 'Original SDK native RSA PKCS1 WSS, HTTPS API and TURN TLS'],
  ['congestion', 'Real TURN bottleneck and sustained media recovery'],
];

let rows;
try {
  rows = JSON.parse(readFileSync(path, 'utf8'));
} catch (error) {
  console.error(`::error::cannot read ${path}: ${error.message}. The leg recorder never ran or the job died inside it, so no leg conclusion is known.`);
  process.exit(1);
}
if (!Array.isArray(rows)) {
  console.error(`::error::${path} does not hold a leg array`);
  process.exit(1);
}

const byId = new Map(rows.filter(row => row && typeof row.id === 'string').map(row => [row.id, row]));
const lines = [];
const problems = [];
for (const [id, name] of LEGS) {
  const row = byId.get(id);
  if (!row) {
    problems.push(id);
    lines.push(`${id.padEnd(18)} never      ${name}`);
    continue;
  }
  if (row.conclusion !== 'success') problems.push(id);
  lines.push(`${id.padEnd(18)} ${String(row.conclusion).padEnd(10)} ${String(row.durationMs ?? '?').padStart(7)} ms  ${name}`);
}
// A recorded leg that is not in LEGS means the workflow and this list drifted apart,
// which would let a real leg escape the gate; report it instead of ignoring it.
const extras = [...byId.keys()].filter(id => !LEGS.some(([known]) => known === id));

console.log(`Recorded interoperability legs (${path}):\n${lines.join('\n')}`);
for (const id of extras) console.error(`::error::leg ${id} is recorded but not listed in ci-leg-summary.mjs, so it is not gated`);
for (const id of problems) {
  const row = byId.get(id);
  const detail = row ? `conclusion=${row.conclusion} exit=${row.exitCode ?? 'null'} signal=${row.signal ?? 'null'} tail=${JSON.stringify((row.outputTail || '').slice(-600))}` : 'never recorded: the job aborted before this leg ran';
  console.error(`::error::leg ${id}: ${detail}`);
}
if (problems.length || extras.length) {
  console.error(`Failing the job for ${problems.length} unsuccessful leg(s) and ${extras.length} ungated leg(s); ${LEGS.length - problems.length}/${LEGS.length} legs succeeded.`);
  process.exit(1);
}
console.log(`All ${LEGS.length} recorded interoperability legs succeeded.`);
