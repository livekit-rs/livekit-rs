// Record one browser-interoperability leg conclusion, then re-raise that leg's exit
// code so the step still shows red on the run page.
//
// Why a recorder at all: every media leg runs with continue-on-error so one red leg
// cannot abort the legs after it (a step-18 failure used to mask steps 19 onward).
// continue-on-error also keeps that failure out of the job verdict, so the final
// aggregation step has to judge something it can read. The `steps` context is not
// usable for this: a step only appears in it when it carries an explicit `id`, and a
// continue-on-error step's recorded conclusion is ambiguous. This file is.
//
//   node ci-leg.mjs <leg-id> -- <command...>
//   node ci-leg.mjs --failed <leg-id>   exits 0 only if that leg is recorded non-success
//   node ci-leg.mjs --list              prints the recorded table
//
// With exactly one argument after `--` the command runs through `sh -c`, which is how
// the workflow keeps its `TLS_SPKI="$(cat ...)" npm test` form; more arguments run as
// argv without a shell. ci-leg-summary.mjs maps these ids to the workflow step names.
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const resultsPath = resolve(process.env.CI_STEP_RESULTS_FILE || join(root, 'output', 'ci-leg-results.json'));
const TAIL_BYTES = 4000;

// RFC 1918 host addresses reach the artifact through leg stdout, so the embedded tail
// uses the same RFC 5737 TEST-NET-2 placeholder as the committed evidence, keeping the
// last octet so a reader can still tell interfaces apart.
function redact(text) {
  return String(text).replace(/\b(10|192\.168|172\.(1[6-9]|2\d|3[01]))((\.\d{1,3}){1,3})\b/g, match => {
    const octets = match.split('.');
    return `198.51.100.${octets[octets.length - 1]}`;
  });
}

function load() {
  try {
    const parsed = JSON.parse(readFileSync(resultsPath, 'utf8'));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function save(rows) {
  mkdirSync(dirname(resultsPath), { recursive: true });
  const byId = new Map();
  for (const row of rows) if (row && typeof row.id === 'string') byId.set(row.id, row);
  writeFileSync(resultsPath, JSON.stringify([...byId.values()], null, 2));
}

function record(id, fields) {
  const rows = load();
  rows.push({ id, ...fields });
  save(rows);
}

function table(rows) {
  if (!rows.length) return '(no leg recorded)';
  return rows.map(row =>
    `${String(row.id).padEnd(18)} ${(row.conclusion || 'none').padEnd(9)} ${String(row.durationMs ?? '?').padStart(7)} ms  ${row.command || ''}`).join('\n');
}

const argv = process.argv.slice(2);

if (argv[0] === '--list') {
  console.log(table(load()));
  process.exit(0);
}

if (argv[0] === '--failed') {
  const id = argv[1];
  if (!id) {
    console.error('usage: node ci-leg.mjs --failed <leg-id>');
    process.exit(2);
  }
  const row = load().find(entry => entry.id === id);
  if (!row) {
    console.log(`leg ${id} is not recorded, so it is not treated as failed`);
    process.exit(1);
  }
  const failed = row.conclusion !== 'success';
  console.log(`leg ${id} recorded conclusion=${row.conclusion} exitCode=${row.exitCode ?? 'null'} signal=${row.signal ?? 'null'}`);
  process.exit(failed ? 0 : 1);
}

const id = argv[0];
if (!id || id.startsWith('--')) {
  console.error('usage: node ci-leg.mjs <leg-id> -- <command...>');
  process.exit(2);
}
let cursor = 1;
if (argv[cursor] !== '--') {
  console.error('missing `--` before the leg command');
  process.exit(2);
}
const command = argv.slice(cursor + 1);
if (!command.length) {
  console.error('no leg command given after `--`');
  process.exit(2);
}

let tail = '';
function capture(chunk) {
  tail = (tail + chunk.toString()).slice(-TAIL_BYTES);
}
function forward(stream, out) {
  stream.on('data', chunk => {
    capture(chunk);
    out.write(chunk);
  });
}

const startedAt = new Date().toISOString();
const started = Date.now();
const child = command.length === 1
  ? spawn('sh', ['-c', command[0]], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] })
  : spawn(command[0], command.slice(1), { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
forward(child.stdout, process.stdout);
forward(child.stderr, process.stderr);

child.on('error', error => {
  const finishedAt = new Date().toISOString();
  record(id, {
    command: command.join(' '), conclusion: 'error', exitCode: null,
    signal: null, error: `${error.code || error.name}: ${error.message}`,
    durationMs: Date.now() - started, startedAt, finishedAt, outputTail: redact(tail),
  });
  console.error(`::error::leg ${id} could not start: ${error.message}`);
  process.exit(1);
});

child.on('close', (code, signal) => {
  const finishedAt = new Date().toISOString();
  const conclusion = signal ? 'signal' : code === 0 ? 'success' : 'failure';
  record(id, {
    command: command.join(' '), conclusion, exitCode: code, signal,
    durationMs: Date.now() - started, startedAt, finishedAt, outputTail: redact(tail),
  });
  if (conclusion !== 'success') {
    console.error(`leg ${id} recorded ${conclusion} (exit=${code} signal=${signal}); the job continues and the aggregation step fails on it`);
  }
  // Re-raise the leg's own status so the step conclusion on the run page stays honest
  // even though continue-on-error lets the job proceed.
  process.exit(conclusion === 'success' ? 0 : (typeof code === 'number' && code !== 0 ? code : 1));
});
