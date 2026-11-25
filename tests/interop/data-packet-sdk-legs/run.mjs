/** C86-A campaign runner: launched Rust main + official livekit-client SDK.
 *  Responsibilities kept thin and honest:
 *   1. resolve the server binary, record its sha256 BEFORE any leg runs;
 *   2. launch a dedicated probe-config server instance (own PID, own log);
 *   3. wait for readiness on the HTTP port;
 *   4. write a RUNNING receipt first, then run probe.mjs against it;
 *   5. merge binary/config/SDK/browser identities into the final receipt.
 *  Usage: node tests/interop/data-packet-sdk-legs/run.mjs
 *  Env:   LIVECLOUD_BIN (default target/debug/livecloud), PROBE_OUT
 *         (default <dir>/output/receipt.json), SERVER_LOG, SKIP_SERVER=1 to
 *         attach to an already-launched instance (identity still recorded).
 *  A stale listener on the HTTP port makes the readiness poll succeed against a
 *  server this run does not own, so the launched path first requires the port to
 *  be free instead of trusting the poll. */
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import path from 'node:path';

const dir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(dir, '..', '..', '..');
const bin = process.env.LIVECLOUD_BIN || path.join(repoRoot, 'target', 'debug', 'livecloud');
const config = path.join(dir, 'probe-config.yaml');
const out = process.env.PROBE_OUT || path.join(dir, 'output', 'receipt.json');
const httpUrl = process.env.LIVEKIT_HTTP || 'http://127.0.0.1:8891';
const logPath = process.env.SERVER_LOG || path.join(dir, 'output', 'server.log');

const sha256 = async f => createHash('sha256').update(await readFile(f)).digest('hex');
const sleep = ms => new Promise(r => setTimeout(r, ms));

if (!existsSync(bin)) throw new Error('server binary not found: ' + bin + ' (build it or set LIVECLOUD_BIN)');
if (!existsSync(config)) throw new Error('config not found: ' + config);
await mkdir(path.dirname(out), { recursive: true });
await mkdir(path.dirname(logPath), { recursive: true });

const identity = {
  binary: bin,
  binary_sha256: await sha256(bin),
  config,
  config_sha256: await sha256(config),
  sdk_package: (JSON.parse(await readFile(path.join(repoRoot, 'tests', 'interop', 'package.json'), 'utf8'))).dependencies,
};

let server = null;
const listening = port => new Promise(res => {
  const s = net.connect({ host: '127.0.0.1', port });
  s.once('connect', () => { s.destroy(); res(true); });
  s.once('error', () => res(false));
});
// A previous instance can still hold the RTC UDP port when the next run starts.
// The server then logs "bind: address already in use", keeps serving signaling
// and silently loses media-channel frames, so teardown waits for the process to
// exit and the HTTP port to close, and the next start refuses to launch until
// the port is free again.
const waitForFree = async (port, ms) => {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (!(await listening(port))) return true;
    await sleep(250);
  }
  return !(await listening(port));
};
const cleanup = async () => {
  if (server && server.exitCode === null) {
    server.kill('SIGTERM');
    const gone = await new Promise(res => {
      server.once('exit', () => res(true));
      setTimeout(() => res(false), 1500);
    });
    if (!gone) {
      server.kill('SIGKILL');
      await new Promise(res => {
        server.once('exit', () => res(true));
        setTimeout(() => res(false), 5000);
      });
    }
  }
  await waitForFree(Number(new URL(httpUrl).port), 20000);
  await sleep(1000);
};
try {
  if (process.env.SKIP_SERVER !== '1') {
    const port = Number(new URL(httpUrl).port);
    if (!(await waitForFree(port, 10000))) {
      throw new Error(`127.0.0.1:${port} still accepts connections, so this run cannot own the server it measures.`
        + ' Stop that listener or point LIVEKIT_HTTP at a free port with a matching probe-config.yaml.');
    }
    const cfgRel = path.relative(repoRoot, config);
    const { open } = await import('node:fs/promises');
    await writeFile(logPath, '');
    const lf = await open(logPath, 'a');
    server = spawn(bin, ['--dev', '--config', cfgRel], { cwd: repoRoot, stdio: ['ignore', lf.fd, lf.fd] });
    identity.server_pid = server.pid;
  }
  // Readiness: poll the HTTP port until it answers (any HTTP status counts).
  const deadline = Date.now() + 30000;
  let ready = false;
  while (Date.now() < deadline) {
    try { const r = await fetch(httpUrl + '/'); ready = true; break; } catch { await sleep(300); }
  }
  if (!ready) throw new Error('server not ready on ' + httpUrl);
  const early = await readFile(logPath, 'utf8');
  if (early.includes('address already in use')) {
    throw new Error('the launched server could not bind its ports (see ' + logPath + '), so any result would be a half-dead server');
  }

  // RUNNING receipt written BEFORE results, per campaign rules.
  await writeFile(out, JSON.stringify({ status: 'RUNNING', started_at: new Date().toISOString(), identity }, null, 2) + '\n');

  const { runProbe } = await import('./probe.mjs');
  const report = await runProbe({ url: process.env.LIVEKIT_URL || httpUrl.replace(/^http/, 'ws'), httpUrl, roomName: 'c86a-' + Date.now(), out });
  report.out_path = out;
  report.identity = identity;
  report.finished_at = new Date().toISOString();
  await writeFile(out, JSON.stringify(report, null, 2) + '\n');
  console.log(report.status, out);
  // process.exit() inside the try would skip the teardown and leave the server
  // holding 8891/8892 for the next run, so the status is returned as an exit code.
  process.exitCode = report.status === 'PROBE_DONE' ? 0 : 1;
} finally {
  await cleanup();
}
