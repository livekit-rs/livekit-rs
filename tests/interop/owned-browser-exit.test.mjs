import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import { observeChildExit } from './owned-browser-exit.mjs';
class Child extends EventEmitter {
  exitCode = null;
  signalCode = null;
  exit(code, signal = null) { this.exitCode = code; this.signalCode = signal; this.emit('exit', code, signal); }
}
test('actual exit during a close phase longer than grace is not a timeout', async () => {
  const child = new Child(); const wait = observeChildExit(child);
  await new Promise(resolve => setTimeout(resolve, 15));
  child.exit(0);
  assert.deepEqual(await wait(5), { code: 0, signal: null, observedBy: 'child-exit-event' });
  assert.equal(child.listenerCount('exit'), 0);
});
test('already exited child state is observed even if event predates subscription', async () => {
  const child = new Child(); child.exit(0);
  assert.deepEqual(await observeChildExit(child)(5), { code: 0, signal: null, observedBy: 'child-state' });
});
test('real nonzero exit and termination signals are never converted to success', async () => {
  const child = new Child(); const wait = observeChildExit(child); child.exit(2);
  assert.equal((await wait(5)).code, 2);
  const signaled = new Child(); const signalWait = observeChildExit(signaled); signaled.exit(null, 'SIGKILL');
  assert.deepEqual(await signalWait(5), { code: null, signal: 'SIGKILL', observedBy: 'child-exit-event' });
});
test('a still-running child fails the original finite grace', async () => {
  const child = new Child();
  assert.deepEqual(await observeChildExit(child)(5), { timeout: true, graceMs: 5 });
  assert.equal(child.exitCode, null); assert.equal(child.listenerCount('exit'), 0);
});

test('public contexts finish before owner shutdown and client disconnection', async () => {
  const { closeOwnedBrowser } = await import('./owned-browser-exit.mjs');
  const child = new Child(); const order = []; let live = true;
  const browser = {
    contexts: () => live ? [{ close: async () => { await new Promise(r => setTimeout(r, 5)); live = false; order.push('context'); } }] : [],
    close: async () => { order.push('client'); },
  };
  const server = { process: () => child, close: async () => { assert.equal(live, false); order.push('owner'); child.exit(0); } };
  const result = await closeOwnedBrowser(browser, server);
  assert.deepEqual(order, ['context', 'owner', 'client']);
  assert.equal(result.contextsBefore, 1); assert.equal(result.contextsAfter, 0);
  assert.equal(result.exit.code, 0); assert.deepEqual(result.failures, []);
});
test('context disposal failure still closes owner and remains failure despite exit0', async () => {
  const { closeOwnedBrowser } = await import('./owned-browser-exit.mjs');
  const child = new Child(); let ownerClosed = false;
  const browser = { contexts: () => [{ close: async () => { throw Error('context failure'); } }], close: async () => {} };
  const server = { process: () => child, close: async () => { ownerClosed = true; child.exit(0); } };
  const result = await closeOwnedBrowser(browser, server);
  assert.equal(ownerClosed, true); assert.equal(result.exit.code, 0);
  assert.deepEqual(result.failures, ['owned-browser-context-close', 'owned-browser-contexts-remain']);
});
test('owned stderr observation retains a bounded tail without dropping hash coverage', async () => {
  const { observeChildStderr } = await import('./owned-browser-exit.mjs');
  const { createHash } = await import('node:crypto');
  const child = { stderr: new EventEmitter() }; const finish = observeChildStderr(child, 5);
  child.stderr.emit('data', Buffer.from('12345')); child.stderr.emit('data', Buffer.from('678'));
  const observed = finish(); assert.equal(observed.bytes.toString(), '45678');
  assert.equal(observed.metadata.totalBytes, 8); assert.equal(observed.metadata.retainedBytes, 5);
  assert.equal(observed.metadata.truncated, true);
  assert.equal(observed.metadata.sha256, createHash('sha256').update('12345678').digest('hex'));
  assert.equal(child.stderr.listenerCount('data'), 0);
});
