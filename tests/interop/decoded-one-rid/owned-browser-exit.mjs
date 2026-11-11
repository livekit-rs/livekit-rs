import { createHash } from 'node:crypto';
/** Capture real browser child termination before closing either Playwright peer.
 * The finite grace only starts after those close operations have completed.
 * No exit code is synthesized from client disconnection or a missing PID.
 */
export function observeChildExit(child) {
  if (!child) return async () => null;
  let observed;
  let resolveEvent;
  const event = new Promise(resolve => { resolveEvent = resolve; });
  const listener = (code, signal) => {
    observed = { code, signal, observedBy: 'child-exit-event' };
    resolveEvent(observed);
  };
  child.once('exit', listener);
  const actual = () => observed ?? (
    child.exitCode != null || child.signalCode != null
      ? { code: child.exitCode, signal: child.signalCode, observedBy: 'child-state' }
      : null
  );
  return async (graceMs = 5000) => {
    let timer;
    try {
      const before = actual();
      if (before) return before;
      await Promise.race([
        event,
        new Promise(resolve => { timer = setTimeout(resolve, graceMs); }),
      ]);
      // Check actual process state at the decision boundary as well: neither a
      // previously elapsed timer nor an SDK close promise establishes exit.
      return actual() ?? { timeout: true, graceMs };
    } finally {
      clearTimeout(timer);
      child.removeListener('exit', listener);
    }
  };
}

/** Release contexts through the still-connected public client before closing
 * its server. Disconnecting the client first starts asynchronous server-side
 * context disposal that is not awaited by Browser.close() over connect().
 */
export async function closeOwnedBrowser(browser, browserServer) {
  const waitForExit = observeChildExit(browserServer?.process());
  const result = { contextsBefore: browser?.contexts().length ?? 0, contextsAfter: 0, failures: [] };
  for (const context of browser?.contexts() ?? []) {
    try { await context.close(); }
    catch { result.failures.push('owned-browser-context-close'); }
  }
  result.contextsAfter = browser?.contexts().length ?? 0;
  if (result.contextsAfter !== 0) result.failures.push('owned-browser-contexts-remain');
  // The owner closes the actual child only after public context disposal has
  // acknowledged. Disconnecting the already-closed client is then local cleanup.
  try { await browserServer?.close(); }
  catch { result.failures.push('owned-browser-server-close'); }
  try { await browser?.close(); }
  catch { result.failures.push('owned-browser-client-close'); }
  result.exit = await waitForExit(5000);
  if (result.exit?.timeout || (result.exit && result.exit.code !== 0))
    result.failures.push('owned-browser-exit');
  return result;
}


/** Passive bounded tail of this exact owned child's stderr; hash covers all
 * observed bytes, even if diagnostics exceed the retained tail limit. */
export function observeChildStderr(child, limit = 2 * 1024 * 1024) {
  let totalBytes = 0; let tail = Buffer.alloc(0); const hash = createHash('sha256');
  const listener = value => {
    const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
    totalBytes += bytes.length; hash.update(bytes);
    tail = Buffer.concat([tail, bytes]).subarray(-limit);
  };
  child?.stderr?.on('data', listener);
  return () => {
    child?.stderr?.removeListener('data', listener);
    return { metadata: { totalBytes, retainedBytes: tail.length, limit,
      truncated: totalBytes > tail.length, sha256: hash.digest('hex') }, bytes: tail };
  };
}
