// Model a bounded burst of ordinary signaling delivery on the replacement
// connection. Forward the original frames in order. No SDK or ICE timers are
// changed, and neither the first connection nor later retries is buffered.
const MAX_WAIT_MS = 250;
const MAX_FRAMES = 64;

export function coalesceRejoinSignal(page, SignalRequest, evidence) {
  return page.routeWebSocket(/\/rtc(?:\/v1)?(?:\?|$)/, route => {
    const server = route.connectToServer();
    const delivery = { connection: evidence.length + 1 };
    evidence.push(delivery);
    if (delivery.connection !== 2) return;

    const queue = [];
    const descriptions = new Set();
    let timer;
    let started;
    let flushed = false;
    const flush = reason => {
      flushed = true;
      clearTimeout(timer);
      delivery.flush = reason;
      delivery.queued = queue.length;
      delivery.descriptionKinds = [...descriptions].sort();
      delivery.waitMs = Number((performance.now() - started).toFixed(3));
      for (const message of queue) server.send(message);
      queue.length = 0;
    };
    let closed = false;
    const close = (other, code, reason) => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      queue.length = 0;
      void other.close({ code, reason }).catch(() => {
        delivery.closeError = true;
      });
    };
    route.onClose((code, reason) => close(server, code, reason));
    server.onClose((code, reason) => close(route, code, reason));
    route.onMessage(message => {
      if (flushed) {
        server.send(message);
        return;
      }
      let kind;
      try {
        const request = typeof message === 'string'
          ? SignalRequest.fromJsonString(message)
          : SignalRequest.fromBinary(message);
        kind = request.message.case;
      } catch {
        // Preserve delivery on a decode error but fail the fixture's gate.
        delivery.decodeError = true;
      }
      const description = kind === 'offer' || kind === 'answer';
      if (!queue.length && !description) {
        server.send(message);
        return;
      }
      if (!queue.length) {
        started = performance.now();
        timer = setTimeout(() => flush('bounded-timeout'), MAX_WAIT_MS);
      }
      queue.push(message);
      if (description) descriptions.add(kind);
      if (descriptions.size === 2) flush('pair');
      else if (queue.length >= MAX_FRAMES) flush('bounded-frames');
    });
  });
}
