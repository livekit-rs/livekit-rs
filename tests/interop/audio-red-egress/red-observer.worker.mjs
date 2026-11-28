// Insertable-stream observer for uplink (sender) and egress (receiver) audio frames. A frame
// carries the payload exactly as the endpoint put it in the RTP packet, so an RFC 2198 chain is
// measurable here even though the TURN proxy only sees cleartext headers.
// Header layout is the one both the fixed Go encoder (pkg/sfu/redreceiver.go encodeRedForPrimary)
// and str0m's public RedDecoder use: 4-octet redundant headers (F|PT, ts(14), len(10)) followed by
// a 1-octet primary header (PT) whose payload runs to the end of the packet.
const F_BIT = 0x80;

function parseRed(bytes) {
  const blocks = [];
  let declared = 0;
  for (let i = 0;;) {
    const header = bytes[i];
    if (header === undefined) return { blocks: null, error: 'truncated-header' };
    if ((header & F_BIT) === 0) {
      const length = bytes.length - (i + 1) - declared;
      if (length <= 0) return { blocks: null, error: 'inconsistent-block-lengths' };
      return {
        blocks: [...blocks, { pt: header & 0x7f, timestampOffset: 0, length, primary: true }],
        error: null,
      };
    }
    if (i + 4 > bytes.length) return { blocks: null, error: 'truncated-header' };
    const length = ((bytes[i + 2] & 0x03) << 8) | bytes[i + 3];
    if (declared + length > bytes.length - (i + 5)) return { blocks: null, error: 'block-length-overflow' };
    blocks.push({
      pt: header & 0x7f,
      timestampOffset: (bytes[i + 1] << 6) | (bytes[i + 2] >> 2),
      length,
      primary: false,
    });
    declared += length;
    i += 4;
    if (blocks.length > 8) return { blocks: null, error: 'too-many-blocks' };
  }
}

// An Opus payload starts with the TOC octet (RFC 6716 §3.1) and frame code 3 is reserved. Reading it
// is the observer's positive control: cut the chain at the wrong offset and the frame code that
// survives is usually the reserved one.
function parseOpusToc(bytes) {
  const toc = bytes[0];
  if (toc === undefined) return null;
  return { toc, config: (toc >> 3) & 0x1f, stereo: (toc >> 2) & 1, code: toc & 0x03, valid: (toc & 0x03) !== 3 };
}

// FNV-1a over the payload: enough to prove two legs carried the same bytes without shipping them.
function fnv1a(bytes) {
  let hash = 0x811c9dc5;
  for (const byte of bytes) hash = Math.imul(hash ^ byte, 0x01000193) >>> 0;
  return hash.toString(16).padStart(8, '0');
}

// Chrome delivers each transform to the worker as an `rtctransform` event. The legacy
// `createEncodedStreams()` path is unusable here: the SDK track event fires after the receiver has
// started, and Chrome rejects it with "Too late to create encoded streams".
self.addEventListener('rtctransform', event => {
  const { readable, writable, options } = event.transformer;
  // A hook that never fires would otherwise read as a server that sends no redundancy.
  globalThis.postMessage({ hook: 'rtctransform', label: options?.label ?? null, origin: options?.origin ?? null });
  const pending = [];
  const flush = () => {
    if (pending.length) globalThis.postMessage({ rows: pending.splice(0, pending.length) });
  };
  setInterval(flush, 250);
  readable
    .pipeThrough(
      new TransformStream({
        transform(frame, controller) {
          const bytes = new Uint8Array(frame.data);
          const chain = parseRed(bytes);
          const primary = chain.blocks?.at(-1);
          const offset = primary ? bytes.byteLength - primary.length : 0;
          pending.push({
            label: options?.label ?? null,
            origin: options?.origin ?? null,
            ctor: frame.constructor?.name ?? null,
            // RTCEncodedAudioFrame in this Chromium exposes the media clock as `timestamp` and has
            // no `rtpTimestamp`, so the join key to the wire header falls back to it.
            mediaTs: frame.timestamp ?? null,
            rtpTs: frame.rtpTimestamp ?? null,
            ts: frame.rtpTimestamp ?? frame.timestamp ?? null,
            byteLength: bytes.byteLength,
            head: Array.from(bytes.slice(0, 12)),
            chainBlocks: chain.blocks,
            chainError: chain.error,
            chainPrimaryOffset: primary ? offset : null,
            opusAtZero: parseOpusToc(bytes),
            opusAtChain: primary ? parseOpusToc(bytes.subarray(offset)) : null,
            digest: fnv1a(bytes),
            primaryDigest: primary ? fnv1a(bytes.subarray(offset)) : fnv1a(bytes),
          });
          controller.enqueue(frame);
        },
      }),
    )
    .pipeTo(writable);
});
