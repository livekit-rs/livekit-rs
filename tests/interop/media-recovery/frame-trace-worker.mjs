// Diagnostic wrapper around the original, byte-identical SDK worker.
// Observe existing metadata reads. The SDK still owns both transforms.
import './e2ee-worker.mjs';

const seen = new WeakSet();
const getMetadata = RTCEncodedVideoFrame.prototype.getMetadata;
RTCEncodedVideoFrame.prototype.getMetadata = function () {
  const metadata = getMetadata.call(this);
  if (this.type === 'key' && !seen.has(this)) {
    seen.add(this);
    self.postMessage({
      mediaRecoveryKeyframe: {
        unixMs: performance.timeOrigin + performance.now(),
        rtpTimestamp: metadata.rtpTimestamp ?? this.timestamp,
        ssrc: metadata.synchronizationSource ?? null,
        spatialIndex: metadata.spatialIndex ?? null,
        frameId: metadata.frameId ?? null,
        pt: metadata.payloadType ?? null,
        bytes: this.data.byteLength,
      },
    });
  }
  return metadata;
};
