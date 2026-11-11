// Run inside the coordinator-owned Linux guest. No server/worker/build is started.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { resolve, dirname, basename, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { readFile, writeFile, mkdir, readdir, lstat, rm, rmdir } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID, createHash } from 'node:crypto';
const env = process.env;
for (const key of ['INTEROP_PROJECT', 'AGENT_SDK_ROOT', 'LIVEKIT_URL', 'EGRESS_WORKER_PID', 'EGRESS_AUTO_DIR', 'EGRESS_MEDIA_REPORT']) assert(env[key], `${key} required`);
assert.equal(process.platform, 'linux');
assert.equal(process.arch, 'arm64');
const require = createRequire(resolve(env.INTEROP_PROJECT, 'package.json'));
const { AccessToken, RoomServiceClient, EgressClient, RoomEgress, AutoTrackEgress } = require('livekit-server-sdk');
const sdkVersion = JSON.parse(await readFile(resolve(env.INTEROP_PROJECT, 'node_modules/livekit-server-sdk/package.json'), 'utf8')).version;
assert.equal(sdkVersion, '2.19.2');
const rtc = await import(pathToFileURL(resolve(env.AGENT_SDK_ROOT, 'node_modules/@livekit/rtc-node/dist/index.js')));
const exec = promisify(execFile);
const sha = value => createHash('sha256').update(value).digest('hex');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const key = env.LIVEKIT_API_KEY || 'devkey', secret = env.LIVEKIT_API_SECRET || 'secret';
const rooms = new RoomServiceClient(env.LIVEKIT_URL, key, secret);
const egress = new EgressClient(env.LIVEKIT_URL, key, secret);
const roomName = `auto-track-${randomUUID()}`;
const room = new rtc.Room();
let ownedDirectory = false, roomCreated = false, source, track, producer, producerError;
let stop = false, sample = 0, frames = 0, id, child, completed = false;
const report = {
  status: 'RUNNING',
  scope: 'original external Egress automatic per-track policy: CreateRoom -> real PCM/RTP -> actual file/whole decoded content; no StartEgress API call',
  input: { runner_sha256: sha(await readFile(new URL(import.meta.url))), server_sdk: sdkVersion, architecture: process.arch },
  observations: {}, cleanup_errors: [],
};
// A pre-existing receipt is never overwritten. An interrupted run remains RUNNING.
await writeFile(env.EGRESS_MEDIA_REPORT, JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
async function wait(check, label, ms = 45000) {
  const until = Date.now() + ms;
  while (Date.now() < until) { const value = await check(); if (value) return value; await sleep(100); }
  throw Error(`${label} deadline`);
}
async function children() {
  const { stdout } = await exec('/bin/ps', ['-axo', 'pid=,ppid=,comm='], { maxBuffer: 1024 * 1024 });
  return stdout.split('\n').map(value => value.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean)
    .filter(value => +value[2] === +env.EGRESS_WORKER_PID && value[3].split('/').at(-1) === 'egress').map(value => +value[1]);
}
try {
  const directory = resolve(env.EGRESS_AUTO_DIR);
  assert(directory.startsWith('/home/fixture/fixture/output/') && dirname(directory) === '/home/fixture/fixture/output', 'new direct child of owned output root required');
  assert(basename(directory).startsWith('auto-') && !directory.includes('{'), 'task-specific automatic output directory required');
  await mkdir(directory, { mode: 0o700 }); // EEXIST fails: never mix with an earlier recording.
  ownedDirectory = true;
  assert.equal((await children()).length, 0, 'exclusive owned external worker required');
  await rooms.createRoom({ name: roomName, egress: new RoomEgress({ tracks: new AutoTrackEgress({ filepath: join(directory, '{track_id}.ogg') }) }) });
  roomCreated = true;
  const token = new AccessToken(key, secret, { identity: 'automatic-real-publisher' });
  token.addGrant({ roomJoin: true, room: roomName, canPublish: true });
  await room.connect(env.LIVEKIT_URL.replace(/^http/, 'ws'), await token.toJwt());
  source = new rtc.AudioSource(48000, 1);
  track = rtc.LocalAudioTrack.createAudioTrack('real automatic sine', source);
  const published = await room.localParticipant.publishTrack(track, new rtc.TrackPublishOptions({ source: rtc.TrackSource.SOURCE_MICROPHONE, dtx: false, red: false }));
  assert(published.sid);
  producer = (async () => {
    while (!stop) {
      const pcm = new Int16Array(960);
      for (let i = 0; i < pcm.length; i++) pcm[i] = Math.round(10000 * Math.sin(2 * Math.PI * 440 * sample++ / 48000));
      await source.captureFrame(new rtc.AudioFrame(pcm, 48000, 1, 960)); frames++;
    }
  })();
  producer.catch(error => { producerError = error; });
  const outbound = await wait(async () => {
    if (producerError) throw producerError;
    let packets = 0n, bytes = 0n;
    for (const value of (await room.getRtcStats()).publisherStats) if (value.stats.case === 'outboundRtp') {
      packets += value.stats.value.sent?.packetsSent ?? 0n; bytes += value.stats.value.sent?.bytesSent ?? 0n;
    }
    return packets > 0n && bytes > 0n ? { packets: Number(packets), bytes: Number(bytes) } : null;
  }, 'actual publisher outbound RTP');
  // The server policy alone starts this job. Never call an explicit Start API.
  const started = await wait(async () => {
    const jobs = await egress.listEgress({ roomName });
    assert(jobs.length <= 1, 'one automatic job for one actually published track');
    if (!jobs.length) return null;
    const value = jobs[0];
    if ([4, 5, 6].includes(value.status)) throw Error('automatic original worker failed');
    return value;
  }, 'actual automatically started Egress');
  id = started.egressId; assert.match(id, /^EG_/);
  assert.equal(started.roomName, roomName);
  assert.equal(started.request.case, 'track');
  assert.equal(started.request.value.trackId, published.sid);
  const filename = join(directory, `${published.sid}.ogg`);
  child = await wait(async () => { const ids = await children(); assert(ids.length <= 1); return ids[0]; }, 'actual original run-handler child');
  await wait(async () => {
    const jobs = await egress.listEgress({ roomName }); assert.equal(jobs.length, 1);
    const value = jobs[0]; assert.equal(value.egressId, id);
    if ([4, 5, 6].includes(value.status)) throw Error('automatic Egress failed before ACTIVE');
    return value.status === 1 ? value : null;
  }, 'actual automatic ACTIVE IOInfo callback');
  await sleep(8000); if (producerError) throw producerError;
  await egress.stopEgress(id);
  const terminal = await wait(async () => {
    const jobs = await egress.listEgress({ roomName }); assert.equal(jobs.length, 1);
    const value = jobs[0]; assert.equal(value.egressId, id);
    if ([4, 5, 6].includes(value.status)) throw Error('automatic real worker terminal error');
    return value.status === 3 ? value : null;
  }, 'actual automatic COMPLETE callback');
  completed = true;
  assert.equal(terminal.error, ''); assert.equal(terminal.errorCode, 0);
  assert(BigInt(terminal.endedAt) > BigInt(terminal.startedAt));
  assert.equal(terminal.fileResults.length, 1);
  const result = terminal.fileResults[0];
  assert.equal(result.filename, filename); assert(BigInt(result.size) > 0n); assert(BigInt(result.duration) > 0n);
  await wait(async () => !(await children()).includes(child), 'actual automatic handler exit');
  const file = await readFile(filename); assert.equal(BigInt(file.length), BigInt(result.size)); assert(file.length <= 16 * 1024 * 1024);
  const { stdout } = await exec('ffprobe', ['-v', 'error', '-show_entries', 'stream=codec_name,sample_rate,codec_type:format=format_name,duration', '-of', 'json', filename], { maxBuffer: 65536, timeout: 15000 });
  const probe = JSON.parse(stdout);
  assert.equal(probe.streams.length, 1); assert.equal(probe.streams[0].codec_type, 'audio'); assert.equal(probe.streams[0].codec_name, 'opus');
  assert.equal(probe.streams[0].sample_rate, '48000'); assert.equal(probe.format.format_name, 'ogg'); assert(Number(probe.format.duration) >= 5);
  const decoded = await exec('ffmpeg', ['-nostdin', '-v', 'error', '-i', filename, '-f', 's16le', '-ac', '1', '-ar', '48000', 'pipe:1'], { encoding: 'buffer', maxBuffer: 16 * 1024 * 1024, timeout: 15000 });
  const pcm = decoded.stdout; assert.equal(pcm.length % 2, 0); const count = pcm.length / 2; assert(count >= 240000);
  let square = 0, sine = 0, cosine = 0;
  for (let i = 0; i < count; i++) { const value = pcm.readInt16LE(i * 2); square += value * value; sine += value * Math.sin(2 * Math.PI * 440 * i / 48000); cosine += value * Math.cos(2 * Math.PI * 440 * i / 48000); }
  const rms = Math.sqrt(square / count), amplitude = 2 * Math.hypot(sine, cosine) / count, ratio = amplitude * amplitude / (2 * rms * rms);
  assert(rms > 100); assert(ratio > 0.5);
  const artifacts = [];
  for (const name of await readdir(directory)) { const path = join(directory, name); assert((await lstat(path)).isFile()); const bytes = await readFile(path); assert(bytes.length <= 16 * 1024 * 1024); artifacts.push({ filename_sha256: sha(name), bytes: bytes.length, sha256: sha(bytes) }); }
  report.observations = { no_explicit_start_api: true, one_job_for_real_track: true, actual_handler: true, publisher_outbound_rtp: outbound, submitted_pcm_frames: frames, active_complete: true, handler_exited: true, file_size: file.length, file_sha256: sha(file), codec: 'opus', format: 'ogg', duration_seconds: Number(probe.format.duration), decoded_samples: count, rms, tone_power_ratio: ratio, artifacts };
  report.status = 'PASS';
} catch (error) {
  report.status = 'FAIL'; report.failure = { name: error.name, code: error.code ?? null, message_sha256: sha(String(error.message)) };
} finally {
  if (id && !completed) try { await egress.stopEgress(id); } catch { report.cleanup_errors.push('owned automatic Egress stop failed'); }
  if (child && !completed) try { await wait(async () => !(await children()).includes(child), 'failed automatic handler cleanup exit'); } catch { report.cleanup_errors.push('owned automatic handler did not exit'); }
  stop = true;
  try { if (producer) await producer; } catch { report.cleanup_errors.push('PCM producer exit failed'); }
  try { await room.disconnect(); } catch { report.cleanup_errors.push('RTC disconnect failed'); }
  try { if (track) await track.close(false); if (source) await source.close(); } catch { report.cleanup_errors.push('RTC audio handles failed'); }
  try { await rtc.dispose(); } catch { report.cleanup_errors.push('RTC runtime dispose failed'); }
  if (roomCreated) try { await rooms.deleteRoom(roomName); } catch { report.cleanup_errors.push('owned room delete failed'); }
  // Preserve failed recording evidence for coordinator inspection. Successful
  // cleanup is confined to the fresh directory this runner actually created.
  if (ownedDirectory && report.status === 'PASS') try {
    for (const name of await readdir(env.EGRESS_AUTO_DIR)) { const path = join(env.EGRESS_AUTO_DIR, name); assert((await lstat(path)).isFile()); await rm(path); }
    await rmdir(env.EGRESS_AUTO_DIR);
  } catch { report.cleanup_errors.push('owned recording cleanup failed'); }
  if (report.cleanup_errors.length) report.status = 'FAIL';
  await writeFile(env.EGRESS_MEDIA_REPORT, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
  if (report.status !== 'PASS') process.exitCode = 1;
}
