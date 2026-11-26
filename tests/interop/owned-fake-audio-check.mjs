#!/usr/bin/env node
// Isolate whether the hang is the renderer, the media stack, or the audio device:
// resolve a plain promise, then enumerateDevices, then audio vs video fake capture.
import { chromium } from 'playwright';
import http from 'node:http';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
  res.end('<body>x</body>');
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const origin = `http://127.0.0.1:${server.address().port}`;
const cap = (p, ms, label) => Promise.race([p, new Promise((_, rej) => setTimeout(
  () => rej(new Error('timeout ' + label)), ms))]);

const browser = await chromium.launch({
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream'],
});
const context = await browser.newContext();
await context.grantPermissions(['microphone', 'camera']);
const page = await context.newPage();
await page.goto(origin + '/', { waitUntil: 'load' });
const run = async (label, fn, ms) => {
  try {
    console.log(`${label}: ${await cap(page.evaluate(fn), ms, label)}`);
  } catch (e) {
    console.log(`${label}: HANG/ERR ${String(e.message).slice(0, 70)}`);
  }
};
await run('js-loop', async () => String(await Promise.resolve('alive')), 5000);
await run('flags', () => JSON.stringify(navigator.mediaDevices.getSupportedConstraints
  ? Object.entries(navigator.mediaDevices.getSupportedConstraints())
    .filter(([, v]) => v).map(([k]) => k) : 'none'), 5000);
await run('enumerate', async () => (await navigator.mediaDevices.enumerateDevices())
  .map((d) => d.kind).join(','), 12000);
await run('gum-video', async () => {
  const s = await navigator.mediaDevices.getUserMedia({ video: true });
  const t = s.getVideoTracks()[0];
  return 'ok:' + (t ? t.label + ':' + t.readyState : 'none');
}, 15000);
await run('gum-audio', async () => {
  const s = await navigator.mediaDevices.getUserMedia({ audio: true });
  const t = s.getAudioTracks()[0];
  return 'ok:' + (t ? t.label + ':' + t.readyState : 'none');
}, 15000);
await run('audiocontext', async () => {
  const ac = new AudioContext();
  await ac.resume();
  return ac.state + '/' + ac.sampleRate;
}, 15000);
await run('pc-construct', async () => {
  const pc = new RTCPeerConnection({});
  const offer = await pc.createOffer({ offerToReceiveAudio: true });
  return 'ok:' + offer.type;
}, 15000);
server.close();
await Promise.race([browser.close(), new Promise((r) => setTimeout(r, 5000))]);
