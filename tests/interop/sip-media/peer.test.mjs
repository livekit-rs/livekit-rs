/** Offline pure UA/parser/codec checks; no sockets or LiveKit substitutes. NOT_RUN. */
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {muEncode,muDecode,parseMessage,toneEvidence} from './peer.mjs';
test('G711 mu-law fixed reference points and bounded quantization',()=>{
 for(const [pcm,code] of [[0,0xff],[1000,0xce],[-1000,0x4e],[32767,0x80],[-32767,0x00]])assert.equal(muEncode(pcm),code);
 for(const [code,pcm] of [[0xff,0],[0x7f,0],[0xce,988],[0x4e,-988],[0x80,32124],[0, -32124]])assert.equal(muDecode(code),pcm);
 for(let pcm=-30000;pcm<=30000;pcm+=173)assert.ok(Math.abs(muDecode(muEncode(pcm))-pcm)<=Math.max(40,Math.abs(pcm)*.04));
});
test('actual decoded tone detector distinguishes reverse audio from echo',()=>{
 const make=frequency=>Array.from({length:16000},(_,i)=>muDecode(muEncode(7000*Math.sin(2*Math.PI*frequency*i/8000))));
 const reverse=toneEvidence(make(880),8000,880),echo=toneEvidence(make(440),8000,880);
 assert.ok(reverse.ready&&reverse.toneAmplitude>.2&&reverse.toneAmplitude>reverse.reference440Amplitude*5);
 assert.ok(echo.toneAmplitude<.005&&echo.reference440Amplitude>.2);
 assert.equal(toneEvidence([1,2,3],8000,880).ready,false);
});
test('SIP parser preserves repeated Via and enforces exact octet content length',()=>{
 const raw='INVITE sip:peer@127.0.0.1:15060 SIP/2.0\r\nVia: one\r\nVia: two\r\nContent-Length: 2\r\n\r\né';
 const parsed=parseMessage(Buffer.from(raw));assert.equal(parsed.get('via'),'one');assert.deepEqual(parsed.headers.get('via'),['one','two']);assert.equal(parsed.body,'é');
 assert.throws(()=>parseMessage(Buffer.from(raw.replace('Length: 2','Length: 1'))),/Content-Length/);
 assert.throws(()=>parseMessage(Buffer.from('INVITE nope')),/delimiter/);
});
