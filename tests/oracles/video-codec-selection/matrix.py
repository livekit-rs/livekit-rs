#!/usr/bin/env python3
"""C62 literal inputs and independent expected ACKs; never reads observer output."""
import copy
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
CASE_COUNT = 48


def layer(q, tag):
    return dict(quality=q, width=100 + tag, height=200 + tag, bitrate=100000 + tag,
                ssrc=3000 + tag, spatial_layer=tag % 3, rid='rid-' + str(tag), repair_ssrc=4000 + tag)


C0, C2 = layer(0, 10), layer(2, 12)
V0, V2 = layer(0, 20), layer(2, 22)
H0, H2 = layer(0, 30), layer(2, 32)
U0, U2 = layer(0, 40), layer(2, 42)
COMMON, SORTED_COMMON = [C2, C0], [C0, C2]
V, SV, H, SH, U, SU = [V2, V0], [V0, V2], [H2, H0], [H0, H2], [U2, U0], [U0, U2]


def enabled(*names):
    return [dict(mime=n, fmtp_line='') for n in names]


def codec(name, cid='v', layers=V, mode=0):
    return dict(codec=name, cid=cid, layers=layers, video_layer_mode=mode)


def ack(mime, cid='v', layers=SV, mode=1):
    return dict(mime=mime, cid=cid, mid='', sdp_cid='', layer_mode=mode, layers=layers)


VP8 = enabled('video/vp8')
OPUS = enabled('audio/opus')
# name, enabled, browser, track type, request codecs, expected codecs, simulcast.
# All expected MIME/layer/mode selections below are literals, not selection logic.
VECTORS = [
    ('full-video-prefix', VP8, '', 1, [codec('video/vp8')], [ack('video/vp8', layers=SORTED_COMMON)], True),
    ('full-video-mixed-suffix', VP8, '', 1, [codec('video/VP8')], [ack('video/VP8', layers=SORTED_COMMON)], True),
    ('wrong-video-prefix', enabled('video/h264'), '', 1, [codec('audio/vp8')], [ack('video/h264', layers=SORTED_COMMON)], True),
    ('case-video-prefix', enabled('video/h264'), '', 1, [codec('Video/vp8')], [ack('video/h264', layers=SORTED_COMMON)], True),
    ('bare-video-mixed-case', VP8, '', 1, [codec('vP8')], [ack('video/vP8')], True),
    ('full-audio-prefix', OPUS, '', 0, [codec('audio/opus')], [ack('audio/opus', layers=SORTED_COMMON, mode=0)], True),
    ('wrong-audio-prefix', enabled('audio/red'), '', 0, [codec('video/opus')], [ack('audio/red', layers=SORTED_COMMON, mode=0)], True),
    ('case-audio-prefix', enabled('audio/red'), '', 0, [codec('Audio/opus')], [ack('audio/red', layers=SORTED_COMMON, mode=0)], True),
    ('bare-audio-mixed-case', OPUS, '', 0, [codec('OpUs')], [ack('audio/OpUs', mode=0)], True),
    ('opus-long-s', OPUS, '', 0, [codec('opuſ')], [ack('audio/opuſ', mode=0)], True),
    ('empty-unsupported', VP8, '', 1, [codec('')], [ack('video/vp8', layers=SORTED_COMMON)], True),
    ('whitespace-unsupported', VP8, '', 1, [codec(' ')], [ack('video/vp8', layers=SORTED_COMMON)], True),
    ('case-distinct-exact-dedup', VP8, '', 1, [codec('vp8'), codec('VP8', 'h', H, 99)], [ack('video/vp8'), ack('video/VP8', 'h', SV, 99)], True),
    ('identical-fallback-dedup', VP8, '', 1, [codec('av1', 'u', U, -1), codec('h265', 'h', H, 99)], [ack('video/vp8', 'u', SORTED_COMMON, -1)], True),
    ('different-unknown-mimes-equal', enabled('video/quux'), '', 1, [codec('foo', 'u', U), codec('bar', 'h', H)], [ack('video/foo', 'u', SU), ack('video/bar', 'h', SU)], True),
    ('unknown-output-none-first-match', enabled('video/mystery'), '', 1, [codec('av1', 'u', U), codec('video/other', 'h', H)], [ack('video/mystery', 'u', SH), ack('video/other', 'h', SH)], True),
    ('empty-enabled-video-default', [], '', 1, [codec('h264')], [ack('video/VP8', layers=SORTED_COMMON)], True),
    ('empty-enabled-audio-default', [], '', 0, [codec('red')], [ack('audio/opus', layers=SORTED_COMMON, mode=0)], True),
    ('vp8-priority-reversed-input', enabled('video/av1', 'video/h264', 'video/vp8'), '', 1, [codec('h265')], [ack('video/vp8', layers=SORTED_COMMON)], True),
    ('h264-priority-before-others', enabled('video/av1', 'video/h265', 'video/h264'), '', 1, [codec('vp8')], [ack('video/h264', layers=SORTED_COMMON)], True),
    ('other-codecs-insertion-order', enabled('video/h265', 'video/av1'), '', 1, [codec('vp8')], [ack('video/h265', layers=SORTED_COMMON)], True),
    ('multiple-vp8-reverse-insertion', enabled('video/vp8', 'video/VP8'), '', 1, [codec('av1')], [ack('video/VP8', layers=SORTED_COMMON)], True),
    ('skip-rtx-flexfec03', enabled('video/rtx', 'video/flexfec-03', 'video/av1'), '', 1, [codec('vp8')], [ack('video/av1', layers=SORTED_COMMON, mode=2)], False),
    ('flexfec-not-skipped', enabled('video/flexfec', 'video/av1'), '', 1, [codec('vp8')], [ack('video/flexfec', layers=SORTED_COMMON)], True),
    ('ulpfec-not-skipped', enabled('video/ulpfec', 'video/av1'), '', 1, [codec('vp8')], [ack('video/ulpfec', layers=SORTED_COMMON)], True),
    ('audio-red-fallback', enabled('audio/red', 'audio/opus'), '', 0, [codec('pcma')], [ack('audio/red', layers=SORTED_COMMON, mode=0)], True),
    ('uppercase-enabled-video-no-fallback', enabled('VIDEO/VP8'), '', 1, [codec('av1')], [ack('video/VP8', layers=SORTED_COMMON)], True),
    ('uppercase-enabled-audio-no-fallback', enabled('AUDIO/OPUS'), '', 0, [codec('red')], [ack('audio/opus', layers=SORTED_COMMON, mode=0)], True),
    ('fmtp-blocks-direct-match', [dict(mime='video/h264', fmtp_line='profile-level-id=42e01f'), dict(mime='video/vp8', fmtp_line='')], '', 1, [codec('h264')], [ack('video/vp8', layers=SORTED_COMMON)], True),
    ('fmtp-fallback-still-selects', [dict(mime='video/h264', fmtp_line='profile-level-id=42e01f')], '', 1, [codec('h264')], [ack('video/h264')], True),
    ('final-svc-mode-inference', enabled('video/av1'), '', 1, [codec('vp8')], [ack('video/av1', layers=SORTED_COMMON, mode=2)], False),
    ('svc-explicit-99-retained', enabled('video/vp9'), '', 1, [codec('vp9', mode=99)], [ack('video/vp9', mode=99)], True),
    ('video-negative-mode-retained', VP8, 'OBS', 1, [codec('vp8', mode=-1)], [ack('video/vp8', mode=-1)], True),
    ('video-explicit-svc-mode', VP8, 'OBS', 1, [codec('vp8', mode=2)], [ack('video/vp8', mode=2)], False),
    ('explicit-video-obs', VP8, 'OBS', 1, [codec('vp8')], [ack('video/vp8', mode=3)], True),
    ('explicit-video-obs-substring', VP8, 'xOBSx', 1, [codec('vp8')], [ack('video/vp8', mode=3)], True),
    ('explicit-video-lowercase-obs', VP8, 'obs', 1, [codec('vp8')], [ack('video/vp8')], True),
    ('svc-wins-over-obs', enabled('video/av1'), 'OBS', 1, [codec('av1')], [ack('video/av1', mode=2)], False),
    ('legacy-video-empty-browser', VP8, '', 1, [], [ack('', 'track-legacy-video-empty-browser', SORTED_COMMON)], False),
    ('legacy-video-obs', VP8, 'OBS', 1, [], [ack('', 'track-legacy-video-obs', SORTED_COMMON, 3)], False),
    ('legacy-video-obs-substring', VP8, 'xOBSx', 1, [], [ack('', 'track-legacy-video-obs-substring', SORTED_COMMON, 3)], False),
    ('legacy-video-lowercase-obs', VP8, 'obs', 1, [], [ack('', 'track-legacy-video-lowercase-obs', SORTED_COMMON)], False),
    ('legacy-audio-empty-browser', OPUS, '', 0, [], [ack('', 'track-legacy-audio-empty-browser', SORTED_COMMON)], False),
    ('legacy-audio-obs', OPUS, 'OBS', 0, [], [ack('', 'track-legacy-audio-obs', SORTED_COMMON, 3)], False),
    ('legacy-audio-obs-substring', OPUS, 'xOBSx', 0, [], [ack('', 'track-legacy-audio-obs-substring', SORTED_COMMON, 3)], False),
    ('legacy-audio-lowercase-obs', OPUS, 'obs', 0, [], [ack('', 'track-legacy-audio-lowercase-obs', SORTED_COMMON)], False),
    ('explicit-audio-obs-mode-zero', OPUS, 'OBS', 0, [codec('opus')], [ack('audio/opus', mode=0)], True),
    ('explicit-audio-negative-mode', OPUS, 'xOBSx', 0, [codec('opus', mode=-1)], [ack('audio/opus', mode=-1)], True),
]


def expected():
    assert len(VECTORS) == CASE_COUNT
    rows = []
    for name, codecs, browser, kind, requested, out_codecs, simulcast in VECTORS:
        request = dict(cid='track-' + name, name='C62 ' + name, type=kind, width=1920, height=1080,
                       muted=False, disable_dtx=False, source=1, layers=COMMON, simulcast_codecs=requested,
                       sid='', stereo=False, disable_red=False, encryption=0, stream='c62-stream',
                       backup_codec_policy=0, audio_features=[], packet_trailer_features=[])
        track = {k: request[k] for k in ('type', 'name', 'width', 'height', 'muted', 'disable_dtx', 'source',
                                        'stereo', 'disable_red', 'encryption', 'stream', 'backup_codec_policy',
                                        'audio_features', 'packet_trailer_features')}
        track.update(layers=SORTED_COMMON, codecs=out_codecs, simulcast=simulcast, mime_type='', mid='',
                     backup_codec_policy={0: 1, 1: 0}[kind])
        rows.append(dict(schema_version=2, case=name,
                         input=dict(enabled_codecs=codecs, browser=browser, request=request),
                         output=dict(cid=request['cid'], track=track)))
    return copy.deepcopy(rows)


def schema():
    string = dict(type='string')
    boolean = dict(type='boolean')
    i32 = dict(type='integer', minimum=-2147483648, maximum=2147483647)
    u32 = dict(type='integer', minimum=0, maximum=4294967295)
    def obj(properties):
        return dict(type='object', properties=properties, required=list(properties), additionalProperties=False)
    def arr(items):
        return dict(type='array', items=items)
    layers = arr(obj(dict(quality=i32, width=u32, height=u32, bitrate=u32, ssrc=u32,
                          spatial_layer=i32, rid=string, repair_ssrc=u32)))
    request = obj(dict(cid=string, name=string, type=i32, width=u32, height=u32, muted=boolean,
                       disable_dtx=boolean, source=i32, layers=layers,
                       simulcast_codecs=arr(obj(dict(codec=string, cid=string, layers=layers, video_layer_mode=i32))),
                       sid=string, stereo=boolean, disable_red=boolean, encryption=i32, stream=string,
                       backup_codec_policy=i32, audio_features=arr(i32), packet_trailer_features=arr(i32)))
    track = {k: v for k, v in request['properties'].items() if k not in ('cid', 'sid', 'simulcast_codecs')}
    track.update(simulcast=boolean, mime_type=string, mid=string,
                 codecs=arr(obj(dict(mime=string, cid=string, mid=string, sdp_cid=string,
                                     layer_mode=i32, layers=layers))))
    case = obj(dict(schema_version=dict(const=2, type='integer'), case=string,
                    input=obj(dict(enabled_codecs=arr(obj(dict(mime=string, fmtp_line=string))),
                                   browser=string, request=request))))
    observation = copy.deepcopy(case)
    observation['properties']['output'] = obj(dict(cid=string, track=obj(track)))
    observation['required'].append('output')
    cases = dict(type='array', items=case, minItems=CASE_COUNT, maxItems=CASE_COUNT)
    return {'$schema': 'https://json-schema.org/draft/2020-12/schema',
            'title': 'C62 original-Go codec selection ACK observer',
            '$defs': {'case': case, 'cases': cases, 'observation': observation},
            '$ref': '#/$defs/observation'}


if __name__ == '__main__':
    rows = expected()
    (HERE / 'cases.json').write_text(json.dumps([{k: v for k, v in r.items() if k != 'output'} for r in rows], indent=2) + '\n')
    (HERE / 'schema.json').write_text(json.dumps(schema(), indent=2) + '\n')
    print(f'wrote {CASE_COUNT} cases and exact schema v2')
