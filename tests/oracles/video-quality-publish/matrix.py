#!/usr/bin/env python3
"""Literal C61 inputs and independently selected expected ACK vectors (no Go output read)."""
import copy
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def layer(q, tag):
    # Every metadata component identifies its original layer, independently of quality.
    return dict(quality=q, width=100 + tag, height=200 + tag, bitrate=100000 + tag,
                ssrc=3000 + tag, spatial_layer=tag % 3, rid='rid-' + str(tag), repair_ssrc=4000 + tag)


C0, C1, C2 = layer(0, 10), layer(1, 11), layer(2, 12)
V0, V1, V2 = layer(0, 20), layer(1, 21), layer(2, 22)
H0, H2 = layer(0, 30), layer(2, 32)
U0, U2 = layer(0, 40), layer(2, 42)
MIN, NEG, OFF, Q77, MAX = [layer(q, t) for q, t in
                          [(-2147483648, 50), (-1, 51), (3, 52), (77, 53), (2147483647, 54)]]
DUP = layer(0, 60)
COMMON = [C2, C0, C1]
SORTED_COMMON = [C0, C1, C2]


def codec(name, cid, layers, mode=0):
    return dict(codec=name, cid=cid, layers=layers, video_layer_mode=mode)


def ack_codec(mime, cid, layers, mode=1):
    return dict(mime=mime, cid=cid, mid='', sdp_cid='', layer_mode=mode, layers=layers)


# Explicit expected layer choices/order: not computed by sorting, fallback, or MIME matching.
VECTORS = [
    ('supported-vp8', COMMON, [codec('vp8', 'v', [V2, V0, V1])],
     SORTED_COMMON, [ack_codec('video/vp8', 'v', [V0, V1, V2])], True),
    ('supported-two-codecs', COMMON, [codec('vp8', 'v', [V2, V0]), codec('h264', 'h', [H2, H0])],
     SORTED_COMMON, [ack_codec('video/vp8', 'v', [V0, V2]), ack_codec('video/h264', 'h', [H0, H2])], True),
    ('unsupported-absent-vp8-common', COMMON, [codec('av1', 'unsupported', [U2, U0])],
     SORTED_COMMON, [ack_codec('video/vp8', 'unsupported', SORTED_COMMON)], True),
    ('unsupported-before-explicit-vp8', COMMON, [codec('av1', 'origin', [U2, U0], 3), codec('vp8', 'later', [V2, V0], 2)],
     SORTED_COMMON, [ack_codec('video/vp8', 'origin', [V0, V2], 3)], True),
    ('unsupported-before-empty-vp8', COMMON, [codec('av1', 'origin', [U2, U0], 3), codec('vp8', 'later', [], 2)],
     SORTED_COMMON, [ack_codec('video/vp8', 'origin', SORTED_COMMON, 3)], True),
    ('supported-empty-specific-common', COMMON, [codec('vp8', 'v', [])],
     SORTED_COMMON, [ack_codec('video/vp8', 'v', SORTED_COMMON)], True),
    ('duplicate-final-codec-first-match', COMMON, [codec('vp8', 'first', [V2, V0], 2), codec('vp8', 'second', [U2, U0], 3)],
     SORTED_COMMON, [ack_codec('video/vp8', 'first', [V0, V2], 2)], False),
    ('signed-quality-common-fallback', [MAX, OFF, MIN, Q77, NEG], [codec('av1', 'origin', [U2, U0])],
     [MIN, NEG, OFF, Q77, MAX], [ack_codec('video/vp8', 'origin', [MIN, NEG, OFF, Q77, MAX])], True),
    ('signed-quality-specific', COMMON, [codec('h264', 'h', [Q77, NEG, MAX, MIN, OFF])],
     SORTED_COMMON, [ack_codec('video/h264', 'h', [MIN, NEG, OFF, Q77, MAX])], True),
    ('duplicate-quality-small-fixture', [C2, C0, DUP, C1], [codec('vp8', 'v', [])],
     [C0, DUP, C1, C2], [ack_codec('video/vp8', 'v', [C0, DUP, C1, C2])], True),
    ('legacy-no-codecs', COMMON, [],
     SORTED_COMMON, [ack_codec('', 'track-legacy-no-codecs', SORTED_COMMON)], False),
    ('no-layers', [], [codec('vp8', 'v', [])],
     [], [ack_codec('video/vp8', 'v', [])], False),
]


def expected():
    rows = []
    for name, common, codecs, out_common, out_codecs, simulcast in VECTORS:
        request = dict(cid='track-' + name, name='C61 ' + name, type=1, width=1920, height=1080,
                       muted=False, disable_dtx=False, source=1, layers=common, simulcast_codecs=codecs,
                       sid='', stereo=False, disable_red=False, encryption=0, stream='c61-stream',
                       backup_codec_policy=0, audio_features=[], packet_trailer_features=[])
        track = {k: request[k] for k in ('type', 'name', 'width', 'height', 'muted', 'disable_dtx', 'source',
                                        'stereo', 'disable_red', 'encryption', 'stream', 'backup_codec_policy',
                                        'audio_features', 'packet_trailer_features')}
        track.update(layers=out_common, codecs=out_codecs, simulcast=simulcast, mime_type='', mid='')
        rows.append(dict(schema_version=1, case=name,
                         input=dict(enabled_codecs=['video/vp8', 'video/h264'], request=request),
                         output=dict(cid=request['cid'], track=track)))
    return copy.deepcopy(rows)


def schema():
    integer = dict(type='integer')
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
    case = obj(dict(schema_version=dict(const=1, type='integer'), case=string,
                    input=obj(dict(enabled_codecs=dict(type='array', const=['video/vp8', 'video/h264'], items=string),
                                   request=request))))
    observation = copy.deepcopy(case)
    observation['properties']['output'] = obj(dict(cid=string, track=obj(track)))
    observation['required'].append('output')
    return {'$schema': 'https://json-schema.org/draft/2020-12/schema',
            'title': 'C61 original-Go AddTrack ACK observer; each JSONL line is an observation',
            '$defs': {'case': case, 'cases': arr(case), 'observation': observation},
            '$ref': '#/$defs/observation'}


if __name__ == '__main__':
    rows = expected()
    # Regeneration is explicit, never part of validation or observation.
    (HERE / 'cases.json').write_text(json.dumps([{k: v for k, v in r.items() if k != 'output'} for r in rows], indent=2) + '\n')
    (HERE / 'schema.json').write_text(json.dumps(schema(), indent=2) + '\n')
    print('wrote 12 cases and exact schema')
