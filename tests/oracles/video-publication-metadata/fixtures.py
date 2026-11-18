#!/usr/bin/env python3
"""Literal C63+C64 stimuli and source-derived expectations, authored before execution.

This script never reads observed output. Run explicitly to regenerate fixtures.
"""
import copy
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def fixtures():
    rows, expected = [], []
    for name, rid, early in [('sdp-signalled-f', 'f', True), ('sdp-empty-rid', '', True),
                             ('rtp-signalled-f', 'f', False), ('rtp-empty-rid', '', False)]:
        # The sole HIGH quality is intentionally labelled spatial=2 by signalling.
        layer = dict(quality=2, width=1280, height=720, bitrate=900000,
                     ssrc=0, spatial_layer=2, rid=rid, repair_ssrc=0)
        request = dict(cid='c63-video', name='C63 video', type=1, width=1280,
                       height=720, muted=False, disable_dtx=False, source=1,
                       layers=[layer], simulcast_codecs=[dict(codec='vp8', cid='c63-video',
                       layers=[layer], video_layer_mode=0)], sid='', stereo=False,
                       disable_red=False, encryption=0, stream='c63-stream',
                       backup_codec_policy=0, audio_features=[], packet_trailer_features=[])
        stimulus = dict(fire_on_track_by_sdp=early, request=request,
                        enabled_codecs=[dict(mime='video/vp8', fmtp_line='')],
                        browser='', protocol_version=6, peer_codec=dict(mime_type='video/VP8',
                        clock_rate=90000, payload_type=96), sdp_ssrc=424242,
                        sdp_mid='0', sdp_rids=[], sdp_simulcast=False,
                        rtp=dict(count=10, sequence_start=1000, timestamp_start=90000,
                                 timestamp_step=3000, marker=True, payload_hex='1001000000',
                                 interval_ms=20))
        rows.append(dict(schema_version=1, case=name, input=stimulus))
        # Literal boundary values from participant.go 3140-3279, 3525-3571;
        # mediatrackreceiver.go 213-227, 675-725. Not a reimplementation.
        common = {k: request[k] for k in ('type', 'name', 'width', 'height', 'muted',
                  'disable_dtx', 'source', 'stereo', 'disable_red', 'encryption', 'stream',
                  'backup_codec_policy', 'audio_features', 'packet_trailer_features')}
        ack = dict(common, layers=[layer], simulcast=False, mime_type='', mid='',
                   codecs=[dict(mime_type='video/vp8', cid='c63-video', mid='', sdp_cid='',
                                video_layer_mode=1, layers=[layer])])
        published_layer = dict(quality=2, width=1280, height=720, bitrate=900000,
                               ssrc=424242, spatial_layer=0, rid='', repair_ssrc=0)
        published = dict(common, layers=[published_layer], simulcast=False,
                         mime_type='video/VP8', mid='0', codecs=[dict(mime_type='video/VP8',
                         cid='c63-video', mid='0', sdp_cid='', video_layer_mode=1,
                         layers=[published_layer])])
        expected.append(dict(schema_version=1, case=name, input=stimulus,
                             output=dict(ack=ack, callback=published, settled=published,
                             callback_before_rtp=early, publication_count=1)))
    # C64: literal source predictions, not an implementation of Go's mapping.
    # participant.go 3125-3137 sorts but does not deduplicate layers; 3275-3278
    # sets ACK simulcast=true for these multi-layer, non-SVC primary codecs.
    # mediatrackreceiver.go 151-156 retains that flag; 652-669 only ever sets it
    # true (never clears it for one sender), and mediatrack.go 272-274 clones it.
    # participant.go 1124-1169 and 3535-3539 rewrite codec/top spatial and RID.
    # buffer/videolayerutils.go 281-345 gives the literal spatial arrays below;
    # 380-382, 469-481 and mediatrackreceiver.go 675-729 select only the first
    # matching spatial-0 quality for SSRC, then propagate all codec layers.
    # Tuples: quality, width, height, bitrate, signalled spatial, signalled RID.
    shapes = [
        ('low-high', [(0, 320, 180, 120000, 7, 'c64-low'),
                      (2, 1280, 720, 910000, 9, 'c64-high')],
         [0, 1], [424242, 0]),
        ('medium-high', [(1, 640, 360, 330000, 8, 'c64-medium'),
                         (2, 1280, 720, 920000, 9, 'c64-high')],
         [0, 1], [424242, 0]),
        ('low-low-high', [(0, 320, 180, 130000, 7, 'c64-low-first'),
                          (0, 480, 270, 230000, 8, 'c64-low-second'),
                          (2, 1280, 720, 930000, 9, 'c64-high')],
         [0, 0, 1], [424242, 0, 0]),
        ('low-high-99', [(0, 320, 180, 140000, 7, 'c64-low'),
                        (2, 1280, 720, 940000, 9, 'c64-high'),
                        (99, 1920, 1080, 1940000, 11, 'c64-unknown')],
         [0, 1, -1], [424242, 0, 0]),
    ]
    for shape, values, spatial, ssrcs in shapes:
        layers = [dict(quality=q, width=w, height=h, bitrate=b, ssrc=0,
                       spatial_layer=s, rid=r, repair_ssrc=0)
                  for q, w, h, b, s, r in values]
        published_layers = [dict(layer, spatial_layer=s, rid='', ssrc=ssrc)
                            for layer, s, ssrc in zip(layers, spatial, ssrcs)]
        for phase, early in [('sdp', True), ('rtp', False)]:
            name = 'c64-' + phase + '-' + shape
            # Reuse only the literal C63 envelope/transport and boundary fields.
            # Never read cases.json, expected.json, or an observer output here.
            row = copy.deepcopy(rows[0])
            row['case'] = name
            stimulus = row['input']
            stimulus['fire_on_track_by_sdp'] = early
            request = stimulus['request']
            request.update(cid='c64-video', name='C64 video', stream='c64-stream',
                           layers=copy.deepcopy(layers))
            request['simulcast_codecs'][0].update(cid='c64-video', layers=copy.deepcopy(layers))
            prediction = copy.deepcopy(expected[0])
            prediction.update(case=name, input=copy.deepcopy(stimulus))
            prediction['output']['callback_before_rtp'] = early
            for boundary in ('ack', 'callback', 'settled'):
                track = prediction['output'][boundary]
                boundary_layers = layers if boundary == 'ack' else published_layers
                track.update(name='C64 video', stream='c64-stream', simulcast=True,
                             layers=copy.deepcopy(boundary_layers))
                track['codecs'][0].update(cid='c64-video', layers=copy.deepcopy(boundary_layers))
            rows.append(row)
            expected.append(prediction)
    return copy.deepcopy(rows), copy.deepcopy(expected)


if __name__ == '__main__':
    import subprocess
    subprocess.run(['/bin/ls', '-d', str(HERE)], check=True)
    for name, value in zip(('cases.json', 'expected.json'), fixtures()):
        (HERE / name).write_text(json.dumps(value, indent=2) + '\n')
