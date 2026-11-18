#!/usr/bin/env python3
"""Literal C63 stimuli and source-derived expectations, authored before execution.

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
    return copy.deepcopy(rows), copy.deepcopy(expected)


if __name__ == '__main__':
    import subprocess
    subprocess.run(['/bin/ls', '-d', str(HERE)], check=True)
    for name, value in zip(('cases.json', 'expected.json'), fixtures()):
        (HERE / name).write_text(json.dumps(value, indent=2) + '\n')
