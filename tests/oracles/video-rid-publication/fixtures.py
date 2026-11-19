#!/usr/bin/env python3
"""C65/C66 literal hypotheses, fixed before execution; never read observed output."""
import copy
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def fixtures():
    rows, expected = [], []
    shapes = [
        ('low-high', [(0, 320, 180, 120000, 7, 'c64-low'),
                      (2, 1280, 720, 910000, 9, 'c64-high')],
         [0, 1], ['q', 'f'], [424242, 0]),
        ('low-low-high', [(0, 320, 180, 130000, 7, 'c64-low-first'),
                          (0, 480, 270, 230000, 8, 'c64-low-second'),
                          (2, 1280, 720, 930000, 9, 'c64-high')],
         [0, 0, 1], ['q', 'q', 'f'], [424242, 0, 0]),
    ]
    for shape, values, spatial, rids, ssrcs in shapes:
        layers = [dict(quality=q, width=w, height=h, bitrate=b, ssrc=0,
                       spatial_layer=s, rid=r, repair_ssrc=0)
                  for q, w, h, b, s, r in values]
        published_layers = [dict(layer, spatial_layer=s, rid=r, ssrc=ssrc)
                            for layer, s, r, ssrc in zip(layers, spatial, rids, ssrcs)]
        for marker in (False, True):
            name = 'c65-' + shape + ('-complete' if marker else '-incomplete')
            request = dict(cid='c65-video', name='C65 video', type=1, width=1280,
                           height=720, muted=False, disable_dtx=False, source=1,
                           layers=layers, simulcast_codecs=[dict(codec='vp8', cid='c65-video',
                           layers=layers, video_layer_mode=0)], sid='', stereo=False,
                           disable_red=False, encryption=0, stream='c65-stream',
                           backup_codec_policy=0, audio_features=[], packet_trailer_features=[])
            stimulus = dict(fire_on_track_by_sdp=False, request=request,
                            enabled_codecs=[dict(mime='video/vp8', fmtp_line='')],
                            browser='', protocol_version=6, peer_codec=dict(mime_type='video/VP8',
                            clock_rate=90000, payload_type=96), sdp_ssrc=424242,
                            idle_ssrc=434343, sdp_mid='0', sdp_rids=['q', 'f'], sdp_simulcast=True,
                            quiet_ms=100, callback_timeout_ms=5000, stats_timeout_ms=5000,
                            settled_ms=100, rtp=dict(count=1, sequence_start=1000,
                            timestamp_start=90000, marker=marker, payload_hex='1001000000'))
            common = {k: request[k] for k in ('type', 'name', 'width', 'height', 'muted',
                      'disable_dtx', 'source', 'stereo', 'disable_red', 'encryption', 'stream',
                      'backup_codec_policy', 'audio_features', 'packet_trailer_features')}
            ack = dict(common, layers=layers, simulcast=True, mime_type='', mid='',
                       codecs=[dict(mime_type='video/vp8', cid='c65-video', mid='', sdp_cid='',
                                    video_layer_mode=1, layers=layers)])
            published = dict(common, layers=published_layers, simulcast=True,
                             mime_type='video/VP8', mid='0', codecs=[dict(mime_type='video/VP8',
                             cid='c65-video', mid='0', sdp_cid='', video_layer_mode=1,
                             layers=published_layers)])
            rows.append(dict(schema_version=1, case=name, input=stimulus))
            expected.append(dict(schema_version=1, case=name, input=stimulus,
                                 output=dict(ack=ack, callback=published, settled=published,
                                 callback_before_rtp=False, callback_rtp_writes=1,
                                 rtp_write_attempts=1, rtp_write_successes=1, idle_rtp_writes=0,
                                 quiet_publication_count=0, quiet_published_track_count=0,
                                 publication_count=1, settled_published_track_count=1)))
    for original in copy.deepcopy(expected):
        marker = original['input']['rtp']['marker']
        shape = original['case'].removeprefix('c65-').rsplit('-', 1)[0]
        original['case'] = 'c66-' + shape + ('-marked-padding' if marker else '-unmarked-padding')
        original['input']['rtp'].update(payload_hex='', padding_size=1)
        rows.append({k: original[k] for k in ('schema_version', 'case', 'input')})
        expected.append(original)
    return copy.deepcopy(rows), copy.deepcopy(expected)


if __name__ == '__main__':
    import subprocess
    subprocess.run(['/bin/ls', '-d', str(HERE)], check=True)
    for name, value in zip(('cases.json', 'expected.json'), fixtures()):
        (HERE / name).write_text(json.dumps(value, indent=2) + '\n')
