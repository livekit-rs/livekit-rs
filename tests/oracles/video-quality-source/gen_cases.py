#!/usr/bin/env python3
"""Reproducible inventory for the fixed-Go VideoLayer open-quality consumer observer.

Writes two byte-identical copies: the oracle cases.json (read by the Go observer
via VIDEO_QUALITY_CASES) and the Rust include_str! copy. Every case is a TrackInfo
premise (non-nil, codec-less so GetVideoLayersForMimeType falls back to ti.Layers)
plus one quality / spatial / rid query. Layer qualities are NON-NEGATIVE: fixed Go
LayerPresenceFromTrackInfo indexes layerPresence[layer.Quality] when Quality<=HIGH,
so a negative layer quality panics upstream (a documented boundary the Rust mirror
safely excludes, already covered by the M04 component test). Requested query quality
may be any int32 (the switch default returns InvalidLayerSpatial, never indexes).

Three focused families per premise give full input-domain coverage of every observed
consumer without an exploding cross product: the quality family sweeps query_quality
(the spatial/rid consumers ignore it), the spatial family sweeps query_spatial, the
rid family sweeps query_rid. Each row still emits all eight outputs, so the fixed
representative queries of the non-focused families are compared too.
"""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUST_COPY = HERE.parent.parent.parent / "crates" / "rtc-media" / "src" / "video_quality_cases.json"

# (name, [(quality, spatial_layer, rid), ...]) — quality ascending except where the
# premise deliberately exercises non-canonical signalled spatial / unknown quality.
LAYER_SETS = [
    ("empty", []),
    ("low", [(0, 0, "q")]),
    ("med", [(1, 0, "q")]),
    ("high", [(2, 0, "q")]),
    ("lm", [(0, 0, "q"), (1, 1, "h")]),
    ("lh", [(0, 0, "q"), (2, 1, "h")]),
    ("mh", [(1, 0, "q"), (2, 1, "h")]),
    ("lmh", [(0, 0, "q"), (1, 1, "h"), (2, 2, "f")]),
    ("h_unk", [(2, 0, "q"), (77, 1, "x")]),
    ("noncanon", [(0, 2, "f"), (1, 0, "q"), (2, 1, "h")]),
    ("off", [(3, 0, "q")]),
    ("lmh_norid", [(0, 0, ""), (1, 1, ""), (2, 2, "")]),
]
RID_SPACES = [["q", "h", "f"], ["2", "1", "0"], ["q", "h", ""], ["", "", ""]]
QUALITIES = [0, 1, 2, 3, 4, 77, -1, 2147483647]
SPATIALS = [-1, 0, 1, 2, 3, 77]
RIDS = ["", "q", "h", "f", "2", "1", "0", "x"]


def layer_obj(index, quality, spatial, rid):
    height = 180 * (spatial + 1) if 0 <= spatial <= 2 else 180
    return {
        "quality": quality,
        "spatial": spatial,
        "rid": rid,
        "width": height * 16 // 9,
        "height": height,
        "bitrate": 150000 * (spatial + 1) if spatial >= 0 else 150000,
        "ssrc": 1001 + index,
        "repair_ssrc": 0,
    }


def build():
    cases = []
    for name, layers in LAYER_SETS:
        objs = [layer_obj(i, q, s, r) for i, (q, s, r) in enumerate(layers)]
        for rs_idx, rid_space in enumerate(RID_SPACES):
            premise = {"mime": "video/VP8", "layers": objs, "rid_space": rid_space}
            for j, q in enumerate(QUALITIES):
                cases.append({"id": f"{name}-{rs_idx}-Q-{j}", **premise,
                              "query_quality": q, "query_spatial": 0, "query_rid": "q"})
            for j, s in enumerate(SPATIALS):
                cases.append({"id": f"{name}-{rs_idx}-S-{j}", **premise,
                              "query_quality": 2, "query_spatial": s, "query_rid": "q"})
            for j, r in enumerate(RIDS):
                cases.append({"id": f"{name}-{rs_idx}-R-{j}", **premise,
                              "query_quality": 2, "query_spatial": 0, "query_rid": r})
    return cases


def main():
    cases = build()
    ids = [c["id"] for c in cases]
    assert len(ids) == len(set(ids)), "duplicate case id"
    text = json.dumps(cases, indent=2, ensure_ascii=False) + "\n"
    for path in (HERE / "cases.json", RUST_COPY):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    digest = hashlib.sha256(text.encode()).hexdigest()
    print(f"cases={len(cases)} sha256={digest}")
    print(f"oracle={HERE / 'cases.json'}")
    print(f"rust_copy={RUST_COPY}")


if __name__ == "__main__":
    main()