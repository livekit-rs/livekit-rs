#!/usr/bin/env python3
"""Official SDK media recovery under impairment: pinned Go vs Rust.

  python3 run.py --go <pinned-go-server> --rust <livecloud> --deps <tests/interop> --output <new-dir>
                [--single false] [--profile vp8|vp8-simulcast|vp9-svc|h264|av1] [--e2ee]
                [--layer-switch | --vp9-pli-control]

Each server runs owned, single node, loopback, with its built-in TURN/UDP enabled; both browsers are forced
onto the relay through per-client impairment proxies. The comparison checks, per phase, which recovery
mechanisms were actually used (NACK, retransmission, PLI, keyframes) and that decoding resumed after each
blackout within a bounded time relative to Go. Every decoded frame carries the publisher's counter: the
comparison also checks corrupted and out-of-order pictures, how far the subscriber lags after recovery,
audio resumption and the negotiated codec. Received RTP header facts (per SSRC sequence gaps, reordering,
timestamp jumps) are reported alongside. Raw per-phase data stays in each evidence.json.
"""
import argparse, hashlib, json, os, signal, socket, subprocess, tempfile, time
from pathlib import Path

# (phase, side, field): compared as "did this happen" (delta > 0)
CHECKS = [
    ("baseline", "subscriber", "framesDecoded"),
    ("down-loss-5", "subscriber", "nackCount"), ("down-loss-5", "subscriber", "retransmittedPacketsReceived"),
    ("down-loss-5", "publisher", "nackCount"), ("down-loss-5", "subscriber", "framesDecoded"),
    ("down-jitter-30", "subscriber", "framesDecoded"),
    ("down-blackout-3s", "publisher", "pliCount"), ("down-blackout-3s", "publisher", "keyFramesEncoded"),
    ("down-blackout-3s", "subscriber", "pliCount"), ("down-blackout-3s", "subscriber", "keyFramesDecoded"),
    ("up-loss-5", "publisher", "nackCount"), ("up-loss-5", "publisher", "retransmittedPacketsSent"),
    ("up-loss-5", "subscriber", "framesDecoded"),
    ("up-blackout-3s", "publisher", "pliCount"), ("up-blackout-3s", "publisher", "keyFramesEncoded"),
    ("up-blackout-3s", "subscriber", "keyFramesDecoded"),
]
PHASES = (
    "baseline", "down-loss-5", "down-jitter-30", "down-blackout-3s",
    "up-loss-5", "up-jitter-30", "up-blackout-3s",
)


def free(kind, taken=set()):
    # Pick from 20000-29999 (outside the OS ephemeral range) so other processes do not grab
    # the port between this probe and the server's bind; startup retries cover the rest.
    import random
    for _ in range(200):
        p = random.randint(20000, 29999)
        if p in taken:
            continue
        s = socket.socket(socket.AF_INET, kind)
        try:
            s.bind(("127.0.0.1", p))
        except OSError:
            continue
        finally:
            s.close()
        taken.add(p)
        return p
    raise RuntimeError("no free port")


def run_one(kind, binary, deps, out, single, profile, e2ee, layer_switch, vp9_pli_control=False):
    import shutil
    for attempt in range(3):
        try:
            return run_attempt(kind, binary, deps, out, single, profile, e2ee, layer_switch, vp9_pli_control)
        except PortTaken:
            # The server never served: keep only the log line, then retry on fresh ports.
            (out.parent / f"{out.name}-port-retry{attempt}.log").write_bytes((out / "server.log").read_bytes()[-2000:])
            shutil.rmtree(out)
    raise RuntimeError(f"{kind}: ports kept being taken")


class PortTaken(Exception):
    pass


def run_attempt(kind, binary, deps, out, single, profile, e2ee, layer_switch, vp9_pli_control):
    out.mkdir()
    http, tcp, udp, turn = free(socket.SOCK_STREAM), free(socket.SOCK_STREAM), free(socket.SOCK_DGRAM), free(socket.SOCK_DGRAM)
    relay = 37000 + (os.getpid() % 200) * 20
    cfg = {"port": http, "bind_addresses": ["127.0.0.1"], "keys": {"devkey": "secret"},
           "rtc": {"node_ip": "127.0.0.1", "use_external_ip": False, "enable_loopback_candidate": True,
                   "ips": {"includes": ["127.0.0.1/32"]}, "tcp_port": tcp, "udp_port": udp},
           "turn": {"enabled": True, "udp_port": turn, "bind_addresses": ["127.0.0.1"],
                    "relay_range_start": relay, "relay_range_end": relay + 19, "allow_restricted_peer_cidrs": ["127.0.0.1/32"]}}
    if layer_switch:
        # Isolate explicit subscriber selection from allocator-driven changes.
        # The separate default campaign continues to exercise congestion control.
        cfg["rtc"]["congestion_control"] = {"enabled": False}
    if os.environ.get("LK_REC_LOGGING"):
        # Diagnostic runs only, e.g. {"level":"info","component_levels":{"sfu":"debug"}}.
        cfg["logging"] = json.loads(os.environ["LK_REC_LOGGING"])
    with tempfile.TemporaryDirectory(prefix="lk-rec-") as tmp:
        config = Path(tmp) / "server.yaml"; config.write_text(json.dumps(cfg))
        with (out / "server.log").open("wb") as log:
            server = subprocess.Popen([str(binary), "--config", str(config)], stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if server.poll() is not None:
                        if b"address already in use" in (out / "server.log").read_bytes():
                            raise PortTaken()
                        raise AssertionError(f"{kind} server exited during startup")
                    try:
                        socket.create_connection(("127.0.0.1", http), timeout=.5).close(); break
                    except OSError:
                        assert time.monotonic() < deadline; time.sleep(.1)
                env = dict(os.environ, INTEROP_DEPENDENCY_ROOT=str(deps), INTEROP_OUTPUT_DIR=str(out / "sdk"),
                           LIVEKIT_URL=f"ws://127.0.0.1:{http}", TURN_PORT=str(turn), SINGLE_PC=single, PROFILE=profile,
                           E2EE=str(e2ee).lower(), RELAY="true", LK_REC_LAYER_SWITCH="1" if layer_switch else "0",
                           LK_REC_VP9_PLI_CONTROL="1" if vp9_pli_control else "0")
                with (out / "probe.log").open("wb") as plog:
                    probe = subprocess.run(["node", str(Path(__file__).with_name("run.mjs"))], env=env, stdout=plog, stderr=subprocess.STDOUT, timeout=600)
            finally:
                server.send_signal(signal.SIGINT)
                try:
                    server_exit = server.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL); server_exit = server.wait()
    return {"probe_exit": probe.returncode, "server_exit": server_exit, "config": cfg,
            "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
            "evidence": json.loads((out / "sdk" / "evidence.json").read_text())}


def compare(runs, exits, e2ee, layer_switch=False, vp9_pli_control=False):
    phases = {k: {p["name"]: p for p in v["evidence"].get("phases", [])} for k, v in runs.items()}
    rows = []

    def gate(field, go, rust, match):
        rows.append({"phase": "campaign", "side": "both", "field": field, "go": go, "rust": rust, "match": match})

    go, rust = (runs[k]["evidence"] for k in ("go", "rust"))
    gate("exits", exits["go"], exits["rust"], all(x == {"probe": 0, "server": 0} for x in exits.values()))
    for field in ("status", "sdkSha256", "sdkVersion", "lockSha256", "browser"):
        g, r = go.get(field), rust.get(field)
        gate(field, g, r, g == r and (g == "DONE" if field == "status" else bool(g)))
    g, r = go.get("fixtureHashes"), rust.get("fixtureHashes")
    if g is not None or r is not None:  # historical receipts lack this field
        gate("fixtureHashes", g, r, bool(g) and g == r)
    g, r = (e.get("cleanupErrors") for e in (go, rust))
    gate("cleanupErrors", g, r, g == r == [])
    g, r = (e.get("relay", {}) for e in (go, rust))
    gate("selectedRelay", g, r, all(
        peers.get(k) and all(p.get("type") == "relay" for p in peers[k])
        for peers in (g, r) for k in ("a", "b")
    ))
    if any(e.get("videoLossSchema") == 1 for e in (go, rust)):
        modes = [e.get("single") for e in (go, rust)]
        topology = [{
            peer: {
                "relayPairs": len(e.get("relay", {}).get(peer, [])),
                "negotiated": sum(bool(pc.get("local") and pc.get("remote"))
                                  for pc in e.get("feedback", {}).get(peer, [])),
            } for peer in ("a", "b")
        } for e in (go, rust)]
        expected = 1 if modes[0] is True else 2
        gate("layeredPeerConnectionCount", *topology, modes[0] is modes[1]
             and isinstance(modes[0], bool) and all(
                 pc == {"relayPairs": expected, "negotiated": expected}
                 for report in topology for pc in report.values()
             ))
    g, r = (e.get("e2ee", False) for e in (go, rust))
    gate("e2eeMode", g, r, g == r == e2ee)
    if e2ee:
        g, r = (e.get("workerSha256") for e in (go, rust))
        gate("workerSha256", g, r, bool(g) and g == r)
        g, r = (e.get("encryption", {}) for e in (go, rust))
        gate("encryptedAudioVideo", g, r, all(
            set(e.get("tracks", [])) == {"audio", "video"}
            and e.get("errors") == [] and e.get("audioSamples", 0) > 0 for e in (g, r)
        ))
    if layer_switch:
        gs, rs = ((e.get("layerSwitch") or {}).get("stages", []) for e in (go, rust))
        expected = ["initial-high", "low-with-loss", "high-with-loss"]
        g, r = ([s["name"] for s in stages] for stages in (gs, rs))
        gate("layerSwitchStageOrder", g, r, g == r == expected)
        for a, b in zip(gs, rs):
            g, r = a["size"], b["size"]
            gate(f"{a['name']}.size", g, r, bool(g) and g == r)
        return all(row["match"] for row in rows), rows
    if vp9_pli_control:
        g, r = [list(p) for p in phases.values()]
        gate("phaseOrder", g, r, g == r == ["baseline", "down-blackout-3s"])
        outcomes = [p.get("down-blackout-3s", {}) for p in phases.values()]
        for side, field in (("subscriber", "pliCount"), ("publisher", "pliCount"),
                            ("publisher", "keyFramesEncoded"), ("subscriber", "keyFramesDecoded")):
            g, r = [p.get(side, {}).get(field, 0) for p in outcomes]
            gate(f"{side}.{field}", g, r, g > 0 and r > 0)
        recovered = [p.get("afterRestore") or {} for p in outcomes]
        g, r = [p.get("firstValidMs") for p in recovered]
        gate("decodedAfterBlackout", g, r, all(
            value is not None and 0 <= value < 3000 for value in (g, r)
        ))
        headers = [p.get("videoFeedback", {}).get("publisherReceived", []) for p in outcomes]
        g, r = [[row["ms"] for row in stream if row["pt"] == 206 and row["fmt"] == 1]
                for stream in headers]
        gate("publisherReceivedPliHeader", g, r, bool(g) and bool(r))
        g, r = go.get("frameTrace"), rust.get("frameTrace")
        if g is not None or r is not None:
            gate("frameTraceMode", g, r, g == r and bool(g))
            traces = [p.get("frameTrace") or {} for p in outcomes]

            def has_wire_timestamp(trace, frames, leg, bound_ssrc=False):
                observed = trace.get(frames, [])
                return bool(observed) and any(
                    isinstance(frame.get("rtpTimestamp"), int)
                    and packet["ts"] == frame["rtpTimestamp"]
                    and (not bound_ssrc or packet["ssrc"] == frame["ssrc"])
                    for frame in observed[:1] for packet in trace.get(leg, {}).get("forwarded", [])
                )

            g, r = [has_wire_timestamp(t, "publisherKeyframes", "publisherWire", True) for t in traces]
            gate("publisherKeyframeOnWire", g, r, g and r)
            g, r = [has_wire_timestamp(t, "firstPicturesAfterRelease", "subscriberWire") for t in traces]
            gate("firstDecodedPictureOnWire", g, r, g and r)
            if (go.get("frameTrace") or {}).get("stableSource"):
                g, r = [tuple((t.get("lastPictureBeforeRelease") or {}).get(k) for k in ("w", "h"))
                        for t in traces]
                gate("stablePictureBeforeBlackout", g, r, g == r == (960, 544))
        for field in ("invalid", "regressions"):
            g, r = [p.get(field) for p in recovered]
            gate(f"recoveredPictures.{field}", g, r, g == r == 0)
        return all(row["match"] for row in rows), rows
    g, r = ([p["name"] for p in e.get("phases", [])] for e in (go, rust))
    gate("phaseOrder", g, r, tuple(g) == tuple(r) == PHASES)
    g, r = (phases[k].get("baseline", {}).get("pictures", {}).get("valid", 0) for k in ("go", "rust"))
    gate("validBaselinePictures", g, r, g > 0 and r > 0)
    g, r = (e.get("videoLossSchema") for e in (go, rust))
    if g is not None or r is not None:  # historical layered receipts predate video-only attribution
        gate("videoLossSchema", g, r, g == r == 1)
        for phase in ("down-loss-5", "up-loss-5"):
            name = go.get("profile")
            if name != rust.get("profile") or name not in ("vp8-simulcast", "vp9-svc"):
                gate(f"{phase}.layeredProfile", name, rust.get("profile"), False)
                continue
            codec = "video/VP8" if name == "vp8-simulcast" else "video/VP9"
            evidence = [phases[k].get(phase, {}).get("videoLoss") or {} for k in ("go", "rust")]
            streams = [[{
                **{key: s[key] for key in ("rid", "ssrc", "pt", "ingress", "forwarded")},
                "dropped": len(s["dropped"]),
            } for s in item.get("wire", []) if s["codec"] == codec] for item in evidence]
            gate(f"{phase}.negotiatedVideoLoss", *streams, all(
                any(s["dropped"] > 0 and s["forwarded"] > 0 for s in side)
                and all(s["ingress"] == s["forwarded"] + s["dropped"] for s in side)
                for side in streams
            ))
            pictures = [item.get("pictures", {}) for item in evidence]
            gate(f"{phase}.decodedDuringLoss", *pictures, all(p.get("valid", 0) > 0 for p in pictures))
            if name == "vp8-simulcast":
                side, rtx = ("publisher", "retransmittedPacketsSent") if phase == "up-loss-5" else (
                    "subscriber", "retransmittedPacketsReceived",
                )
                repair = [{field: phases[k].get(phase, {}).get(side, {}).get(field)
                           for field in ("nackCount", rtx)} for k in ("go", "rust")]
                gate(f"{phase}.videoNackAndRetransmission", *repair, all(
                    all(value is not None and value > 0 for value in stats.values()) for stats in repair
                ))
            if phase == "up-loss-5" and name == "vp8-simulcast":
                gate("up-loss-5.independentSimulcastEncodings", *streams, all(
                    len(side) == 3 and {s["rid"] for s in side} == {"q", "h", "f"}
                    and len({s["ssrc"] for s in side}) == 3
                    and all(s["dropped"] > 0 and s["forwarded"] > 0 for s in side)
                    for side in streams
                ))
            if phase == "up-loss-5" and name == "vp9-svc":
                encoders = [[[
                    {key: s.get(key) for key in (
                        "ssrc", "pt", "rid", "requestedScalabilityMode", "scalabilityMode", "frames"
                    )}
                    for s in item.get(sample, {}).get("publisher", [])
                    if s["codec"] == codec and s["active"] and s["frames"] > 0
                ] for sample in ("before", "after")] for item in evidence]

                def svc_encoding_survives_loss(samples, wire):
                    before, after = samples
                    if len(before) != 1 or len(after) != 1:
                        return False
                    start, end = before[0], after[0]
                    # Chromium may adapt a requested L3T3_KEY encoding to two
                    # or one spatial layers while the same source keeps sending.
                    effective = {"L1T3", "L2T3_KEY", "L3T3_KEY"}
                    return (
                        start["rid"] is None and end["rid"] is None
                        and start["requestedScalabilityMode"] == end["requestedScalabilityMode"] == "L3T3_KEY"
                        and all(s["scalabilityMode"] in effective for s in (start, end))
                        and end["frames"] > start["frames"]
                        and len(wire) == 1 and all(
                            stream["ssrc"] == start["ssrc"] == end["ssrc"]
                            and stream["pt"] == start["pt"] == end["pt"]
                            and stream["dropped"] > 0 and stream["forwarded"] > 0
                            for stream in wire
                        )
                    )

                gate("up-loss-5.activeVp9SvcEncoding", *encoders, all(
                    svc_encoding_survives_loss(samples, wire)
                    for samples, wire in zip(encoders, streams)
                ))
    ok = all(row["match"] for row in rows)
    for phase, side, field in CHECKS:
        g = phases["go"].get(phase, {}).get(side, {}).get(field); r = phases["rust"].get(phase, {}).get(side, {}).get(field)
        same = g is not None and r is not None and (g > 0) == (r > 0)
        ok &= same
        rows.append({"phase": phase, "side": side, "field": field, "go": g, "rust": r, "match": same})
    for phase in ("down-blackout-3s", "up-blackout-3s"):
        g = phases["go"].get(phase, {}).get("recoveryMs"); r = phases["rust"].get(phase, {}).get("recoveryMs")
        same = g is not None and r is not None and r <= max(2 * g, g + 1500)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "recoveryMs", "go": g, "rust": r, "match": same})
        # Go sends upstream PLIs only for layer lock / forwarded subscriber PLI, so a
        # blackout costs the publisher about one keyframe; Rust must not add its own.
        g = phases["go"].get(phase, {}).get("publisher", {}).get("pliCount")
        r = phases["rust"].get(phase, {}).get("publisher", {}).get("pliCount")
        same = g is not None and r is not None and r <= g + 1
        ok &= same
        rows.append({"phase": phase, "side": "publisher", "field": "pliCount<=go+1", "go": g, "rust": r, "match": same})
    for phase in PHASES:
        G, R = phases["go"].get(phase, {}), phases["rust"].get(phase, {})
        gp, rp = G.get("pictures") or {}, R.get("pictures") or {}
        # Corrupted or out-of-order pictures: never where Go has none, never more than Go.
        for field in ("invalid", "regressions"):
            g, r = gp.get(field), rp.get(field)
            same = g is not None and r is not None and (r == 0 if g == 0 else r <= 2 * g + 2)
            ok &= same
            rows.append({"phase": phase, "side": "subscriber", "field": f"pictures.{field}", "go": g, "rust": r, "match": same})
        ga, ra = G.get("audio") or {}, R.get("audio") or {}
        g, r = ga.get("packetsLost"), ra.get("packetsLost")
        same = g is not None and r is not None and (r <= 0 if g <= 0 else r <= 2 * g + 5)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "audio.packetsLost", "go": g, "rust": r, "match": same})
        # Which audio packets the seeded proxy drops depends on audio/video interleaving, so
        # with loss compare concealment per lost packet; without loss compare totals.
        g, r = ga.get("concealedSamples"), ra.get("concealedSamples")
        gl, rl = ga.get("packetsLost") or 0, ra.get("packetsLost") or 0
        if g is not None and r is not None and gl > 0 and rl > 0:
            g, r = round(g / gl), round(r / rl)
            same = r <= max(1.3 * g, g + 150)
            field = "audio.concealedPerLostPacket"
        else:
            same = g is not None and r is not None and r <= max(1.5 * g, g + 4800)
            field = "audio.concealedSamples"
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": field, "go": g, "rust": r, "match": same})
        g, r = G.get("codecs"), R.get("codecs")
        same = g is not None and g == r
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "codec", "go": g, "rust": r, "match": same})
    for phase in ("down-blackout-3s", "up-blackout-3s"):
        G = phases["go"].get(phase, {}).get("afterRestore") or {}
        R = phases["rust"].get(phase, {}).get("afterRestore") or {}
        g, r = G.get("firstValidMs"), R.get("firstValidMs")
        same = g is not None and r is not None and r <= max(2 * g, g + 1500)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "firstValidPictureMs", "go": g, "rust": r, "match": same})
        # Which layer the subscriber settles on after recovery (simulcast/SVC layer choice).
        g = ((G.get("lagTimeline") or [{}])[-1]).get("size")
        r = ((R.get("lagTimeline") or [{}])[-1]).get("size")
        same = g is not None and g == r
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "resolutionAfterRecovery", "go": g, "rust": r, "match": same})
        g, r = G.get("lagFrames"), R.get("lagFrames")
        same = g is not None and r is not None and r <= max(g + 15, 2 * g)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "lagFramesAfterRecovery", "go": g, "rust": r, "match": same})
        g, r = (G.get("audio") or {}).get("concealedSamples"), (R.get("audio") or {}).get("concealedSamples")
        same = g is not None and r is not None and r <= max(1.5 * g, g + 4800)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "audio.concealedAfterRestore", "go": g, "rust": r, "match": same})
        g, r = (G.get("audio") or {}).get("packetsReceived"), (R.get("audio") or {}).get("packetsReceived")
        same = g is not None and r is not None and (g > 0) == (r > 0)
        ok &= same
        rows.append({"phase": phase, "side": "subscriber", "field": "audio.packetsReceived", "go": g, "rust": r, "match": same})
    return ok, rows


def rtp_facts(runs):
    """Observed (not asserted) received-RTP header facts per blackout: largest SSRC streams."""
    out = {}
    for kind, v in runs.items():
        for p in v["evidence"].get("phases", []):
            rr = p.get("restoreRtp")
            if not rr:
                continue
            pick = lambda xs: [{k: x[k] for k in ("pt", "packets", "missing", "reordered", "maxSeqJump", "maxTsJump")} for x in xs[:3]]
            out.setdefault(p["name"], {})[kind] = {"outage": pick(rr["outage"]), "afterRestore": pick(rr["afterRestore"])}
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--go", type=Path, required=True); p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--deps", type=Path, required=True); p.add_argument("--output", type=Path, required=True)
    p.add_argument("--single", default="true", choices=("true", "false"))
    p.add_argument("--profile", default="vp8", choices=("vp8", "vp8-simulcast", "vp9-svc", "h264", "av1"))
    p.add_argument("--e2ee", action="store_true")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--layer-switch", action="store_true", help="explicit low/high selection under downstream loss")
    mode.add_argument("--vp9-pli-control", action="store_true",
                      help="require positive PLI and keyframe recovery after a focused VP9 downstream blackout")
    p.add_argument("--recompare", action="store_true")
    a = p.parse_args()
    if a.layer_switch and a.profile not in ("vp8-simulcast", "vp9-svc"):
        p.error("--layer-switch requires vp8-simulcast or vp9-svc")
    if a.vp9_pli_control and (a.profile != "vp9-svc" or not a.e2ee):
        p.error("--vp9-pli-control requires --profile vp9-svc --e2ee")
    if a.recompare:
        runs = {k: {"evidence": json.loads((a.output / k / "sdk" / "evidence.json").read_text())} for k in ("go", "rust")}
        old = json.loads((a.output / "comparison.json").read_text())
        a.layer_switch = old.get("layer_switch", False)
        a.vp9_pli_control = old.get("vp9_pli_control", False)
    else:
        a.output.mkdir()
        runs = {k: run_one(k, b.resolve(), a.deps.resolve(), a.output / k, a.single, a.profile,
                           a.e2ee, a.layer_switch, a.vp9_pli_control)
                for k, b in (("go", a.go), ("rust", a.rust))}
    exits = old["exits"] if a.recompare else {k: {"probe": v["probe_exit"], "server": v["server_exit"]} for k, v in runs.items()}
    print("PROCESS_EXITS " + json.dumps(exits), flush=True)
    ok, rows = compare(runs, exits, a.e2ee, a.layer_switch, a.vp9_pli_control)
    report = {"single_pc": a.single, "profile": a.profile, "e2ee": a.e2ee, "layer_switch": a.layer_switch,
              "vp9_pli_control": a.vp9_pli_control, "match": ok, "rows": rows, "rtp": rtp_facts(runs),
              "binaries": old["binaries"] if a.recompare else {k: v["binary_sha256"] for k, v in runs.items()},
              "exits": exits}
    (a.output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    for r in rows:
        print(("MATCH " if r["match"] else "DIFF  ") + f"{r['phase']:18} {r['side']:10} {r['field']:30} go={r['go']} rust={r['rust']}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
