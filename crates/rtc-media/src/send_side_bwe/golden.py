#!/usr/bin/env python3
"""Execute pinned actual Go send-side BWE policy with injected deterministic clocks.

Coordinator-owned compiler invocation. Originals are only read; temporary source
is self-cleaning. No Rust results are used to generate expected output.

Unlike the remote BWE golden, the collaborator packages (ccutils prober, trend
detector, probe regulator, probe signal and the bwe contracts) are compiled from
their real pinned sources, so probe cluster accounting, trend statistics and the
sequence number randomisation boundary are the only adapted spots.

Stimulus units are microseconds, end to end, and every time-valued leaf key
carries the `_us` suffix that says so. This matters because the pinned Go types
do not agree: `ProbeClusterGoal.Duration` is a `time.Duration`, the prober's
`ProbeClusterResult.StartTime`/`EndTime` are raw `int64` nanoseconds and
`congestionDetector.rtt` is a float count of seconds. Reading the same digits as
another unit is the difference between a 600 ms probe and a 600 s one, so a key
without the `_us` suffix, or spelled `_ns`/`_ms`/`_s`, aborts generation instead
of being silently rescaled. The generator also asserts the round trip after each
conversion and echoes the goal/result fields it actually received.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
FILES = [
    'pkg/sfu/bwe/bwe.go',
    'pkg/sfu/bwe/null_bwe.go',
    'pkg/sfu/bwe/sendsidebwe/congestion_detector.go',
    'pkg/sfu/bwe/sendsidebwe/packet_group.go',
    'pkg/sfu/bwe/sendsidebwe/packet_info.go',
    'pkg/sfu/bwe/sendsidebwe/packet_tracker.go',
    'pkg/sfu/bwe/sendsidebwe/probe_packet_group.go',
    'pkg/sfu/bwe/sendsidebwe/send_side_bwe.go',
    'pkg/sfu/bwe/sendsidebwe/traffic_stats.go',
    'pkg/sfu/bwe/sendsidebwe/twcc_feedback.go',
    'pkg/sfu/ccutils/prober.go',
    'pkg/sfu/ccutils/trenddetector.go',
    'pkg/sfu/ccutils/probe_regulator.go',
    'pkg/sfu/ccutils/probe_signal.go',
]
COMMIT = 'e5655ac20267fa5005dccaf3f0e3bfc3ea688c07'
# Real third-party packages kept as imports, never substituted.
KEEP_IMPORTS = ('github.com/livekit/protocol/utils', 'github.com/pion/rtcp',
                'github.com/gammazero/deque', 'go.uber.org/atomic')
# Import lines removed because only the injected clock and the pinned sequence
# number start touch those symbols.
DROP_IMPORTS = ('github.com/livekit/livekit-server/pkg/sfu/bwe',
                'github.com/livekit/livekit-server/pkg/sfu/ccutils',
                'github.com/livekit/protocol/logger', 'github.com/livekit/protocol/utils/mono',
                'go.uber.org/zap/zapcore', 'math/rand')

def transformed(source):
    source = re.sub(r'^package \w+$', 'package main', source, flags=re.M)
    lines = []
    for line in source.splitlines(True):
        match = re.match(r'^\s*"([^"]*)"\n$', line)
        if match and match.group(1) in DROP_IMPORTS:
            if match.group(1) in KEEP_IMPORTS:
                raise SystemExit('import is both kept and dropped: %s' % match.group(1))
            continue
        lines.append(line)
    source = ''.join(lines)
    source = re.sub(r'^var _ bwe\.BWE = .*\n', '', source, flags=re.M)
    source = source.replace('logger.Logger', 'goldenLogger').replace('logger.ObjectSlice', 'goldenSlice')
    source = source.replace('logger.Int64Slice', 'goldenInt64Slice').replace('zapcore.ObjectEncoder', 'goldenEncoder')
    source = re.sub(r'\bbwe\.(?=[A-Za-z_])', '', source)
    source = re.sub(r'\bccutils\.(?=[A-Za-z_])', '', source)
    source = source.replace('mono.Now()', 'goldenNow()').replace('time.Now()', 'goldenNow()')
    source = source.replace('mono.UnixMicro()', 'goldenUnixMicro()').replace('time.Since(', 'goldenSince(')
    # The tracker starts its transport-wide sequence number at a random point of the
    # space. Only the random draw is pinned; the sequence number arithmetic itself,
    # including the 16-bit wrap, is the original code.
    source = source.replace('uint64(rand.Intn(1<<14)) + uint64(1<<15)', 'goldenSequenceNumberStart')
    for leftover in ('mono.', 'logger.', 'zapcore.', 'rand.'):
        if leftover in source:
            raise SystemExit('unadapted reference to %s' % leftover)
    return source

STUBS = r'''package main
import "time"
var currentGoldenTime = time.Unix(1000, 0)
var goldenSequenceNumberStart = uint64(1 << 15)
func goldenNow() time.Time { return currentGoldenTime }
func goldenUnixMicro() int64 { return currentGoldenTime.UnixMicro() }
func goldenSince(t time.Time) time.Duration { return currentGoldenTime.Sub(t) }
type goldenLogger struct{}
func (goldenLogger) Infow(string, ...any) {}
func (goldenLogger) Debugw(string, ...any) {}
func (goldenLogger) Warnw(string, ...any) {}
func (goldenLogger) Errorw(string, ...any) {}
func goldenSlice[T any](v []T) any { return v }
func goldenInt64Slice(v []int64) any { return v }
type goldenEncoder interface {
 AddString(string,string); AddDuration(string,time.Duration); AddFloat64(string,float64)
 AddTime(string,time.Time); AddUint32(string,uint32); AddUint16(string,uint16); AddUint64(string,uint64)
 AddInt(string,int); AddInt64(string,int64); AddBool(string,bool); AddArray(string,any); AddObject(string,any)
}
type goldenObserver struct {
 changes  int
 committed int64
 from     CongestionState
 to       CongestionState
}
func (o *goldenObserver) OnCongestionStateChange(fromState CongestionState, toState CongestionState, capacity int64) {
 o.changes++
 o.from = fromState
 o.to = toState
 o.committed = capacity
}
'''

MAIN = r'''package main
import ("bufio";"encoding/csv";"fmt";"os";"strconv";"strings";"time";"github.com/pion/rtcp")

func num(s string) int64 { n, e := strconv.ParseInt(s, 10, 64); if e != nil { panic(s + ": " + e.Error()) }; return n }
func us(s string) time.Duration { return time.Duration(num(s)) * time.Microsecond }
func flt(s string) float64 { v, e := strconv.ParseFloat(s, 64); if e != nil { panic(e) }; return v }
func f(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
func bi(v bool) int { if v { return 1 }; return 0 }
func leafKey(s string, key string) int64 {
 for _, item := range strings.Split(s, ";") {
  if item == "" { continue }
  kv := strings.SplitN(item, "=", 2)
  if kv[0] == key { return num(kv[1]) }
 }
 return 0
}

// leafKeyOr is leafKey with an explicit default when the key is absent.
func leafKeyOr(s string, key string, def int64) int64 {
 for _, item := range strings.Split(s, ";") {
  if item == "" { continue }
  kv := strings.SplitN(item, "=", 2)
  if kv[0] == key { return num(kv[1]) }
 }
 return def
}

// timeUs reads one time-valued leaf key in the single unit this campaign uses:
// microseconds. The suffix is not decoration. The pinned Go types disagree about
// units (Goal.Duration is a time.Duration, Result.StartTime/EndTime are raw
// nanoseconds, congestionDetector.rtt is float seconds), so an ambiguous or
// mis-suffixed spelling must abort generation rather than be read at the wrong
// scale. The second result reports absence, which only the optional keys use.
func timeUs(op, extra, key string) (int64, bool) {
 for _, item := range strings.Split(extra, ";") {
  if item == "" { continue }
  kv := strings.SplitN(item, "=", 2)
  if kv[0] != key && !strings.HasPrefix(kv[0], key+"_") { continue }
  if kv[0] != key+"_us" {
   panic(op + ": key " + kv[0] + " does not declare microseconds; spell it " + key + "_us")
  }
  return num(kv[1]), true
 }
 return 0, false
}

// timeUsOrFail is timeUs for a key whose unit cannot be left implicit.
func timeUsOrFail(op, extra, key string) int64 {
 v, ok := timeUs(op, extra, key)
 if !ok { panic(op + ": missing required key " + key + "_us") }
 return v
}

// requireUsScale asserts that a Go duration built from a declared-microsecond
// stimulus value still reads back as exactly that many microseconds. A factor of
// a thousand anywhere in the conversion trips it.
func requireUsScale(op, what string, wantUs int64, got time.Duration) {
 if int64(got/time.Microsecond) != wantUs {
  panic(fmt.Sprintf("%s: %s unit scale drifted: stimulus %d us, Go %d us", op, what, wantUs, int64(got/time.Microsecond)))
 }
}

// requireKeys rejects any leaf key an op does not interpret, so a misspelled or
// stale-unit field can never be dropped silently.
func requireKeys(op, extra string, allowed ...string) {
 for _, item := range strings.Split(extra, ";") {
  if item == "" { continue }
  kv := strings.SplitN(item, "=", 2)
  known := false
  for _, a := range allowed {
   if a == kv[0] {
    known = true
    break
   }
  }
  if !known { panic("unknown " + op + " key " + kv[0]) }
 }
}

// config applies the case's key=value overrides to the pinned default config.
// An unknown key is a hard error, so a typo can never silently drop a leaf.
func config(spec string) SendSideBWEConfig {
 c := DefaultSendSideBWEConfig
 d := &c.CongestionDetector
 goldenSequenceNumberStart = uint64(1 << 15)
 for _, item := range strings.Split(spec, ";") {
  if item == "" { continue }
  kv := strings.SplitN(item, "=", 2)
  k, v := kv[0], kv[1]
  switch k {
  case "sn_start": goldenSequenceNumberStart = uint64(num(v))
  case "grp_min": d.PacketGroup.MinPackets = int(num(v))
  case "grp_win": d.PacketGroup.MaxWindowDuration = us(v)
  case "max_age": d.PacketGroupMaxAge = us(v)
  case "pgrp_min": d.ProbePacketGroup.PacketGroup.MinPackets = int(num(v))
  case "pgrp_win": d.ProbePacketGroup.PacketGroup.MaxWindowDuration = us(v)
  case "settle_rtt": d.ProbePacketGroup.SettleWaitNumRTT = uint32(num(v))
  case "settle_min": d.ProbePacketGroup.SettleWaitMin = us(v)
  case "settle_max": d.ProbePacketGroup.SettleWaitMax = us(v)
  case "wl_mindur": d.WeightedLoss.MinDurationForLossValidity = us(v)
  case "wl_base": d.WeightedLoss.BaseDuration = us(v)
  case "wl_pps": d.WeightedLoss.BasePPS = int(num(v))
  case "wl_pen": d.WeightedLoss.LossPenaltyFactor = flt(v)
  case "jqr_delay": d.JQRMinDelay = us(v)
  case "dqr_delay": d.DQRMaxDelay = us(v)
  case "jqr_coef": d.JQRMinTrendCoefficient = flt(v)
  case "sig_min_bytes": d.ProbeSignal.MinBytesRatio = flt(v)
  case "sig_min_dur": d.ProbeSignal.MinDurationRatio = flt(v)
  case "sig_jqr_delay": d.ProbeSignal.JQRMinDelay = us(v)
  case "sig_dqr_delay": d.ProbeSignal.DQRMaxDelay = us(v)
  case "sig_jqr_wl": d.ProbeSignal.JQRMinWeightedLoss = flt(v)
  case "sig_dqr_wl": d.ProbeSignal.DQRMaxWeightedLoss = flt(v)
  case "qdew_jqr_groups": d.QueuingDelayEarlyWarningJQR.MinNumberOfGroups = int(num(v))
  case "qdew_jqr_dur": d.QueuingDelayEarlyWarningJQR.MinDuration = us(v)
  case "qdew_dqr_groups": d.QueuingDelayEarlyWarningDQR.MinNumberOfGroups = int(num(v))
  case "qdew_dqr_dur": d.QueuingDelayEarlyWarningDQR.MinDuration = us(v)
  case "lew_jqr_groups": d.LossEarlyWarningJQR.MinNumberOfGroups = int(num(v))
  case "lew_jqr_dur": d.LossEarlyWarningJQR.MinDuration = us(v)
  case "lew_dqr_groups": d.LossEarlyWarningDQR.MinNumberOfGroups = int(num(v))
  case "lew_dqr_dur": d.LossEarlyWarningDQR.MinDuration = us(v)
  case "qdc_jqr_groups": d.QueuingDelayCongestedJQR.MinNumberOfGroups = int(num(v))
  case "qdc_jqr_dur": d.QueuingDelayCongestedJQR.MinDuration = us(v)
  case "qdc_dqr_groups": d.QueuingDelayCongestedDQR.MinNumberOfGroups = int(num(v))
  case "qdc_dqr_dur": d.QueuingDelayCongestedDQR.MinDuration = us(v)
  case "lc_jqr_groups": d.LossCongestedJQR.MinNumberOfGroups = int(num(v))
  case "lc_jqr_dur": d.LossCongestedJQR.MinDuration = us(v)
  case "lc_dqr_groups": d.LossCongestedDQR.MinNumberOfGroups = int(num(v))
  case "lc_dqr_dur": d.LossCongestedDQR.MinDuration = us(v)
  case "ctr_req": d.CongestedCTRTrend.RequiredSamples = int(num(v))
  case "ctr_req_min": d.CongestedCTRTrend.RequiredSamplesMin = int(num(v))
  case "ctr_th": d.CongestedCTRTrend.DownwardTrendThreshold = flt(v)
  case "ctr_wait": d.CongestedCTRTrend.DownwardTrendMaxWait = us(v)
  case "ctr_collapse": d.CongestedCTRTrend.CollapseThreshold = us(v)
  case "ctr_valid": d.CongestedCTRTrend.ValidityWindow = us(v)
  case "ctr_eps": d.CongestedCTREpsilon = flt(v)
  case "cg_min": d.CongestedPacketGroup.MinPackets = int(num(v))
  case "cg_win": d.CongestedPacketGroup.MaxWindowDuration = us(v)
  case "est_win": d.EstimationWindowDuration = us(v)
  case "pr_base": d.ProbeRegulator.BaseInterval = us(v)
  case "pr_min": d.ProbeRegulator.MinDuration = us(v)
  case "pr_backoff": d.ProbeRegulator.BackoffFactor = flt(v)
  case "pr_max_interval": d.ProbeRegulator.MaxInterval = us(v)
  case "pr_max_duration": d.ProbeRegulator.MaxDuration = us(v)
  case "pr_diff_factor": d.ProbeRegulator.DurationIncreaseFactor = flt(v)
  default: panic("unknown config key " + k)
  }
 }
 return c
}

// report builds a TWCC feedback report from the case's state spec. Every field is
// the pion wire representation, so the detector reads exactly what a parsed packet
// would give it.
func report(spec string, reports int) *rtcp.TransportLayerCC {
 r := &rtcp.TransportLayerCC{MediaSSRC: 1, SenderSSRC: 2, ReferenceTime: uint32(goldenUnixMicro() / 64000 & ((1 << 24) - 1)), FbPktCount: uint8(reports)}
 symSize := uint16(rtcp.TypeTCCSymbolSizeTwoBit)
 var symbols []uint16
 var deltas []*rtcp.RecvDelta
 for _, item := range strings.Split(spec, ";") {
  kv := strings.SplitN(item, "=", 2)
  k, v := kv[0], kv[1]
  switch k {
  case "base": r.BaseSequenceNumber = uint16(num(v))
  case "count": r.PacketStatusCount = uint16(num(v))
  case "ref": r.ReferenceTime = uint32(num(v))
  case "fb": r.FbPktCount = uint8(num(v))
  case "sym":
   if num(v) == 1 { symSize = rtcp.TypeTCCSymbolSizeOneBit }
  case "states":
   for _, st := range strings.Split(v, ",") {
    if st == "" { continue }
    rep := 1
    if parts := strings.SplitN(st, "#", 2); len(parts) == 2 { rep = int(num(parts[1])); st = parts[0] }
    parts := strings.SplitN(st, ":", 2)
    symbol := uint16(num(parts[0]))
    if len(parts) == 1 || parts[1] == "" {
     for i := 0; i < rep; i++ { symbols = append(symbols, symbol) }
     continue
    }
    delta := num(parts[1])
    typ := uint16(rtcp.TypeTCCPacketReceivedSmallDelta)
    if symbol == 2 { typ = rtcp.TypeTCCPacketReceivedLargeDelta }
    for i := 0; i < rep; i++ {
     symbols = append(symbols, symbol)
     deltas = append(deltas, &rtcp.RecvDelta{Type: typ, Delta: delta})
    }
   }
  default: panic("unknown report key " + k)
  }
 }
 if r.PacketStatusCount == 0 { r.PacketStatusCount = uint16(len(symbols)) }
 r.PacketChunks = append(r.PacketChunks, &rtcp.StatusVectorChunk{SymbolSize: symSize, SymbolList: symbols})
 r.RecvDeltas = deltas
 return r
}

func main() {
 file, e := os.Open(os.Args[1]); if e != nil { panic(e) }; defer file.Close()
 input := csv.NewReader(bufio.NewReader(file)); input.Comma = '\t'; input.Comment = '#'; input.FieldsPerRecord = -1
 rows, e := input.ReadAll(); if e != nil { panic(e) }
 origin := time.Unix(1000, 0)
 header := []string{"case", "at_us", "op", "sn", "state", "capacity", "changes", "committed", "can_probe",
  "probe_duration_us", "goal_reached", "num_groups", "fin_signal", "fin_capacity", "fin_ok", "rtt_s", "trk_sn", "trk_base_send_us",
  "trk_base_recv_us", "trk_probe_max_sn", "bstr_us", "bstr_ok", "recv_st_us", "recv_sd_us", "recv_rd_us", "recv_found", "trk_last_recv_sn", "twcc_reports", "twcc_ooo",
  "twcc_interval_us", "twcc_cycles", "twcc_high_ref_64ms", "reason", "switched_at_us", "qd_region", "qd_groups", "qd_jqr", "qd_dqr", "qd_min_idx",
  "qd_max_idx", "qd_sealed", "qd_coef", "loss_region", "loss_groups", "loss_min_idx", "loss_max_idx", "loss_jqr_sealed",
  "loss_dqr_sealed", "loss_wl", "loss_abr", "g_exists", "g_min_sn", "g_max_sn", "g_min_send_us", "g_max_send_us", "g_min_recv_us",
  "g_max_recv_us", "g_send_delta_us", "g_recv_delta_us", "g_inherited_us", "g_propagated_us", "g_pqd_us", "g_is_finalized", "g_pri_pkts",
  "g_pri_bytes", "g_rtx_pkts", "g_rtx_bytes", "g_prb_pkts", "g_prb_bytes", "g_lost_pkts", "g_lost_bytes", "g_wl", "g_abr",
  "g_ctr", "all_min_send_us", "all_max_send_us", "num_finalized", "pg_exists", "pg_max_sn", "pg_done_us", "pg_propagated_us",
  "pg_pri_pkts", "pg_pri_bytes", "pg_lost_pkts", "pg_lost_bytes", "pg_wl", "pg_abr", "pg_ctr", "pci_id", "pci_result_bytes",
  "pci_result_dur_us", "pci_desired_bps", "pci_duration_us", "settle_ok", "ctr_exists", "ctr_samples", "ctr_direction",
  "ctr_has_enough", "ctr_lowest", "ctr_highest", "est_ts_exists", "est_ts_abr",
  "l_traffic", "lt_duration_us", "lt_send_delta_us", "lt_recv_delta_us", "lt_acked_pkts", "lt_acked_bytes",
  "lt_lost_pkts", "lt_lost_bytes", "lt_weighted_loss", "lt_ctr", "lt_abr", "lt_loss_penalty",
  "l_group", "lg_err", "lg_finalized", "lg_min_send_us", "lg_max_send_us", "lg_min_recv_us", "lg_max_recv_us",
  "lg_propagated_us", "lg_pri_pkts", "lg_pri_bytes", "lg_rtx_pkts", "lg_rtx_bytes", "lg_send_delta_us", "lg_recv_delta_us",
  "lg_lost_pkts", "lg_lost_bytes", "lg_prb_pkts", "lg_prb_bytes", "lg_wl", "lg_abr", "lg_ctr",
  "lg_wl_raw", "lg_abr_raw", "lg_ctr_raw",
  "lg_lost_pri_pkts", "lg_lost_rtx_pkts", "lg_lost_prb_pkts",
  "l_trend", "lr_num_samples", "lr_direction", "lr_has_enough", "lr_lowest", "lr_highest",
  "lr_len", "lr_values", "lr_span_us", "pg_err",
  "pci_goal_available_bps", "pci_goal_expected_usage_bps", "pci_goal_desired_bytes",
  "pci_result_start_us", "pci_result_end_us", "pci_result_packets",
  "pci_result_primary_bytes", "pci_result_rtx_bytes", "pci_result_completed"}
 var s *SendSideBWE
 var obs *goldenObserver
 var lg *packetGroup
 var lr *TrendDetector[float64]
 gsel := -1
 previous := ""
 reports := 0
 fmt.Println(strings.Join(header, "\t"))
 for _, row := range rows {
  for len(row) < 9 { row = append(row, "") }
  name, spec, at := row[0], row[1], num(row[2])
  op := row[3]
  a, b2, c2, d2 := num(row[4]), num(row[5]), num(row[6]), num(row[7])
  extra := ""
  if len(row) > 8 { extra = row[8] }
  if name != previous {
   // Every case is its own scenario. currentGoldenTime is package level and
   // survives a case boundary, and the objects below are constructed before this
   // row's time is installed, so without this reset a case's detector is built at
   // whatever clock the PREVIOUS case's last row left behind. That is not a policy
   // property: it made columns such as switched_at_us and can_probe report the
   // order of the cases in the stimulus file, which is what a leaf baseline is
   // supposed to rule out. Constructing at this case's zero time is the only
   // instant that is the same for a case and its defaulted twins.
   currentGoldenTime = origin
   obs = &goldenObserver{}
   cfgP := config(spec)
   s = NewSendSideBWE(SendSideBWEParams{Config: cfgP, Logger: goldenLogger{}})
   s.SetBWEListener(obs)
   lg = newPacketGroup(
    packetGroupParams{
     Config:       cfgP.CongestionDetector.PacketGroup,
     WeightedLoss: cfgP.CongestionDetector.WeightedLoss,
     Logger:       goldenLogger{},
    },
    0,
   )
   lr = NewTrendDetector[float64](TrendDetectorParams{
    Name:   "leaf",
    Logger: goldenLogger{},
    Config: cfgP.CongestionDetector.CongestedCTRTrend,
   })
   previous = name
   reports = 0
   gsel = -1
  }
  currentGoldenTime = origin.Add(time.Duration(at) * time.Microsecond)
  sn := int64(-1)
  var bstr int64
  var bstrOk int
  var recvSt, recvSd, recvRd int64
  recvFnd := -1
  lastRecvSn := int64(-1)
  finSignal, finCapacity, finOk := -1, int64(0), 0
  settleOk := -1
  lgErr := -2
  pgErr := -2
  var ltHas int
  // lgHas/lrHas are per-row, like ltHas: the standalone group and its leaf trend
  // are echo columns of the gadd/lginh/tseed/tadd arms, so a row that ran none of
  // them must print the unarmed sentinels instead of the previous row's state.
  var lgHas, lrHas int
  var ltDur, ltSd, ltRd, ltApk, ltAby, ltLpk, ltLby, ltPen, ltAbr int64
  var ltWl, ltCtr float64
  switch op {
  case "send":
   sn = int64(s.RecordPacketSendAndGetSequenceNumber(goldenUnixMicro(), int(a), b2 == 1, ProbeClusterId(c2), d2 == 1))
  case "msend":
   // a = first ordinal, b = count, c = cadence in microseconds, d = packet size
   size := int(d2)
   if size == 0 { size = 1200 }
   cluster := ProbeClusterId(leafKey(extra, "cluster"))
   rtx := leafKey(extra, "rtx") == 1
   probe := leafKey(extra, "probe") == 1
   for i := int(a); i < int(a)+int(b2); i++ {
    sn = int64(s.RecordPacketSendAndGetSequenceNumber(goldenUnixMicro()+int64(i)*c2, size, rtx, cluster, probe))
   }
  case "twcc":
   reports++
   s.HandleTWCCFeedback(report(extra, reports))
  case "rtt":
   // The measured RTT is a float count of seconds in Go, so its stimulus unit has
   // to be declared: rtt_us microseconds, never the bare integer column a.
   if a != 0 { panic("rtt takes rtt_us= in the leaf keys, not column a") }
   requireKeys("rtt", extra, "rtt_us")
   s.UpdateRTT(float64(timeUsOrFail("rtt", extra, "rtt")) / 1e6)
  case "reset":
   s.Reset()
  case "pstart":
   // a = cluster id, b = desired bps, c = available bandwidth bps,
   // d = expected usage bps, extra bytes=<bytes>; dur_us=<microseconds>
   durUs := timeUsOrFail("pstart", extra, "dur")
   requireKeys("pstart", extra, "bytes", "dur_us")
   goal := ProbeClusterGoal{DesiredBps: int(b2), AvailableBandwidthBps: int(c2), ExpectedUsageBps: int(d2), DesiredBytes: int(leafKey(extra, "bytes")), Duration: time.Duration(durUs) * time.Microsecond}
   requireUsScale("pstart", "Goal.Duration", durUs, goal.Duration)
   s.ProbeClusterStarting(ProbeClusterInfo{Id: ProbeClusterId(a), CreatedAt: currentGoldenTime, Goal: goal})
  case "pdone":
   // a = cluster id, b = bytes probe; extra start_us/end_us are the probe
   // observer's stamps in microseconds (the Go fields themselves are nanoseconds),
   // packets/primary/rtx are counts and bytes, completed defaults to 1.
   startUs, hasStart := timeUs("pdone", extra, "start")
   if !hasStart { panic("pdone: missing required key start_us") }
   endUs := timeUsOrFail("pdone", extra, "end")
   requireKeys("pdone", extra, "start_us", "end_us", "packets", "primary", "rtx", "completed")
   result := ProbeClusterResult{BytesProbe: int(b2),
    StartTime: startUs * int64(time.Microsecond), EndTime: endUs * int64(time.Microsecond),
    PacketsProbe: int(leafKey(extra, "packets")),
    BytesNonProbePrimary: int(leafKey(extra, "primary")),
    BytesNonProbeRTX: int(leafKey(extra, "rtx"))}
   result.IsCompleted = leafKeyOr(extra, "completed", 1) == 1
   requireUsScale("pdone", "Result.Duration", endUs-startUs, result.Duration())
   s.ProbeClusterDone(ProbeClusterInfo{Id: ProbeClusterId(a), Result: result})
  case "pfin":
   signal, capacity, ok := s.ProbeClusterFinalize()
   finSignal, finCapacity, finOk = int(signal), capacity, bi(ok)
   settleOk = bi(ok)
  case "settle":
   // rtt_us is optional: absent means "do not touch the detector's RTT", so a
   // zero and a negative measurement stay expressible arms.
   if r, ok := timeUs("settle", extra, "rtt"); ok {
    s.congestionDetector.rtt = float64(r) / 1e6
   }
   requireKeys("settle", extra, "rtt_us")
   if s.probePacketGroup != nil {
    _, ok := s.probePacketGroup.MaybeFinalizeProbe(s.ProbeMaxSequenceNumber(), s.rtt)
    settleOk = bi(ok)
   }
  case "thresh":
   v, ok := s.BaseSendTimeThreshold(a)
   bstr, bstrOk = v, bi(ok)
  case "recv":
   recvFnd = bi(s.congestionDetector.packetTracker.getPacketInfoExisting(uint16(a)) != nil)
   pi, sd, rd := s.RecordPacketIndicationFromRemote(uint16(a), b2)
   recvSt, recvSd, recvRd = pi.sendTime, sd, rd

  case "traffic":
   wl := s.params.Config.CongestionDetector.WeightedLoss
   if leafKey(extra, "zero_wl") == 1 { wl = WeightedLossConfig{} }
   ltHas = 1
   ts := newTrafficStats(trafficStatsParams{Config: wl, Logger: goldenLogger{}})
   ts.Merge(&trafficStats{
    minSendTime: a, maxSendTime: b2, sendDelta: c2, recvDelta: d2,
    ackedPackets: int(leafKey(extra, "apk")), ackedBytes: int(leafKey(extra, "aby")),
    lostPackets: int(leafKey(extra, "lpk")), lostBytes: int(leafKey(extra, "lb")),
   })
   ltDur, ltSd, ltRd = ts.Duration(), ts.sendDelta, ts.recvDelta
   ltApk, ltAby, ltLpk, ltLby = int64(ts.ackedPackets), int64(ts.ackedBytes), int64(ts.lostPackets), int64(ts.lostBytes)
   ltWl, ltCtr, ltAbr, ltPen = ts.WeightedLoss(), ts.CapturedTrafficRatio(), ts.AcknowledgedBitrate(), ts.lossPenalty()
  case "gadd":
   lgHas = 1
   pi := &packetInfo{
    sequenceNumber: uint64(a), sendTime: b2, recvTime: c2,
    size: uint16(leafKey(extra, "size")), probeClusterId: ProbeClusterId(leafKey(extra, "cluster")),
    isRTX: leafKey(extra, "rtx") == 1, isProbe: leafKey(extra, "probe") == 1,
   }
   err := lg.Add(pi, d2, leafKey(extra, "rd"), leafKey(extra, "lost") == 1)
   lgErr = 0
   if err == errGroupFinalized { lgErr = 1 } else if err == errOldPacket { lgErr = 2 } else if err != nil { lgErr = 3 }
  case "tseed":
   lrHas = 1
   // the sample value is a float, so it lives in the free-text column rather than
   // the integer argument columns that every op shares.
   lr.Seed(flt(extra))
  case "tadd":
   lrHas = 1
   // upstream stamps samples with the clock, which the adapter routes to the
   // supplied per-row time, so the sample gap is the gap between the rows
   lr.AddValue(flt(extra))
  case "pgadd":
   // a = sequence number, b = send time, c = receive time, d = send delta;
   // extra = size/cluster/rd/lost/rtx/probe. This drives the detector's live
   // probe group directly, so an out-of-order cluster packet is expressible
   // even though the tracker itself only assigns increasing sequences.
   if s.congestionDetector.probePacketGroup != nil {
    pi := &packetInfo{
     sequenceNumber: uint64(a), sendTime: b2, recvTime: c2,
     size: uint16(leafKey(extra, "size")), probeClusterId: ProbeClusterId(leafKey(extra, "cluster")),
     isRTX: leafKey(extra, "rtx") == 1, isProbe: leafKey(extra, "probe") == 1,
    }
    err := s.congestionDetector.probePacketGroup.Add(pi, d2, leafKey(extra, "rd"), leafKey(extra, "lost") == 1)
    pgErr = 0
    if err == errGroupFinalized { pgErr = 1 } else if err == errOldPacket { pgErr = 2 } else if err != nil { pgErr = 3 }
   }
  case "lginh":
   // Recreate the standalone group with an inherited queuing delay, which is how
   // the detector seeds a group with the queue its predecessor already built.
   lg = newPacketGroup(
    packetGroupParams{
     Config:       s.params.Config.CongestionDetector.PacketGroup,
     WeightedLoss: s.params.Config.CongestionDetector.WeightedLoss,
     Logger:       goldenLogger{},
    },
    a,
   )
   lgHas = 1
 case "gsel":
   gsel = int(a)
  case "poll":
  default:
   panic(op)
  }

  c := s.congestionDetector
  if c.packetTracker.piLastRecv != nil {
   lastRecvSn = int64(c.packetTracker.piLastRecv.sequenceNumber)
  }
  state, capacity := int(c.CongestionState()), c.estimatedAvailableChannelCapacity
  row2 := []string{name, row[2], op, strconv.FormatInt(sn, 10), strconv.Itoa(state), strconv.FormatInt(capacity, 10),
   strconv.Itoa(obs.changes), strconv.FormatInt(obs.committed, 10), strconv.Itoa(bi(s.CanProbe())),
   strconv.FormatInt(int64(s.ProbeDuration()/time.Microsecond), 10), strconv.Itoa(bi(s.ProbeClusterIsGoalReached())),
   strconv.Itoa(len(c.packetGroups)), strconv.Itoa(finSignal), strconv.FormatInt(finCapacity, 10), strconv.Itoa(finOk),
   f(c.rtt), strconv.FormatUint(c.packetTracker.sequenceNumber, 10), strconv.FormatInt(c.packetTracker.baseSendTime-origin.UnixMicro(), 10),
   strconv.FormatInt(c.packetTracker.baseRecvTime-origin.UnixMicro(), 10), strconv.FormatUint(c.packetTracker.probeMaxSequenceNumber, 10),
   strconv.FormatInt(bstr, 10), strconv.Itoa(bstrOk), strconv.FormatInt(recvSt, 10), strconv.FormatInt(recvSd, 10),
   strconv.FormatInt(recvRd, 10), strconv.Itoa(recvFnd), strconv.FormatInt(lastRecvSn, 10), strconv.Itoa(c.twccFeedback.numReports), strconv.Itoa(c.twccFeedback.numReportsOutOfOrder),
   strconv.FormatInt(int64(c.twccFeedback.estimatedFeedbackInterval/time.Microsecond), 10),
   strconv.FormatInt(c.twccFeedback.cycles, 10), strconv.FormatUint(uint64(c.twccFeedback.highestReferenceTime), 10),
   strconv.Itoa(int(c.congestionReason)), strconv.FormatInt(int64(c.congestionStateSwitchedAt.Sub(origin)/time.Microsecond), 10)}
  if c.qdMeasurement == nil {
   row2 = append(row2, "-2", "0", "0", "0", "0", "0", "0", "0")
  } else {
   qmin, qmax := c.qdMeasurement.GroupRange()
   row2 = append(row2, strconv.Itoa(int(c.qdMeasurement.QueuingRegion())), strconv.Itoa(c.qdMeasurement.numGroups),
    strconv.Itoa(c.qdMeasurement.numJQRGroups), strconv.Itoa(c.qdMeasurement.numDQRGroups), strconv.Itoa(qmin),
    strconv.Itoa(qmax), strconv.Itoa(bi(c.qdMeasurement.IsSealed())), f(c.qdMeasurement.trendCoefficient()))
  }
  if c.lossMeasurement == nil {
   row2 = append(row2, "-2", "0", "0", "0", "0", "0", "0", "0")
  } else {
   lmin, lmax := c.lossMeasurement.GroupRange()
   lts := c.lossMeasurement.ts
   row2 = append(row2, strconv.Itoa(int(c.lossMeasurement.QueuingRegion())), strconv.Itoa(c.lossMeasurement.numGroups),
    strconv.Itoa(lmin), strconv.Itoa(lmax), strconv.Itoa(bi(c.lossMeasurement.isJQRSealed)),
    strconv.Itoa(bi(c.lossMeasurement.isDQRSealed)), f(c.lossMeasurement.weightedLoss), strconv.FormatInt(lts.AcknowledgedBitrate(), 10))
  }
  gi := len(c.packetGroups) - 1
  if gsel >= 0 { gi = gsel }
  if gi >= 0 && gi < len(c.packetGroups) {
   pg := c.packetGroups[gi]
   pqd, pqdOk := pg.FinalizedPropagatedQueuingDelay()
   if !pqdOk { pqd = -1 }
   ts := pg.Traffic()
   row2 = append(row2, "1", strconv.FormatUint(pg.minSequenceNumber, 10), strconv.FormatUint(pg.maxSequenceNumber, 10),
    strconv.FormatInt(pg.minSendTime, 10), strconv.FormatInt(pg.maxSendTime, 10), strconv.FormatInt(pg.minRecvTime, 10),
    strconv.FormatInt(pg.maxRecvTime, 10), strconv.FormatInt(pg.aggregateSendDelta, 10),
    strconv.FormatInt(pg.aggregateRecvDelta, 10), strconv.FormatInt(pg.inheritedQueuingDelay, 10),
    strconv.FormatInt(pg.PropagatedQueuingDelay(), 10), strconv.FormatInt(pqd, 10), strconv.Itoa(bi(pg.IsFinalized())),
    strconv.Itoa(pg.acked.primary.numPackets), strconv.Itoa(pg.acked.primary.numBytes), strconv.Itoa(pg.acked.rtx.numPackets),
    strconv.Itoa(pg.acked.rtx.numBytes), strconv.Itoa(pg.acked.probe.numPackets), strconv.Itoa(pg.acked.probe.numBytes),
    strconv.Itoa(pg.lost.numPackets()), strconv.Itoa(pg.lost.numBytes()), f(ts.WeightedLoss()),
    strconv.FormatInt(ts.AcknowledgedBitrate(), 10), f(ts.CapturedTrafficRatio()))
  } else {
   row2 = append(row2, "0", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "0",
    "-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "0", "0", "0")
  }
  allMin, allMax, numFinalized := int64(0), int64(0), 0
  for idx, pg := range c.packetGroups {
   if idx == 0 || pg.minSendTime < allMin { allMin = pg.minSendTime }
   if pg.maxSendTime > allMax { allMax = pg.maxSendTime }
   if pg.IsFinalized() { numFinalized++ }
  }
  row2 = append(row2, strconv.FormatInt(allMin, 10), strconv.FormatInt(allMax, 10), strconv.Itoa(numFinalized))
  if c.probePacketGroup == nil {
   row2 = append(row2, "0", "-1", "-1", "-1", "-1", "-1", "-1", "-1", "0", "0", "0", "-1", "-1", "-1", "-1", "-1", strconv.Itoa(settleOk))
  } else {
   ppg := c.probePacketGroup
   pts := ppg.Traffic()
   doneUs := int64(0)
   if !ppg.doneAt.IsZero() { doneUs = ppg.doneAt.UnixMicro() - origin.UnixMicro() }
   pci := ppg.ProbeClusterInfo()
   row2 = append(row2, "1", strconv.FormatUint(ppg.maxSequenceNumber, 10), strconv.FormatInt(doneUs, 10),
    strconv.FormatInt(ppg.PropagatedQueuingDelay(), 10), strconv.Itoa(ppg.acked.primary.numPackets),
    strconv.Itoa(ppg.acked.primary.numBytes), strconv.Itoa(ppg.lost.numPackets()), strconv.Itoa(ppg.lost.numBytes()),
    f(pts.WeightedLoss()), strconv.FormatInt(pts.AcknowledgedBitrate(), 10), f(pts.CapturedTrafficRatio()), strconv.FormatUint(uint64(pci.Id), 10),
    strconv.Itoa(pci.Result.Bytes()), strconv.FormatInt(int64(pci.Result.Duration()/time.Microsecond), 10),
    strconv.Itoa(pci.Goal.DesiredBps), strconv.FormatInt(int64(pci.Goal.Duration/time.Microsecond), 10), strconv.Itoa(settleOk))
  }
  if c.congestedCTRTrend == nil {
   row2 = append(row2, "0", "-1", "-1", "0", "0", "0")
  } else {
   row2 = append(row2, "1", strconv.Itoa(c.congestedCTRTrend.numSamples), strconv.Itoa(int(c.congestedCTRTrend.GetDirection())),
    strconv.Itoa(bi(c.congestedCTRTrend.HasEnoughSamples())), f(c.congestedCTRTrend.GetLowest()), f(c.congestedCTRTrend.GetHighest()))
  }
  if c.estimateTrafficStats == nil {
   row2 = append(row2, "0", "0")
  } else {
   row2 = append(row2, "1", strconv.FormatInt(c.estimateTrafficStats.AcknowledgedBitrate(), 10))
  }
  if ltHas == 0 {
   row2 = append(row2, "0", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2")
  } else {
   row2 = append(row2, "1", strconv.FormatInt(ltDur, 10), strconv.FormatInt(ltSd, 10), strconv.FormatInt(ltRd, 10),
    strconv.FormatInt(ltApk, 10), strconv.FormatInt(ltAby, 10), strconv.FormatInt(ltLpk, 10), strconv.FormatInt(ltLby, 10),
    f(ltWl), f(ltCtr), strconv.FormatInt(ltAbr, 10), strconv.FormatInt(ltPen, 10))
  }
  if lgHas == 0 {
   row2 = append(row2, "0", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2", "-2")
  } else {
   ltr := newTrafficStats(trafficStatsParams{Config: lg.params.WeightedLoss, Logger: goldenLogger{}})
   ltr.Merge(lg.Traffic())
   lraw := lg.Traffic()
   row2 = append(row2, "1", strconv.Itoa(lgErr), strconv.Itoa(bi(lg.IsFinalized())),
    strconv.FormatInt(lg.minSendTime, 10), strconv.FormatInt(lg.maxSendTime, 10),
    strconv.FormatInt(lg.minRecvTime, 10), strconv.FormatInt(lg.maxRecvTime, 10),
    strconv.FormatInt(lg.PropagatedQueuingDelay(), 10), strconv.Itoa(lg.acked.primary.numPackets),
    strconv.Itoa(lg.acked.primary.numBytes), strconv.Itoa(lg.acked.rtx.numPackets), strconv.Itoa(lg.acked.rtx.numBytes),
    strconv.FormatInt(lg.aggregateSendDelta, 10), strconv.FormatInt(lg.aggregateRecvDelta, 10),
    strconv.Itoa(lg.lost.numPackets()), strconv.Itoa(lg.lost.numBytes()),
    strconv.Itoa(lg.acked.probe.numPackets), strconv.Itoa(lg.acked.probe.numBytes),
    f(ltr.WeightedLoss()), strconv.FormatInt(ltr.AcknowledgedBitrate(), 10), f(ltr.CapturedTrafficRatio()),
    f(lraw.WeightedLoss()), strconv.FormatInt(lraw.AcknowledgedBitrate(), 10), f(lraw.CapturedTrafficRatio()),
    strconv.Itoa(lg.lost.primary.numPackets), strconv.Itoa(lg.lost.rtx.numPackets), strconv.Itoa(lg.lost.probe.numPackets))
  }
  if lrHas == 0 {
   row2 = append(row2, "0", "-2", "-2", "-2", "-2", "-2", "-2", "", "-2")
  } else {
   vals := make([]string, 0, len(lr.samples))
   spanUs := int64(0)
   if len(lr.samples) != 0 {
    spanUs = int64(lr.samples[len(lr.samples)-1].at.Sub(lr.samples[0].at) / time.Microsecond)
    for _, sv := range lr.samples {
     vals = append(vals, f(sv.value))
    }
   }
   row2 = append(row2, "1", strconv.Itoa(lr.numSamples), strconv.Itoa(int(lr.GetDirection())),
    strconv.Itoa(bi(lr.HasEnoughSamples())), f(lr.GetLowest()), f(lr.GetHighest()),
    strconv.Itoa(len(lr.samples)), strings.Join(vals, ","), strconv.FormatInt(spanUs, 10))
  }
  row2 = append(row2, strconv.Itoa(pgErr))
  // Normalized stimulus echo: the probe cluster goal and result fields exactly as
  // the pinned Go types received them, in the declared stimulus unit. The stamps
  // are nanoseconds inside Go and microseconds here, so reading the echo back is
  // what proves the conversion round-tripped. -1 means no probe group exists.
  goalAvail, goalExpect, goalBytes := "-1", "-1", "-1"
  resStartUs, resEndUs, resPkts, resPrimary, resRtx, resCompleted := "-1", "-1", "-1", "-1", "-1", "-1"
  if c.probePacketGroup != nil {
   pci := c.probePacketGroup.ProbeClusterInfo()
   goalAvail = strconv.Itoa(pci.Goal.AvailableBandwidthBps)
   goalExpect = strconv.Itoa(pci.Goal.ExpectedUsageBps)
   goalBytes = strconv.Itoa(pci.Goal.DesiredBytes)
   resStartUs = strconv.FormatInt(pci.Result.StartTime/int64(time.Microsecond), 10)
   resEndUs = strconv.FormatInt(pci.Result.EndTime/int64(time.Microsecond), 10)
   resPkts = strconv.Itoa(pci.Result.PacketsProbe)
   resPrimary = strconv.Itoa(pci.Result.BytesNonProbePrimary)
   resRtx = strconv.Itoa(pci.Result.BytesNonProbeRTX)
   resCompleted = strconv.Itoa(bi(pci.Result.IsCompleted))
  }
  row2 = append(row2, goalAvail, goalExpect, goalBytes, resStartUs, resEndUs, resPkts, resPrimary, resRtx, resCompleted)
  if len(row2) != len(header) {
   panic(fmt.Sprintf("column drift: have %d want %d", len(row2), len(header)))
  }
  fmt.Println(strings.Join(row2, "\t"))
 }
}
'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, default=ROOT/'references/livekit-stable')
    parser.add_argument('--go', default='go')
    parser.add_argument('--operations', type=Path, default=HERE/'operations.tsv')
    parser.add_argument('--output', type=Path, default=HERE/'golden.tsv')
    parser.add_argument('--evidence', type=Path, required=True)
    args=parser.parse_args()
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=args.reference,text=True).strip()
    if commit!=COMMIT: raise SystemExit('reference commit mismatch')
    original_hashes={}; transformed_hashes={}
    with tempfile.TemporaryDirectory(prefix='livekit-rs-send-side-bwe-golden-') as temporary:
        work=Path(temporary)
        (work/'go.mod').write_text('module golden\n\ngo 1.26.0\n\nrequire (\n'
            '\tgithub.com/gammazero/deque v1.2.1\n\tgithub.com/livekit/protocol v1.52.2-0.20261012065025-9bb708cdb819\n'
            '\tgithub.com/pion/rtcp v1.2.19\n\tgo.uber.org/atomic v1.11.0\n)\n')
        for i,name in enumerate(FILES):
            raw=(args.reference/name).read_bytes();original_hashes[name]=hashlib.sha256(raw).hexdigest()
            code=transformed(raw.decode());transformed_hashes[name]=hashlib.sha256(code.encode()).hexdigest()
            (work/f'policy{i}.go').write_text(code)
        (work/'support.go').write_text(STUBS);(work/'main.go').write_text(MAIN)
        env={'HOME':'/Users/zoujiaqing','PATH':'/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin',
             'GOPATH':'/Users/zoujiaqing/go','GOPROXY':'off','GOFLAGS':'-mod=mod','GOCACHE':'/Users/zoujiaqing/Library/Caches/go-build'}
        run=subprocess.run([args.go,'run','.',str(args.operations)],cwd=work,text=True,capture_output=True,env=env)
        if run.returncode: raise SystemExit(run.stdout+run.stderr)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text('# Actual pinned Go policy output; generated independently of Rust.\n'+run.stdout)
    evidence={'status':'PASS',
        'status_means':'the pinned Go sources compiled and the campaign produced observations; '
            'this says nothing about the Rust port and is not a Rust differential',
        'rust_differential':False,
        'clock':'adapted: a supplied deterministic clock, so these rows are not an official SDK or launched-MAIN acceptance',
        'reference_commit':COMMIT,'original_sha256':original_hashes,
        'clock_adapted_sha256':transformed_hashes,'operations_sha256':hashlib.sha256(args.operations.read_bytes()).hexdigest(),
        'output_sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),'rows':len(run.stdout.splitlines()),
        'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'real_dependencies':['pkg/sfu/bwe contracts compiled from source','ccutils prober/trend detector/probe regulator/probe signal compiled from source',
            'github.com/pion/rtcp v1.2.19 TWCC representation and github.com/livekit/protocol/utils Bitmap are the real packages'],
        'transforms':['original policy bodies retained','package/import namespaces unified into one main package',
            'mono.Now/mono.UnixMicro/time.Now/time.Since replaced by the supplied test clock',
            'logger and zapcore boundary types stubbed; log output cannot reach the result stream',
            'packet tracker random transport-wide sequence number start pinned to 1<<15; the wrap arithmetic itself is original',
        'each case constructs its objects at its own zero time, so a row cannot depend on the case that preceded it in the stimulus file'],
        'scope':'Actual Go policy golden, not Rust execution or network/media acceptance.','temporary_directory_removed':True}
    args.evidence.parent.mkdir(parents=True,exist_ok=True);args.evidence.write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps(evidence,indent=2))
if __name__=='__main__':main()
