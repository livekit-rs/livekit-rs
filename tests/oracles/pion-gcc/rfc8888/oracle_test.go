// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Actual source adapter, RTCP marshal/unmarshal and SendSideBWE.WriteRTCP.
package gcc
import("bufio";"encoding/hex";"encoding/json";"os";"testing";"time";"github.com/pion/interceptor/internal/cc";"github.com/pion/interceptor/internal/ntp";"github.com/pion/logging";"github.com/pion/rtcp";"github.com/pion/rtp")
var rfcSourceClock time.Time
func rfcSourceNow()time.Time{return rfcSourceClock}
type rfcSourceCase struct{Name string `json:"name"`;ReportTimestamp uint32 `json:"report_timestamp"`;Base uint16 `json:"base"`;SSRC uint32 `json:"ssrc"`;Sent []struct{Seq uint16 `json:"seq"`;BeforeNowNS int64 `json:"before_now_ns"`;PayloadSize int `json:"payload_size"`} `json:"sent"`;Metrics []struct{Received bool `json:"received"`;Offset uint16 `json:"offset"`;ECN uint8 `json:"ecn"`} `json:"metrics"`}
func TestRFC8888ReferenceSourceOracle(t *testing.T){
 input,output:=os.Getenv("PION_RFC8888_INPUT"),os.Getenv("PION_RFC8888_OUTPUT");if input==""||output==""{t.Fatal("explicit owned paths required")}
 f,err:=os.Open(input);if err!=nil{t.Fatal(err)};defer f.Close();o,err:=os.Create(output);if err!=nil{t.Fatal(err)};defer o.Close();scan:=bufio.NewScanner(f);enc:=json.NewEncoder(o)
 for scan.Scan(){var c rfcSourceCase;if err:=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)}
  ref:=ntp.ToTime(uint64(c.ReportTimestamp)<<16);rfcSourceClock=ref.Add(time.Second)
  a:=cc.NewFeedbackAdapter();sent:=[]map[string]any{}
  for _,s:=range c.Sent{h:=&rtp.Header{Version:2,SequenceNumber:s.Seq,SSRC:c.SSRC};departure:=rfcSourceClock.Add(-time.Duration(s.BeforeNowNS));if err:=a.OnSent(departure,h,s.PayloadSize,nil);err!=nil{t.Fatal(err)};sent=append(sent,map[string]any{"seq":s.Seq,"ssrc":c.SSRC,"size":s.PayloadSize,"departure_unix_ns":departure.UnixNano()})}
  metrics:=[]rtcp.CCFeedbackMetricBlock{};for _,m:=range c.Metrics{metrics=append(metrics,rtcp.CCFeedbackMetricBlock{Received:m.Received,ECN:rtcp.ECN(m.ECN),ArrivalTimeOffset:m.Offset})}
  report:=rtcp.CCFeedbackReport{SenderSSRC:55,ReportTimestamp:c.ReportTimestamp,ReportBlocks:[]rtcp.CCFeedbackReportBlock{{MediaSSRC:c.SSRC,BeginSequence:c.Base,MetricBlocks:metrics}}};wire,err:=report.Marshal();if err!=nil{t.Fatal(err)};var decoded rtcp.CCFeedbackReport;if err:=decoded.Unmarshal(wire);err!=nil{t.Fatal(err)}
  acks:=a.OnRFC8888Feedback(rfcSourceClock,&decoded);ackRows:=[]map[string]any{};for _,ack:=range acks{row:=map[string]any{"seq":ack.SequenceNumber,"ssrc":ack.SSRC,"size":ack.Size,"departure_unix_ns":ack.Departure.UnixNano(),"arrival_unix_ns":nil,"ecn":ack.ECN};if !ack.Arrival.IsZero(){row["arrival_unix_ns"]=ack.Arrival.UnixNano()};ackRows=append(ackRows,row)}
  rate:=newRateController(rfcSourceNow,1000000,5000,50000000,func(DelayStats){});rate.latestRTT=987654321
  // Buffered source controller channels capture the actual synchronous WriteRTCP
  // submissions; no asynchronous estimator trajectory is claimed by this oracle.
  d:=&delayController{rateController:rate,ackPipe:make(chan []cc.Acknowledgment,1),ackRatePipe:make(chan []cc.Acknowledgment,1)}
  e:=&SendSideBWE{feedbackAdapter:a,delayController:d,lossController:newLossBasedBWE(1000000,logging.NewDefaultLoggerFactory()),close:make(chan struct{})}
  if err:=e.WriteRTCP([]rtcp.Packet{&decoded},nil);err!=nil{t.Fatal(err)}
  if err:=enc.Encode(map[string]any{"case":c.Name,"wire_hex":hex.EncodeToString(wire),"report_unix_ns":ref.UnixNano(),"now_unix_ns":rfcSourceClock.UnixNano(),"actual_sent":sent,"acks":ackRows,"rtt_ns":rate.latestRTT});err!=nil{t.Fatal(err)}
 };if err:=scan.Err();err!=nil{t.Fatal(err)}
}
