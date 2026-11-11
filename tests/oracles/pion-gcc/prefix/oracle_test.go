// SPDX-License-Identifier: MIT
// Actual Pion source receives real sent history and raw compound bytes. Test-only
// clock and pipeline barriers choose the runtime's declared rates-before-groups
// interleaving without replacing any arithmetic, parser, adapter or callback.
package gcc
import("bufio";"encoding/hex";"encoding/json";"os";"testing";"time";"github.com/pion/interceptor";"github.com/pion/interceptor/internal/cc";"github.com/pion/logging";"github.com/pion/rtcp";"github.com/pion/rtp")
var prefixClock time.Time
var prefixRateDone chan struct{}
var prefixGroupDone chan struct{}
func prefixNow()time.Time{return prefixClock}
func prefixSince(at time.Time)time.Duration{return prefixClock.Sub(at)}
type prefixStep struct{Kind string `json:"kind"`;AtNS int64 `json:"at_ns"`;Sequence uint16 `json:"sequence"`;SSRC uint32 `json:"ssrc"`;HeaderBytes int `json:"header_bytes"`;PayloadBytes int `json:"payload_bytes"`;TransportSequence uint16 `json:"transport_sequence"`;WireHex string `json:"wire_hex"`}
type prefixCase struct{Name string `json:"name"`;Steps []prefixStep `json:"steps"`;ChallengeIndex int `json:"challenge_index"`}
type prefixPacer struct{*NoOpPacer;changes int}
func(p *prefixPacer)SetTargetBitrate(int){p.changes++}
func TestStatefulPrefixWireSourceOracle(t *testing.T){
 f,err:=os.Open(os.Getenv("PION_PREFIX_INPUT"));if err!=nil{t.Fatal(err)};defer f.Close();o,err:=os.Create(os.Getenv("PION_PREFIX_OUTPUT"));if err!=nil{t.Fatal(err)};defer o.Close();enc:=json.NewEncoder(o);scan:=bufio.NewScanner(f);scan.Buffer(make([]byte,4096),4*1024*1024)
 for scan.Scan(){var c prefixCase;if err:=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)};prefixClock=time.Unix(1000,0);prefixRateDone=make(chan struct{});prefixGroupDone=make(chan struct{});ackCh:=make(chan []cc.Acknowledgment);rateCh:=make(chan []cc.Acknowledgment);done:=make(chan struct{},2);callbacks:=make(chan int,512);pacer:=&prefixPacer{NoOpPacer:NewNoOpPacer()};a:=cc.NewFeedbackAdapter();log:=logging.NewDefaultLoggerFactory();e:=&SendSideBWE{feedbackAdapter:a,lossController:newLossBasedBWE(1000000,log),pacer:pacer,latestBitrate:1000000,close:make(chan struct{})}
  rate:=newRateController(prefixNow,1000000,5000,50000000,e.onDelayUpdate);over:=newOveruseDetector(newAdaptiveThreshold(),10*time.Millisecond,rate.onDelayStats);slope:=newSlopeEstimator(newKalman(),over.onDelayStats);d:=&delayController{rateController:rate,ackPipe:ackCh,ackRatePipe:rateCh};e.delayController=d;e.OnTargetBitrateChange(func(v int){callbacks<-v})
  go func(){newArrivalGroupAccumulator().run(ackCh,slope.onArrivalGroup);done<-struct{}{}}();go func(){newRateCalculator(500*time.Millisecond).run(rateCh,rate.onReceivedRate);done<-struct{}{}}()
  for i,s:=range c.Steps{prefixClock=time.Unix(1000,s.AtNS);before:=pacer.changes;changes:=[]int{};parseError,processError:="","";if s.Kind=="sent"{h:=&rtp.Header{Version:2,SequenceNumber:s.Sequence,SSRC:s.SSRC};ext:=rtp.TransportCCExtension{TransportSequence:s.TransportSequence};b,err:=ext.Marshal();if err!=nil{t.Fatal(err)};if err:=h.SetExtension(1,b);err!=nil{t.Fatal(err)};if h.MarshalSize()!=s.HeaderBytes{t.Fatalf("actual header size %d",h.MarshalSize())};if err:=a.OnSent(prefixClock,h,s.PayloadBytes,interceptor.Attributes{cc.TwccExtensionAttributesKey:uint8(1)});err!=nil{t.Fatal(err)}}else{b,err:=hex.DecodeString(s.WireHex);if err!=nil{t.Fatal(err)};pkts,err:=rtcp.Unmarshal(b);if err!=nil{parseError=err.Error()}else if err:=e.WriteRTCP(pkts,nil);err!=nil{processError=err.Error()}}
   for n:=pacer.changes-before;n>0;n--{select{case v:=<-callbacks:changes=append(changes,v);case<-time.After(time.Second):t.Fatal("actual callback not delivered")}}
   if err:=enc.Encode(map[string]any{"case":c.Name,"index":i,"kind":s.Kind,"parse_error":parseError,"process_error":processError,"changes":changes,"target":e.GetTargetBitrate(),"rtt_ns":int64(rate.latestRTT)});err!=nil{t.Fatal(err)}
  };close(ackCh);close(rateCh);<-done;<-done
 };if err:=scan.Err();err!=nil{t.Fatal(err)}
}
