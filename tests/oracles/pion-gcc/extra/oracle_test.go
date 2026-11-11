// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Test-only observer. Calls actual private source methods; expected results are
// never calculated by a copied estimator. Uses the existing clock-only stager.
package gcc
import (
 "bufio"
 "encoding/json"
 "math"
 "os"
 "runtime"
 "testing"
 "time"
 "github.com/pion/interceptor"
 "github.com/pion/interceptor/internal/ntp"
 "github.com/pion/rtcp"
 "github.com/pion/rtp"
)
func TestAdditionalSourceObserver(t *testing.T) {
 input,err:=os.Open(os.Getenv("PION_GCC_EXTRA_INPUT"));if err!=nil{t.Fatal(err)};defer input.Close()
 output,err:=os.Create(os.Getenv("PION_GCC_EXTRA_OUTPUT"));if err!=nil{t.Fatal(err)};defer output.Close()
 enc:=json.NewEncoder(output);scan:=bufio.NewScanner(input)
 for scan.Scan(){
  var c struct{Name string `json:"name"`;Stage string `json:"stage"`;Events []struct{At int64 `json:"at_ns"`;Measurement int64 `json:"measurement_ns"`;Estimate int64 `json:"estimate_ns"`;Received int `json:"received"`;Departure int64 `json:"departure_ns"`;Reference int64 `json:"reference_ns"`;Offset uint16 `json:"offset"`}}
  if err=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)}
  k:=newKalman();ema:=&exponentialMovingAverage{};threshold:=newAdaptiveThreshold()
  for index,event:=range c.Events{
   sourceOracleClock=time.Unix(1000,event.At)
   row:=map[string]any{"case":c.Name,"stage":c.Stage,"index":index,"goos":runtime.GOOS,"goarch":runtime.GOARCH}
   switch c.Stage{
   case "kalman":
    row["estimate_ns"]=int64(k.updateEstimate(time.Duration(event.Measurement)))
    row["error_bits"]=math.Float64bits(k.estimateError);row["uncertainty_bits"]=math.Float64bits(k.measurementUncertainty);row["gain_bits"]=math.Float64bits(k.gain)
   case "ema":
    ema.update(float64(event.Received))
    row["mean_bits"]=math.Float64bits(ema.average);row["variance_bits"]=math.Float64bits(ema.variance);row["deviation_bits"]=math.Float64bits(ema.stdDeviation)
   case "threshold":
    usage,scaled,current:=threshold.compare(time.Duration(event.Estimate),0)
    row["usage"]=int(usage);row["scaled_ns"]=int64(scaled);row["threshold_ns"]=int64(current);row["next_threshold_ns"]=int64(threshold.thresh)
   case "rfc8888":
    bwe,e:=NewSendSideBWE(SendSideBWEInitialBitrate(1_000_000));if e!=nil{t.Fatal(e)}
    header:=&rtp.Header{Version:2,SequenceNumber:7,SSRC:1}
    if e=bwe.feedbackAdapter.OnSent(time.Unix(1000,event.Departure),header,123,interceptor.Attributes{});e!=nil{t.Fatal(e)}
    feedback:=&rtcp.CCFeedbackReport{ReportTimestamp:ntp.ToNTP32(time.Unix(1000,event.Reference)),ReportBlocks:[]rtcp.CCFeedbackReportBlock{{MediaSSRC:1,BeginSequence:7,MetricBlocks:[]rtcp.CCFeedbackMetricBlock{{Received:true,ArrivalTimeOffset:event.Offset}}}}}
    if e=bwe.WriteRTCP([]rtcp.Packet{feedback},interceptor.Attributes{});e!=nil{t.Fatal(e)}
    bwe.delayController.rateController.lock.Lock();row["rtt_ns"]=int64(bwe.delayController.rateController.latestRTT);bwe.delayController.rateController.lock.Unlock()
    row["report_timestamp"]=feedback.ReportTimestamp
    if e=bwe.Close();e!=nil{t.Fatal(e)}
   default:t.Fatalf("unknown extra source stage %s",c.Stage)
   }
   if err=enc.Encode(row);err!=nil{t.Fatal(err)}
  }
 }
 if err=scan.Err();err!=nil{t.Fatal(err)}
}
