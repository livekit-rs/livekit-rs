package sfu

import (
 "encoding/json"
 "os"
 "testing"
 "time"
 promclient "github.com/prometheus/client_golang/prometheus"
)

// Invoke fixed original summary, buffer and GetStats consumers. No copied algorithm.
func TestOriginalForwardStatsObserver(t *testing.T) {
 initPrometheus(t)
 type row struct{Name string `json:"name"`; Value int64 `json:"value"`}
 rows:=make([]row,0)
 add:=func(name string,value int64,want int64){if value!=want{t.Fatalf("%s actual=%d want=%d",name,value,want)};rows=append(rows,row{name,value})}
 var interpolated forwardSummary
 interpolated.addSample(1000000);interpolated.addSample(1010000)
 add("same-bucket-p90-interpolated",int64(interpolated.percentile(.9)),1009000)
 add("negative-bucket",int64(forwardBucket(-1)),0)
 add("last-bucket-clamp",int64(forwardBucket(134217728000)),215)
 s:=&ForwardStats{ring:make([]forwardSummary,3),summaryInterval:100*time.Millisecond}
 s.Update(0,5000000);s.Update(4000000,5000000);s.flush()
 add("individual-arrival-count",s.ring[0].count,2)
 add("individual-arrival-p90",int64(s.GetStats(0)),5000000)
 s.flush();s.flush();s.flush()
 add("empty-window-aged-p90",int64(s.GetStats(0)),0)
 window:=&ForwardStats{ring:make([]forwardSummary,5),summaryInterval:100*time.Millisecond}
 for i:=5;i>=1;i--{window.Update(0,int64(i)*1000000);window.flush()}
 add("window-full",int64(window.GetStats(0)),5000000)
 add("window-ceil-ns",int64(window.GetStats(time.Nanosecond)),1000000)
 add("window-200ms",int64(window.GetStats(200*time.Millisecond)),2000000)
 overflow:=&ForwardStats{ring:make([]forwardSummary,1)}
 for i:=0;i<forwardSampleShardCap+3;i++{overflow.Update(0,1000)}
 overflow.flush()
 add("overflow-kept",overflow.ring[0].count,int64(forwardSampleShardCap))
 add("overflow-dropped",int64(overflow.samples.takeDropped()),3)
 large:=&ForwardStats{ring:make([]forwardSummary,1)};large.Update(0,10000000000);large.flush();large.report()
 families,err:=promclient.DefaultGatherer.Gather();if err!=nil{t.Fatal(err)}
 found:=false
 for _,family:=range families {if family.GetName()=="livekit_forward_latency"{for _,metric:=range family.Metric{add("uint32-forward-latency-gauge",int64(metric.GetGauge().GetValue()),1410065408);found=true}}}
 if !found{t.Fatal("actual original forwarding latency gauge absent")}
 data,err:=json.MarshalIndent(struct{Rows []row `json:"rows"`}{rows},"","  ");if err!=nil{t.Fatal(err)}
 path:=os.Getenv("FORWARD_STATS_GO_OBSERVATION");if path==""{t.Fatal("observation path required")}
 if err=os.WriteFile(path,append(data,'\n'),0600);err!=nil{t.Fatal(err)}
}
