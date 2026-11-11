// Fixed protocol module oracle: run from references/protocol-stable, without
// modifying that checkout. The real public logger writes into a private buffer.
package main

import (
 "bytes"
 "encoding/json"
 "fmt"
 "os"
 "strings"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/protocol/logger/zaputil"
 "go.uber.org/zap/zapcore"
)

type row struct {
 ID string `json:"id"`
 Sample bool `json:"sample"`
 Initial int `json:"initial"`
 Interval int `json:"interval"`
 Seconds int `json:"seconds"`
 NamedInitial int `json:"named_initial"`
 NamedInterval int `json:"named_interval"`
 Room bool `json:"room"`
 Count int `json:"count"`
 Accepted []int `json:"accepted"`
}
func main() {
 rows := []row{
  {ID:"disabled", Count:7},
  {ID:"root-defaults", Sample:true, Count:125},
  {ID:"named-inert", Sample:true, NamedInitial:1, NamedInterval:1, Count:125},
  {ID:"root-item-values", Sample:true, Initial:1, Interval:2, Count:7},
  {ID:"root-negative-initial", Sample:true, Initial:-1, Count:7},
  {ID:"room-raw-zero", Seconds:1, Room:true, Count:7},
  {ID:"room-without-root", Seconds:1, Initial:1, Room:true, Count:7},
  {ID:"room-replaces-root", Sample:true, Seconds:1, Initial:1, Interval:2, Room:true, Count:7},
 }
 for _, r := range rows {
  conf := &logger.Config{JSON:true, Level:"info", Sample:r.Sample,
   SampleInitial:r.NamedInitial, SampleInterval:r.NamedInterval,
   ItemSampleInitial:r.Initial, ItemSampleInterval:r.Interval, ItemSampleSeconds:r.Seconds}
  var sink bytes.Buffer
  l, err := logger.NewZapLogger(conf); if err != nil {panic(err)}
  var target logger.Logger = l.WithComponentLeveler(zaputil.NewRootComponentLeveler(zapcore.AddSync(&sink), conf))
  if r.Room {target=target.WithItemSampler()}
  for i:=1;i<=r.Count;i++ {target.Infow("same", "i", i)}
  r.Accepted=[]int{}
  for _, line := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
   if line=="" {continue}
   var event struct{ I int `json:"i"` }; if err=json.Unmarshal([]byte(line), &event);err!=nil {panic(err)}
   r.Accepted=append(r.Accepted,event.I)
  }
  data,err:=json.Marshal(r);if err!=nil {panic(err)};fmt.Fprintln(os.Stdout,string(data))
 }
}
