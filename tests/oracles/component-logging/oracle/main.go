// Source observer: invokes the fixed upstream resolver and optional real loggers.
package main
import (
 "encoding/json"
 "os"
 "fmt"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/livekit-server/pkg/config"
 "github.com/livekit/protocol/logger/pionlogger"
)
type Case struct { ID string `json:"id"`; Level string `json:"level"`; Components map[string]string `json:"components"`; Component string `json:"component"` }
func emit(conf *logger.Config) {
 l,e:=logger.NewZapLogger(conf);if e!=nil{panic(e)}
 l.Infow("F137:root-info"); l.Errorw("F137:root-error",nil)
 p:=pionlogger.NewLoggerFactory(l.WithComponent("transport")).NewLogger("ice")
 p.Debug("F137:ice-debug");p.Trace("F137:ice-trace")
 d:=pionlogger.NewLoggerFactory(l.WithComponent("transport")).NewLogger("dtls")
 d.Debug("F137:dtls-debug-log");d.Error("F137:dtls-error-log")
}
func main() {
 if os.Args[1]=="process" {
  body,e:=os.ReadFile(os.Args[2]);if e!=nil{panic(e)}
  cfg,e:=config.NewConfig(string(body),true,nil,nil);if e!=nil{panic(e)}
  emit(&cfg.Logging.Config);return
 }
 f,err:=os.Open(os.Args[1]); if err!=nil {panic(err)}; defer f.Close()
 var cases []Case; if err=json.NewDecoder(f).Decode(&cases);err!=nil{panic(err)}
 for _,c:=range cases {
  conf:=&logger.Config{JSON:true,Level:c.Level,ComponentLevels:c.Components}
  resolved,_:=conf.ResolveComponentLevel(c.Component)
  if err=json.NewEncoder(os.Stdout).Encode(struct{Case;Threshold int8 `json:"threshold"`}{c,int8(resolved)});err!=nil{panic(err)}
  if len(os.Args)>2 && os.Args[2]=="emit" {
   l,e:=logger.NewZapLogger(conf);if e!=nil{panic(e)}
   l.Infow(fmt.Sprintf("F137:%s:root-info",c.ID))
   l.Errorw(fmt.Sprintf("F137:%s:root-error",c.ID),nil)
   p:=pionlogger.NewLoggerFactory(l.WithComponent("transport")).NewLogger("ice")
   p.Debug(fmt.Sprintf("F137:%s:ice-debug",c.ID));p.Trace(fmt.Sprintf("F137:%s:ice-trace",c.ID))
  }
 }
}
