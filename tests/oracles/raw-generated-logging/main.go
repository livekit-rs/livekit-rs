// Development oracle: actual fixed GenerateCLIFlags -> NewConfig -> Zap/Pion logger.
// No copy of Rust parsing or authored expected output is used.
package main
import (
 "context"
 "fmt"
 "os"
 "github.com/livekit/livekit-server/pkg/config"
 "github.com/livekit/protocol/logger"
 "github.com/livekit/protocol/logger/pionlogger"
 "github.com/urfave/cli/v3"
)
func emit(conf *logger.Config) error {
 l,e:=logger.NewZapLogger(conf);if e!=nil{return e}
 l.Infow("F137:root-info");l.Errorw("F137:root-error",nil)
 p:=pionlogger.NewLoggerFactory(l.WithComponent("transport")).NewLogger("ice")
 p.Debug("F137:ice-debug");p.Trace("F137:ice-trace")
 d:=pionlogger.NewLoggerFactory(l.WithComponent("transport")).NewLogger("dtls")
 d.Debug("F137:dtls-debug-log");d.Error("F137:dtls-error-log")
 return nil
}
func run() int {
 base:=[]cli.Flag{&cli.StringFlag{Name:"config"}}
 flags,e:=config.GenerateCLIFlags(base,false);if e!=nil{fmt.Fprintln(os.Stderr,e);return 1}
 cmd:=&cli.Command{Name:"source-raw-logging",Flags:append(base,flags...),Action:func(ctx context.Context,c *cli.Command)error{
  b,e:=os.ReadFile(c.String("config"));if e!=nil{return e}
  conf,e:=config.NewConfig(string(b),true,c,base);if e!=nil{return e}
  return emit(&conf.Logging.Config)
 }}
 if e=cmd.Run(context.Background(),os.Args);e!=nil{fmt.Fprintln(os.Stderr,e);return 1};return 0
}
func main(){os.Exit(run())}
