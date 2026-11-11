// Independent inventory calls the exact exported fixed-Go generator. The
// 16 existing flag names only suppress duplicated base names as main.go does.
package main
import (
 "encoding/json"
 "fmt"
 "os"
 "sort"
 "github.com/livekit/livekit-server/pkg/config"
 "github.com/urfave/cli/v3"
)
func main() {
 base:=[]string{"bind","config","config-body","key-file","keys","region","node-ip","udp-port","redis-host","redis-password","turn-cert","turn-key","cpuprofile","memprofile","dev","disable-strict-config"}
 flags:=make([]cli.Flag,0,len(base));for _,name:=range base {flags=append(flags,&cli.StringFlag{Name:name})}
 generated,e:=config.GenerateCLIFlags(flags,false);if e!=nil {panic(e)}
 rows:=make([]map[string]any,0,len(generated));for _,f:=range generated {names:=f.Names();if len(names)!=1 {panic("unexpected generated alias; retain and review source")};rows=append(rows,map[string]any{"name":names[0],"names":names,"type":fmt.Sprintf("%T",f)})}
 sort.Slice(rows,func(i,j int)bool{return rows[i]["name"].(string)<rows[j]["name"].(string)})
 if e=json.NewEncoder(os.Stdout).Encode(map[string]any{"source_commit":"e5655ac20267fa5005dccaf3f0e3bfc3ea688c07","flags":rows});e!=nil {panic(e)}
}
