package main
import("encoding/json";"fmt";"github.com/livekit/livekit-server/pkg/rtc/signalling";"github.com/livekit/protocol/utils/xtwirp")
func main(){for _,c:=range []struct{id string;err error}{{"name",signalling.ErrNameExceedsLimits},{"metadata",signalling.ErrMetadataExceedsLimits},{"attributes",signalling.ErrAttributesExceedsLimits}}{e:=xtwirp.ToError(c.err);b,err:=json.Marshal(map[string]any{"id":c.id,"code":e.Code(),"msg":e.Msg()});if err!=nil{panic(err)};fmt.Println(string(b))}}
