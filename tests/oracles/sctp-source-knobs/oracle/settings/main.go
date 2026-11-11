package main
import("bufio";"encoding/json";"os";"log";"reflect";rtcconfig "github.com/livekit/mediatransportutil/pkg/rtcconfig")
func main(){if len(os.Args)!=2{log.Fatal("cases path required")};f,e:=os.Open(os.Args[1]);if e!=nil{log.Fatal(e)};defer f.Close();enc:=json.NewEncoder(os.Stdout);scan:=bufio.NewScanner(f)
 for scan.Scan(){var c struct{ID string `json:"id"`;Min int64 `json:"min"`;Fast int64 `json:"fast"`;Step int64 `json:"step"`};if e=json.Unmarshal(scan.Bytes(),&c);e!=nil{log.Fatal(e)}
 cfg:=rtcconfig.RTCConfig{SCTPMinCwnd:int(c.Min),SCTPFastRtxWnd:int(c.Fast),SCTPCwndCAStep:int(c.Step),ICEPortRangeStart:50000,ICEPortRangeEnd:60000,NodeIP:rtcconfig.NodeIP{V4:"127.0.0.1"}}
 actual,e:=rtcconfig.NewWebRTCConfig(&cfg,true);if e!=nil{log.Fatal(e)}
 v:=reflect.ValueOf(actual.SettingEngine).FieldByName("sctp")
 if e=enc.Encode(map[string]any{"id":c.ID,"min":v.FieldByName("minCwnd").Uint(),"fast":v.FieldByName("fastRtxWnd").Uint(),"step":v.FieldByName("cwndCAStep").Uint(),"advertised_max_message_size":v.FieldByName("maxMessageSize").Uint()});e!=nil{log.Fatal(e)}
 };if e=scan.Err();e!=nil{log.Fatal(e)}}
