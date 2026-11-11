package streamallocator

import (
 "encoding/json"
 "os"
 "testing"
 "github.com/livekit/livekit-server/pkg/sfu"
 "github.com/livekit/protocol/livekit"
)

// Exercise the actual fixed Go method and Track.IsManaged, not a copied predicate.
func TestOriginalUnmanagedBWEObserver(t *testing.T) {
 type row struct { Name string `json:"name"`; Disable bool `json:"disable"`; Source int32 `json:"source"`; Multilayer bool `json:"multilayer"`; Known bool `json:"known"`; BWE bool `json:"bwe"` }
 cases := []struct{name string; disable bool; source livekit.TrackSource; layered,known,want bool}{
  {"default-unmanaged",false,livekit.TrackSource_SCREEN_SHARE,false,true,true},
  {"disabled-unmanaged",true,livekit.TrackSource_SCREEN_SHARE,false,true,false},
  {"managed-camera",true,livekit.TrackSource_CAMERA,false,true,true},
  {"multilayer-share",true,livekit.TrackSource_SCREEN_SHARE,true,true,true},
  {"unknown-track",true,livekit.TrackSource_SCREEN_SHARE,false,false,true},
  {"microphone",true,livekit.TrackSource_MICROPHONE,false,true,true},
 }
 rows:=make([]row,0,len(cases))
 for _,c:=range cases {
  allocator:=&StreamAllocator{params:StreamAllocatorParams{Config:StreamAllocatorConfig{DisableEstimationUnmanagedTracks:c.disable}},videoTracks:make(map[livekit.TrackID]*Track)}
  down:=&sfu.DownTrack{}
  if c.known { allocator.videoTracks[livekit.TrackID(down.ID())]=&Track{source:c.source,isMultiLayered:c.layered} }
  got:=allocator.IsBWEEnabled(down)
  if got!=c.want {t.Fatalf("%s: actual IsBWEEnabled=%v want %v",c.name,got,c.want)}
  rows=append(rows,row{c.name,c.disable,int32(c.source),c.layered,c.known,got})
 }
 data,err:=json.MarshalIndent(struct{Rows []row `json:"rows"`}{rows},"","  ");if err!=nil{t.Fatal(err)}
 path:=os.Getenv("UNMANAGED_BWE_GO_OBSERVATION");if path==""{t.Fatal("observation path required")}
 if err=os.WriteFile(path,append(data,'\n'),0600);err!=nil{t.Fatal(err)}
}
