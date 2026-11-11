package main

import (
    "encoding/json"
    "math"
    "os"

    "github.com/livekit/livekit-server/pkg/sfu/audio"
)

func main() {
    type observation struct {
        Interval uint32 `json:"interval_ms"`
        Smooth uint32 `json:"smooth_intervals"`
        Stage string `json:"stage"`
        Level float64 `json:"level"`
        Active bool `json:"active"`
    }
    rows:=[]observation{}
    for _, profile:=range []struct{interval,smooth uint32}{{0,2},{10,2},{40,2},{10,0}} {
        detector:=audio.NewAudioLevel(audio.AudioLevelParams{ClockRate:48000})
        detector.SetConfig(audio.AudioLevelConfig{ActiveLevel:35,MinPercentile:40,UpdateInterval:profile.interval,SmoothIntervals:profile.smooth})
        now:=int64(10_000_000_000)
        record:=func(stage string,at int64,expected float64,active bool){
            level,actualActive:=detector.GetLevel(at)
            if math.Abs(level-expected)>1e-9 || actualActive!=active {panic("original audio source mismatch: "+stage)}
            rows=append(rows,observation{profile.interval,profile.smooth,stage,level,actualActive})
        }
        detector.ObserveWithRTPTimestamp(0,48000,now)
        record("first-rfc6464-sample-zero-duration",now,0,false)
        detector.ObserveWithRTPTimestamp(0,48960,now+20_000_000)
        expected:=0.0
        if profile.interval==10 {if profile.smooth==0 {expected=2} else {expected=4.0/3.0}}
        record("second-sample-after-real-20ms",now+20_000_000,expected,expected>0)
        // Duplicate/backwards RTP timestamps must not count another frame.
        detector.ObserveWithRTPTimestamp(0,48960,now+20_000_000)
        detector.ObserveWithRTPTimestamp(0,48000,now+20_000_000)
        record("duplicate-and-backwards-no-extra-window",now+20_000_000,expected,expected>0)
        if profile.interval==40 {
            detector.ObserveWithRTPTimestamp(0,49920,now+40_000_000)
            record("third-sample-completes-40ms",now+40_000_000,2.0/3.0,true)
            record("stale-after-two-windows",now+120_000_000,0,false)
        } else {
            record("stale-after-two-windows",now+20_000_000+int64(profile.interval)*2_000_000,0,false)
        }
    }
    data,err:=json.MarshalIndent(map[string]any{"status":"PASS_ORIGINAL_EXPORTED_AUDIO_CONSUMER","rows":rows,"row_count":len(rows),"scope":"Original exported NewAudioLevel/SetConfig/ObserveWithRTPTimestamp/GetLevel; no copied algorithm, media or SDK claim"},"","  ");if err!=nil {panic(err)}
    if err=os.WriteFile(os.Getenv("AUDIO_LEVEL_GO_OBSERVATION"),append(data,'\n'),0600);err!=nil {panic(err)}
}
