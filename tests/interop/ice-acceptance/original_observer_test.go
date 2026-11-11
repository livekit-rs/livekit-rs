package ice

import (
    "encoding/json"
    "os"
    "testing"
    "time"
)

// Appended only in a private byte-verified copy of fixed ICE. Call the actual
// original config and selector; do not duplicate the nomination algorithm.
func TestOriginalAcceptanceMinWaitObserver(t *testing.T) {
    constructors := []func() (Candidate,error) {
        func() (Candidate,error) {return NewCandidateHost(&CandidateHostConfig{Network:"udp",Address:"127.0.0.1",Port:9000,Component:1})},
        func() (Candidate,error) {return NewCandidateServerReflexive(&CandidateServerReflexiveConfig{Network:"udp",Address:"127.0.0.1",Port:9000,Component:1})},
        func() (Candidate,error) {return NewCandidatePeerReflexive(&CandidatePeerReflexiveConfig{Network:"udp",Address:"127.0.0.1",Port:9000,Component:1})},
        func() (Candidate,error) {return NewCandidateRelay(&CandidateRelayConfig{Network:"udp",Address:"127.0.0.1",Port:9000,Component:1})},
    }
    type row struct {Profile string `json:"profile"`; Kind string `json:"kind"`; Wait int64 `json:"wait_ns"`; Before bool `json:"before"`; After bool `json:"after"`}
    observations := []row{}
    positive := 370*time.Millisecond
    profiles := []struct{name string; config AgentConfig; expected []time.Duration}{
        {"original-default",AgentConfig{},[]time.Duration{0,500*time.Millisecond,time.Second,2*time.Second}},
        {"explicit-positive",AgentConfig{HostAcceptanceMinWait:&positive,SrflxAcceptanceMinWait:&positive,PrflxAcceptanceMinWait:&positive,RelayAcceptanceMinWait:&positive},[]time.Duration{positive,positive,positive,positive}},
    }
    for _, profile := range profiles {
        agent := &Agent{}
        profile.config.initWithDefaults(agent)
        actual := []time.Duration{agent.hostAcceptanceMinWait,agent.srflxAcceptanceMinWait,agent.prflxAcceptanceMinWait,agent.relayAcceptanceMinWait}
        for i, construct := range constructors {
            candidate,err := construct();if err!=nil {t.Fatal(err)}
            if actual[i]!=profile.expected[i] {t.Fatalf("%s %d default got %s expected %s",profile.name,i,actual[i],profile.expected[i])}
            selector := &controllingSelector{agent:agent}
            // A 100ms margin around a controlled startTime isolates the source
            // comparison from scheduler jitter. Rust tests cover exact ns edges.
            selector.startTime = time.Now().Add(-actual[i]+100*time.Millisecond)
            before := selector.isNominatable(candidate)
            selector.startTime = time.Now().Add(-actual[i]-100*time.Millisecond)
            after := selector.isNominatable(candidate)
            if before || !after {t.Fatalf("%s/%s before=%v after=%v",profile.name,candidate.Type(),before,after)}
            selector.Start()
            if selector.nominatedPair!=nil {t.Fatal("Start must clear nomination")}
            observations=append(observations,row{profile.name,candidate.Type().String(),int64(actual[i]),before,after})
        }
    }
    output,err:=json.MarshalIndent(map[string]any{"status":"PASS_ORIGINAL_SOURCE_OBSERVER","rows":observations,"rows_count":len(observations),"scope":"actual fixed ICE defaults and controllingSelector eligibility; no media/SDK/wire claim"},"","  ");if err!=nil {t.Fatal(err)}
    if err=os.WriteFile(os.Getenv("ICE_ACCEPTANCE_GO_OBSERVATION"),append(output,'\n'),0600);err!=nil {t.Fatal(err)}
}
