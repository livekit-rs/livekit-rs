package rtc

import (
    "encoding/json"
    "os"
    "testing"
    "time"

    "github.com/pion/rtcp"
    "github.com/livekit/protocol/livekit"
    "github.com/livekit/protocol/logger"
    "github.com/livekit/livekit-server/pkg/rtc/transport"
    "github.com/livekit/livekit-server/pkg/rtc/types"
)

type fallbackObserverHandler struct {transport.UnimplementedHandler; failures int; short bool}
func (h *fallbackObserverHandler) OnFailed(short bool,_ *types.ICEConnectionInfo) {h.failures++;h.short=short}

// Owned shell PCs expose original atomic preference setters and connection
// metadata to the ORIGINAL manager methods. No Pion algorithm is copied and no
// real route/media is claimed by this constructor/consumer observer.
func fallbackObserverManager(connection types.ICEConnectionType,threshold int,allow,unstable,turns bool,sdk livekit.ClientInfo_SDK,version string) (*TransportManager,*fallbackObserverHandler,*int) {
    handler:=&fallbackObserverHandler{}
    log:=logger.GetLogger()
    pc:=func() *PCTransport {return &PCTransport{connectionDetails:&types.ICEConnectionDetails{ICEConnectionInfo:types.ICEConnectionInfo{Type:connection}},iceFirstConnectedAt:time.Now().Add(-time.Second)}}
    manager:=&TransportManager{params:TransportManagerParams{AllowTCPFallback:allow,TCPFallbackRTTThreshold:threshold,AllowUDPUnstableFallback:unstable,TURNSEnabled:turns,ClientInfo:ClientInfo{ClientInfo:&livekit.ClientInfo{Sdk:sdk,Version:version}},Logger:log,PublisherHandler:handler,SubscriberHandler:handler},publisher:pc(),subscriber:pc(),iceConfig:&livekit.ICEConfig{},mediaLossProxy:NewMediaLossProxy(MediaLossProxyParams{Logger:log})}
    manager.signalSourceValid.Store(true);manager.UpdateLastSeenSignal()
    manager.mediaLossProxy.OnMediaLossUpdate(manager.onMediaLossUpdate)
    changed:=new(int);manager.OnICEConfigChanged(func(_ *livekit.ICEConfig){*changed = *changed + 1})
    return manager,handler,changed
}

func TestOriginalTCPFallbackConsumerObserver(t *testing.T) {
    type row struct {Name string `json:"name"`; Preference livekit.ICECandidateType `json:"preference"`; PublisherTCP bool `json:"publisher_tcp"`; SubscriberTCP bool `json:"subscriber_tcp"`; Changed int `json:"changed"`; Failed int `json:"failed"`}
    rows:=[]row{}
    record:=func(name string,m *TransportManager,h *fallbackObserverHandler,changed *int,expected livekit.ICECandidateType,count int){
        got:=m.GetICEConfig().PreferenceSubscriber
        if got!=expected || m.GetICEConfig().PreferencePublisher!=expected || *changed!=count {t.Fatalf("%s got=%s changes=%d expected=%s/%d",name,got,*changed,expected,count)}
        tcp:=expected==livekit.ICECandidateType_ICT_TCP
        if m.publisher.preferTCP.Load()!=tcp || m.subscriber.preferTCP.Load()!=tcp {t.Fatalf("%s both real original PC preference setters must agree",name)}
        rows=append(rows,row{name,got,m.publisher.preferTCP.Load(),m.subscriber.preferTCP.Load(),*changed,h.failures})
    }
    cases:=[]struct{name string;threshold int;rtt uint32;allow,turns,valid,stale,oneShot bool;sdk livekit.ClientInfo_SDK;version string;expected livekit.ICECandidateType}{
        {"default-threshold-zero",0,999,true,false,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_TCP},
        {"strict-rtt-before",50,49,true,false,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_TCP},
        {"strict-rtt-equal",50,50,true,false,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
        {"signed-negative-threshold",-1,0,true,false,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
        {"rtt-reject-turns",50,50,true,true,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_TLS},
        {"go-sdk-no-active-tcp",0,0,true,false,true,false,false,livekit.ClientInfo_GO,"",livekit.ICECandidateType_ICT_NONE},
        {"go-sdk-turns",0,0,true,true,true,false,false,livekit.ClientInfo_GO,"",livekit.ICECandidateType_ICT_TLS},
        {"swift-before",0,0,true,false,true,false,false,livekit.ClientInfo_SWIFT,"1.0.4",livekit.ICECandidateType_ICT_NONE},
        {"swift-boundary",0,0,true,false,true,false,false,livekit.ClientInfo_SWIFT,"1.0.5",livekit.ICECandidateType_ICT_TCP},
        {"disabled",0,0,false,true,true,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
        {"invalid-signal",0,0,true,true,false,false,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
        {"stale-signal",0,0,true,true,true,true,false,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
        {"one-shot",0,0,true,true,true,false,true,livekit.ClientInfo_JS,"",livekit.ICECandidateType_ICT_NONE},
    }
    for _,c:=range cases {
        m,h,changed:=fallbackObserverManager(types.ICEConnectionTypeUDP,c.threshold,c.allow,false,c.turns,c.sdk,c.version)
        m.UpdateSignalingRTT(c.rtt);m.signalSourceValid.Store(c.valid);m.params.UseOneShotSignallingMode=c.oneShot
        if c.stale {m.lastSignalAt=time.Now().Add(-15*time.Second)}
        m.handleConnectionFailed(true)
        count:=0;if c.expected!=livekit.ICECandidateType_ICT_NONE {count=1}
        record(c.name,m,h,changed,c.expected,count)
    }
    m,h,changed:=fallbackObserverManager(types.ICEConnectionTypeUDP,0,true,false,true,livekit.ClientInfo_JS,"")
    m.subscriber.connectionDetails.Type=types.ICEConnectionTypeTCP
    m.handleConnectionFailed(true);record("lowest-of-both-pcs",m,h,changed,livekit.ICECandidateType_ICT_TLS,1)
    m,h,changed=fallbackObserverManager(types.ICEConnectionTypeUDP,0,true,false,false,livekit.ClientInfo_JS,"")
    m.handleConnectionFailed(false);record("long-first-failure",m,h,changed,livekit.ICECandidateType_ICT_NONE,0)
    m.handleConnectionFailed(false);record("long-second-in-window",m,h,changed,livekit.ICECandidateType_ICT_TCP,1)
    m,h,changed=fallbackObserverManager(types.ICEConnectionTypeUDP,100,true,true,false,livekit.ClientInfo_JS,"")
    m.UpdateSignalingRTT(10);m.UpdateMediaRTT(100)
    for i:=0;i<20;i++ {m.mediaLossProxy.maxDownFracLostTs=time.Now().Add(-2*time.Second);m.HandleReceiverReport(nil,&rtcp.ReceiverReport{Reports:[]rtcp.ReceptionReport{{SSRC:1,FractionLost:63}}})}
    if h.failures!=1 || !h.short {t.Fatal("actual loss proxy must invoke source short failure callback")}
    record("twenty-real-rr-callbacks",m,h,changed,livekit.ICECandidateType_ICT_TCP,1)
    m,h,changed=fallbackObserverManager(types.ICEConnectionTypeUDP,0,true,false,false,livekit.ClientInfo_JS,"")
    before:=m.GetICEConfig() // caller captured before HandleReconnectAndSendResponse
    m.HandleClientReconnect(livekit.ReconnectReason_RR_PUBLISHER_FAILED)
    record("client-short-reconnect-before-restart",m,h,changed,livekit.ICECandidateType_ICT_TCP,1)
    m.SetICEConfig(before) // actual first operation of later TransportManager.ICERestart
    record("caller-earlier-snapshot-restored-by-restart",m,h,changed,livekit.ICECandidateType_ICT_NONE,2)
    data,err:=json.MarshalIndent(map[string]any{"status":"PASS_ORIGINAL_CONSUMER_METHODS","rows":rows,"row_count":len(rows),"scope":"Actual fixed TransportManager/MediaLossProxy/PC preference methods with owned shell PC metadata. Full SDK networking is a separate gate."},"","  ");if err!=nil {t.Fatal(err)}
    if err=os.WriteFile(os.Getenv("TCP_FALLBACK_GO_OBSERVATION"),append(data,'\n'),0600);err!=nil {t.Fatal(err)}
}
