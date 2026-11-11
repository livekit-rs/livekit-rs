// SPDX-License-Identifier: MIT
// Real fixed LiveKit Pion peer; no modification to the selected modules.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

type observedConn struct {
	net.PacketConn
	sentDTLS  atomic.Uint64
	recvDTLS  atomic.Uint64
	sentACK   atomic.Uint64
	recvACK   atomic.Uint64
	dropFirst bool
	dropped   atomic.Bool
}

func observe(p []byte) (embedded, ack bool) {
	if len(p) < 20 || p[0]&0xc0 != 0 || binary.BigEndian.Uint32(p[4:8]) != 0x2112a442 {
		return
	}
	for at := 20; at+4 <= len(p); {
		kind, n := binary.BigEndian.Uint16(p[at:]), int(binary.BigEndian.Uint16(p[at+2:]))
		if at+4+n > len(p) {
			return
		}
		if kind == 0xc070 && n > 0 {
			embedded = true
		}
		if kind == 0xc071 && n > 0 && n <= 16 && n%4 == 0 {
			ack = true
		}
		at += 4 + (n+3)&^3
	}
	return
}
func (c *observedConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, a, e := c.PacketConn.ReadFrom(p)
	if e == nil {
		d, ack := observe(p[:n])
		if d {
			c.recvDTLS.Add(1)
		}
		if ack {
			c.recvACK.Add(1)
		}
	}
	return n, a, e
}
func (c *observedConn) WriteTo(p []byte, a net.Addr) (int, error) {
	d, ack := observe(p)
	if d {
		c.sentDTLS.Add(1)
	}
	if ack {
		c.sentACK.Add(1)
	}
	if d && c.dropFirst && c.dropped.CompareAndSwap(false, true) {
		return len(p), nil
	}
	return c.PacketConn.WriteTo(p, a)
}

type envelope struct {
	Type string `json:"type"`
	SDP  string `json:"sdp,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	sped := flag.Bool("sped", true, "fixed Pion EnableSped flag")
	drop := flag.Bool("drop-first", false, "drop first real outbound embedded DTLS datagram")
	flag.Parse()
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer udp.Close()
	wire := &observedConn{PacketConn: udp, dropFirst: *drop}
	mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: wire})
	defer mux.Close()
	var settings webrtc.SettingEngine
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	settings.SetICEUDPMux(mux)
	settings.EnableSped(*sped)
	settings.EnableSctpSnap(true)
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer pc.Close()
	dc, err := pc.CreateDataChannel("_reliable", nil)
	if err != nil {
		return err
	}
	sent := make(chan error, 1)
	dc.OnOpen(func() { sent <- dc.Send([]byte("pion-sped-upstream")) })
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	gathering := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return err
	}
	select {
	case <-gathering:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("candidate gathering deadline")
	}
	enc := json.NewEncoder(os.Stdout)
	dec := json.NewDecoder(os.Stdin)
	if err = enc.Encode(envelope{Type: "offer", SDP: pc.LocalDescription().SDP}); err != nil {
		return err
	}
	var answer envelope
	if err = dec.Decode(&answer); err != nil {
		return err
	}
	if answer.Type != "answer" {
		return fmt.Errorf("answer envelope required")
	}
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		return err
	}
	select {
	case err = <-sent:
		if err != nil {
			return err
		}
	case <-time.After(15 * time.Second):
		return fmt.Errorf("real SCTP data channel open/send deadline")
	}
	// Rust acknowledges only after its actual media DataReceived consumer fires.
	var stop envelope
	if err = dec.Decode(&stop); err != nil {
		return err
	}
	if stop.Type != "received" {
		return fmt.Errorf("actual Rust DataReceived receipt required")
	}
	if err = pc.Close(); err != nil {
		return err
	}
	return enc.Encode(map[string]any{"type": "result", "sped": *sped, "drop_first": *drop, "dropped": wire.dropped.Load(), "sent_embedded_dtls": wire.sentDTLS.Load(), "received_embedded_dtls": wire.recvDTLS.Load(), "sent_ack": wire.sentACK.Load(), "received_ack": wire.recvACK.Load(), "actual_sctp_send": true, "pc_closed": true})
}
