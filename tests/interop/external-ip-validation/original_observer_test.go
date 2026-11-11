// Stage this observer alongside an unchanged fixed mediatransportutil source
// copy. It calls original unexported functions; no algorithm is replaced.
package rtcconfig

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ownedSTUN(t *testing.T, mapped net.IP) (string, func()) {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_ = socket.SetReadDeadline(time.Now().Add(3 * time.Second))
		request := make([]byte, 1500)
		n, peer, err := socket.ReadFromUDP(request)
		if err == nil && (n != 20 || binary.BigEndian.Uint16(request[0:2]) != 0x0001 || binary.BigEndian.Uint32(request[4:8]) != 0x2112a442) {
			err = fmt.Errorf("controlled fixture received invalid original STUN binding shape: len=%d", n)
		}
		if err == nil && n == 20 {
			response := make([]byte, 32)
			binary.BigEndian.PutUint16(response[0:2], 0x0101)
			binary.BigEndian.PutUint16(response[2:4], 12)
			copy(response[4:20], request[4:20])
			copy(response[20:28], []byte{0, 0x20, 0, 8, 0, 1, 0x21, 0x12})
			cookie := []byte{0x21, 0x12, 0xa4, 0x42}
			for i, octet := range mapped.To4() {
				response[28+i] = octet ^ cookie[i]
			}
			_, err = socket.WriteToUDP(response, peer)
		}
		_ = socket.Close()
		done <- err
	}()
	return socket.LocalAddr().String(), func() {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
func TestOriginalExternalIPValidationObserver(t *testing.T) {
	output := os.Getenv("EXTERNAL_IP_GO_OBSERVATION")
	if output == "" {
		t.Skip("coordinator provides new evidence path")
	}
	var rows []map[string]any
	for _, row := range []struct {
		name     string
		external string
		validate bool
		want     bool
	}{
		{"reachable-validate", "127.0.0.1", true, true},
		{"unreachable-validate", "127.0.0.2", true, false},
		{"unreachable-skip", "127.0.0.2", false, true},
	} {
		reservation, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		local := reservation.LocalAddr().(*net.UDPAddr)
		_ = reservation.Close()
		stun, join := ownedSTUN(t, net.ParseIP(row.external))
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		ip, callErr := getExternalIP(ctx, []string{stun}, local, row.validate, "udp4")
		cancel()
		join()
		if (callErr == nil) != row.want {
			t.Fatalf("%s: actual original verdict ip=%q err=%v", row.name, ip, callErr)
		}
		released, err := net.ListenUDP("udp4", local)
		if err != nil {
			t.Fatalf("%s leaked source socket: %v", row.name, err)
		}
		_ = released.Close()
		item := map[string]any{"case": row.name, "ip": ip, "success": callErr == nil, "source_port_released": true}
		if callErr != nil {
			item["error"] = callErr.Error()
		}
		rows = append(rows, item)
	}
	// Exercise original interface mapping, not a copied map algorithm. Only the
	// real loopback IPv4 interface is admitted; no external network access.
	for _, skip := range []bool{false, true} {
		stun, join := ownedSTUN(t, net.IPv4(127, 0, 0, 2))
		conf := &RTCConfig{STUNServers: []string{stun}, SkipExternalIPValidation: skip, EnableLoopbackCandidate: true}
		mappings, filter, err := getNAT1to1IPsForConf(conf, nil, func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) })
		join()
		if err != nil {
			t.Fatal(err)
		}
		item := map[string]any{"case": "actual-per-interface-map", "skip": skip, "mappings": mappings, "filter_present": filter != nil}
		if skip && (len(mappings) != 1 || mappings[0] != "127.0.0.2/127.0.0.1") {
			t.Fatalf("skip must preserve exact actual public/local mapping, got %v", mappings)
		}
		if !skip && len(mappings) != 0 {
			t.Fatal("unreachable validated mapping must be absent")
		}
		rows = append(rows, item)
	}
	bytes, err := json.MarshalIndent(map[string]any{"original_source": "mediatransportutil-f234b534b095/pkg/rtcconfig/{ip,webrtc_config}.go", "rows": rows}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, append(bytes, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
