// SPEC-0074 C15 cross-stack WebTransport layered wire probe.
//
// `wtdiff` only reports the final dial error, which cannot distinguish a QUIC/TLS
// handshake failure from an HTTP/3 or WebTransport negotiation failure. This probe
// separates the layers against any listener:
//
//	--mode raw  1) plain quic-go handshake (datagrams + stream-reset partial
//	                delivery enabled, the two features final WebTransport requires),
//	             2) dump and decode the server's HTTP/3 control stream (SETTINGS
//	                frame: every setting ID/value on the wire) plus any other
//	                server-initiated stream header.
//	--mode wt   3) the real webtransport-go dial, reporting the *typed* error
//	             (RequirementsNotMetError message vs idle/handshake timeout) and the
//	             upgrade response headers.
//
// Output is one JSON object per line so a harness can diff the layers.
package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/webtransport-go"
)

type out map[string]any

func emit(stage string, v out) {
	v["stage"] = stage
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

// varint decodes one RFC 9000 variable-length integer (HTTP/3 framing).
func varint(b []byte) (uint64, int, bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	prefix := b[0] >> 6
	n := 1 << prefix
	if len(b) < n {
		return 0, 0, false
	}
	val := uint64(b[0] & 0x3f)
	for i := 1; i < n; i++ {
		val = val<<8 | uint64(b[i])
	}
	return val, n, true
}

// decodeSettings parses an HTTP/3 SETTINGS frame body into id/value pairs.
func decodeSettings(body []byte) []out {
	var res []out
	for len(body) > 0 {
		id, n, ok := varint(body)
		if !ok {
			break
		}
		body = body[n:]
		val, n, ok := varint(body)
		if !ok {
			break
		}
		body = body[n:]
		res = append(res, out{"id": fmt.Sprintf("0x%x", id), "value": fmt.Sprintf("0x%x", val)})
	}
	return res
}

// dumpStreams reads the server-initiated streams of a plain QUIC connection and
// reports their stream-type prefix and, for HTTP/3 SETTINGS, the decoded settings.
func dumpStreams(ctx context.Context, conn *quic.Conn) {
	for i := 0; i < 4; i++ {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		str, err := conn.AcceptStream(sctx)
		if err != nil {
			emit("accept_stream", out{"error": err.Error()})
			cancel()
			return
		}
		buf := make([]byte, 4096)
		_ = str.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, rerr := str.Read(buf)
		info := out{"stream_id": uint64(str.StreamID()), "read_bytes": n}
		if rerr != nil && n == 0 {
			info["read_error"] = rerr.Error()
		}
		if n > 0 {
			body := append([]byte(nil), buf[:n]...)
			info["prefix_hex"] = hex.EncodeToString(body[:min(n, 24)])
			typ, ln, ok := varint(body)
			if ok {
				body = body[ln:]
				info["frame_type"] = fmt.Sprintf("0x%x", typ)
				if typ == 0x1 { // SETTINGS
					size, ln, ok := varint(body)
					if ok {
						body = body[ln:]
						if int(size) <= len(body) {
							info["settings"] = decodeSettings(body[:size])
						}
					}
				}
			}
		}
		emit("server_stream", info)
		cancel()
	}
}

func main() {
	addr := flag.String("addr", "", "host:port of the WebTransport listener")
	token := flag.String("token", "", "access token with the video.agent grant")
	path := flag.String("path", "/agent", "upgrade path")
	mode := flag.String("mode", "wt", "raw (QUIC + h3 SETTINGS dump) or wt (webtransport-go dial)")
	timeout := flag.Duration("timeout", 15*time.Second, "overall budget")
	qlogDir := flag.String("qlog", "", "write quic-go qlog (.sqlog) traces into this directory")
	flag.Parse()

	// quic-go's own tracer is the authoritative per-packet record: it says whether a
	// datagram was received at all, why it was dropped, and which transport
	// parameters the server actually sent — none of which a client error string shows.
	quicConf := func() *quic.Config {
		c := &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		}
		if *qlogDir != "" {
			// qlog.DefaultConnectionTracer is driven by the QLOGDIR environment
			// variable and returns nil (no tracing) when it is unset.
			_ = os.Setenv("QLOGDIR", *qlogDir)
			c.Tracer = qlog.DefaultConnectionTracer
		}
		return c
	}()

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "localhost",
		NextProtos:         []string{"h3"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	dialer := &webtransport.Transport{
		TLSClientConfig: tlsConf,
		QUICConfig:      quicConf,
	}

	if *mode == "raw" {
		// Layer 1: bare QUIC/TLS handshake. quic-go's own client is used through
		// the WebTransport transport so the exact same ALPN/extension set the
		// official agent client sends is on the wire.
		conn, err := quic.DialAddr(ctx, *addr, tlsConf, quicConf)
		if err != nil {
			var timeoutErr interface{ Timeout() bool }
			emit("quic_handshake", out{
				"error":   err.Error(),
				"timeout": errors.As(err, &timeoutErr) && timeoutErr.Timeout(),
			})
			return
		}
		st := conn.ConnectionState()
		emit("quic_handshake", out{
			"ok": true,
			"version":              st.Version.String(),
			"datagrams_remote":     st.SupportsDatagrams.Remote,
			"datagrams_local":      st.SupportsDatagrams.Local,
			"partial_delivery":     st.SupportsStreamResetPartialDelivery.Remote,
			"partial_local":        st.SupportsStreamResetPartialDelivery.Local,
			"cipher_suite":         fmt.Sprintf("0x%x", st.TLS.CipherSuite),
			"peer_cert_present":    len(st.TLS.PeerCertificates) > 0,
			"resumption_attempted": st.TLS.DidResume,
		})
		// Layer 2: what the server actually puts on its HTTP/3 control stream.
		dumpStreams(ctx, conn)
		conn.CloseWithError(0, "probe done")
		return
	}

	url := fmt.Sprintf("https://%s%s", *addr, *path)
	header := http.Header{}
	if *token != "" {
		header.Set("Authorization", "Bearer "+*token)
	}
	resp, sess, err := dialer.Dial(ctx, url, header)
	if err != nil {
		var reqErr *webtransport.RequirementsNotMetError
		var idleErr interface {
			error
			Timeout() bool
		}
		info := out{"error": err.Error(), "error_type": fmt.Sprintf("%T", err)}
		if errors.As(err, &reqErr) {
			info["requirements_not_met"] = reqErr.Message
		}
		if errors.As(err, &idleErr) {
			info["timeout"] = idleErr.Timeout()
		}
		if resp != nil {
			info["status"] = resp.StatusCode
			info["headers"] = resp.Header.Clone()
		}
		emit("wt_dial", info)
		os.Exit(1)
	}
	emit("wt_dial", out{
		"ok":        true,
		"status":    resp.StatusCode,
		"headers":   resp.Header.Clone(),
		"local":     sess.LocalAddr().String(),
		"remote":    sess.RemoteAddr().String(),
		"protocol":  sess.SessionState().ApplicationProtocol,
		"datagrams": sess.SessionState().ConnectionState.SupportsDatagrams.Remote,
	})
	_ = sess.CloseWithError(0, "probe done")
}
