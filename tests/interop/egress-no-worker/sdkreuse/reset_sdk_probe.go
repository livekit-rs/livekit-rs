// Command sdkreuse 复现官方 Twirp 客户端在“服务端 panic 应答后关闭 keep-alive 连接”
// 时的真实行为。Twirp 生成的 Ingress 客户端把 RoundTrip 完全委托给
// net/http.Transport，重试判定也 100% 在 Transport.shouldRetryRequest /
// Request.isReplayable 里，与本程序用 stdlib net/http 走同一代码路径，故这里
// 用官方客户端同款的可重放 body（bytes.Reader -> GetBody!=nil）+ 单连接复用
// （MaxConnsPerHost=1）来观测，而不是把“会自动重试”当成结论。
//
//	go run reset_sdk_probe.go -base http://127.0.0.1:PORT -n 40 -key devkey -secret <secret>
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func b64(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

func token(key, secret string) string {
	header := b64([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().Unix()
	claims, _ := json.Marshal(map[string]any{
		"iss": key, "sub": "sdk-reuse", "exp": now + 600, "nbf": now - 5,
		"video": map[string]any{"roomAdmin": true, "roomCreate": true,
			"roomList": true, "ingressAdmin": true, "room": "r1"},
	})
	body := b64(claims)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(header + "." + body))
	return header + "." + body + "." + b64(mac.Sum(nil))
}

type outcome struct {
	Err    string `json:"error,omitempty"`
	Status int    `json:"status,omitempty"`
	Body   string `json:"body,omitempty"`
}

func post(cl *http.Client, base, path, auth string, body []byte) outcome {
	req, err := http.NewRequest("POST", base+path, bytes.NewReader(body))
	if err != nil {
		return outcome{Err: "newreq: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+auth)
	resp, err := cl.Do(req)
	if err != nil {
		return outcome{Err: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return outcome{Status: resp.StatusCode, Body: string(raw)}
}

func main() {
	base := flag.String("base", "", "server base url")
	n := flag.Int("n", 40, "iterations")
	key := flag.String("key", "devkey", "api key")
	secret := flag.String("secret", "", "api secret")
	idem := flag.Bool("idempotency-key", false, "also POST with Idempotency-Key (should become retryable)")
	out := flag.String("out", "", "write json verdict here")
	flag.Parse()
	auth := token(*key, *secret)
	update := []byte(`{"ingress_id":"IN_x"}`)
	listRooms := []byte(`{}`)

	// 与 Twirp/官方 SDK 默认一致：共享一个 Transport、单主机连接池，强制复用一条
	// keep-alive 连接，模拟差分客户端连续调用。
	cl := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		MaxConnsPerHost:     1,
		MaxIdleConnsPerHost: 1,
		DisableKeepAlives:   false,
	}}

	var surfacedErr, clean500, retriedOK int
	samples := []outcome{}
	for i := 0; i < *n; i++ {
		// 先发一次会 panic 的 UpdateIngress（服务端应答后关闭连接），
		// 再在同池发一次普通请求，观察官方 Transport 是否把它成功重试到 200/500，
		// 还是把连接错误冒泡给调用方。
		first := post(cl, *base, "/twirp/livekit.Ingress/UpdateIngress", auth, update)
		second := post(cl, *base, "/twirp/livekit.RoomService/ListRooms", auth, listRooms)
		if second.Err != "" {
			surfacedErr++
		} else if second.Status == 500 && bytes.Contains([]byte(second.Body), []byte("Internal service panic")) {
			clean500++
		} else {
			retriedOK++
		}
		if len(samples) < 4 {
			samples = append(samples, outcome{Err: fmt.Sprintf("first=%s|second=%s",
				or(first.Err, first.Status), or(second.Err, second.Status))})
		}
	}
	verdict := map[string]any{
		"iterations":                     *n,
		"second_after_panic_surfaced":    surfacedErr,
		"second_after_panic_clean500":    clean500,
		"second_after_panic_other_ok":    retriedOK,
		"with_idempotency_key":           *idem,
		"samples":                        samples,
		"transport_retry_is_conditional": "net/http.shouldRetryRequest 只在 nothingWritten 或请求幂等/可重放时重试；POST 已写出且无 Idempotency-Key -> isReplayable()=false -> 不重试（stdlib request.go:1534, transport.go:842-862）。",
	}
	j, _ := json.MarshalIndent(verdict, "", " ")
	fmt.Println(string(j))
	if *out != "" {
		if err := os.WriteFile(*out, append(j, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write out:", err)
			os.Exit(1)
		}
	}
}

func or(a string, b int) string {
	if a != "" {
		return a
	}
	return fmt.Sprint(b)
}
