package main

import (
    "encoding/json"
    "net/http"
    "os"
    "time"

    "github.com/hashicorp/go-retryablehttp"
)

func main() {
    encoder := json.NewEncoder(os.Stdout)
    for _, input := range []struct {
        Status int
        Values []string
    }{
        {503, []string{"Sun, 06 Nov 1994 08:49:37 GMT"}},
        {503, []string{"Mon, 06 Nov 1994 08:49:37 GMT"}},
        {503, []string{"Sun, 06 nov 1994 8:49:37.125 UTC"}},
        {503, []string{"Sun, 06 Nov 1960 08:49:37 GMT"}},
        {503, []string{"Sunday, 06-Nov-94 08:49:37 GMT"}},
        {503, []string{"Sun Nov  6 08:49:37 1994"}},
        {503, []string{"Sun, 31 Feb 1994 08:49:37 GMT"}},
        {503, []string{"Sun, 06 Nov 1994 08:49:37 GMT+2"}},
        {503, []string{"Sun, 06 Nov 1994 08:49:37 ZZZ"}},
        {429, []string{"-1"}},
        {429, []string{"-0"}},
        {429, []string{"+0"}},
        {429, []string{"0", "100"}},
        {429, []string{"bad", "0"}},
        {429, []string{"301"}},
        {503, []string{"9223372036854775808"}},
        {503, []string{"9223372037"}},
        {503, []string{"18446744074"}},
        {503, []string{"9223372036854775807"}},
        {500, []string{"Sun, 06 Nov 1994 08:49:37 GMT"}},
    } {
        response := &http.Response{StatusCode: input.Status, Header: http.Header{"Retry-After": input.Values}}
        wait := retryablehttp.DefaultBackoff(time.Second, 30*time.Second, 0, response)
        encoder.Encode(map[string]any{"status": input.Status, "values": input.Values, "wait_ns": int64(wait)})
    }
}
