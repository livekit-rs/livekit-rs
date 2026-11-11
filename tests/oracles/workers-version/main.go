// Actual pinned hashicorp/go-version oracle for the Rust workers version port.
// Operands are prepared exactly as RedisStore.Start does it
// (references/livekit-stable/pkg/service/redisstore.go:100-109): the empty stored
// value becomes "0.0.0" before parsing and both parse errors are discarded.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	goversion "github.com/hashicorp/go-version"
)

type vector struct {
	ID       string `json:"id"`
	Existing string `json:"existing"`
	Current  string `json:"current"`
	Source   string `json:"source"`
}

type row struct {
	Existing string `json:"existing"`
	Current  string `json:"current"`
	Valid    bool   `json:"valid"`
	Update   bool   `json:"update"`
	// Upstream discards the NewVersion error and then calls GreaterThan on a nil
	// *Version, whose String() dereferences v.segments. Recorded rather than
	// hidden: the Rust port returns a typed error instead of panicking.
	UpstreamNilDeref bool `json:"upstream_nil_deref"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: workers-version-oracle CASES.json")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var vectors []vector
	if err := json.Unmarshal(raw, &vectors); err != nil {
		panic(err)
	}
	rows := make([]row, 0, len(vectors))
	for _, v := range vectors {
		stored := v.Existing
		if stored == "" {
			stored = "0.0.0"
		}
		existing, existingErr := goversion.NewVersion(stored)
		current, currentErr := goversion.NewVersion(v.Current)
		r := row{Existing: v.Existing, Current: v.Current,
			Valid: existingErr == nil && currentErr == nil}
		if r.Valid {
			r.Update = current.GreaterThan(existing)
		} else {
			r.UpstreamNilDeref = true
		}
		rows = append(rows, r)
	}
	out, err := json.Marshal(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}