// Actual pinned encoding/json oracle for the Rust egress layout metadata port.
// Marshals the real upstream service.LayoutMetadata struct, which is the exact
// expression at references/livekit-stable/pkg/service/egress.go:301.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/livekit/livekit-server/pkg/service"
)

type vector struct {
	Name   string `json:"name"`
	Layout string `json:"layout"`
}

type row struct {
	Name     string `json:"name"`
	Layout   string `json:"layout"`
	Metadata string `json:"metadata"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: workers-layout-oracle CASES.json")
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
		metadata, err := json.Marshal(&service.LayoutMetadata{Layout: v.Layout})
		if err != nil {
			panic(err)
		}
		rows = append(rows, row{Name: v.Name, Layout: v.Layout, Metadata: string(metadata)})
	}
	out, err := json.Marshal(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}