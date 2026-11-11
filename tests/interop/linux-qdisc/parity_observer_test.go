//go:build linux

// Independent observer appended only to a staged copy of the fixed package.
// The getTCStats algorithm and all original source/module files are unchanged.
package prometheus

import (
	"encoding/json"
	"os"
	"testing"
)

func TestOwnedQdiscKernelCounters(t *testing.T) {
	packets, drops, err := getTCStats()
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		p, d, err := getTCStats()
		if err != nil {
			t.Fatal(err)
		}
		if p != packets || d != drops {
			t.Fatal("kernel traffic not quiescent", p, d, packets, drops)
		}
		after, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		if len(after) != len(before) {
			t.Fatal("original NETLINK sample leaked an FD", len(before), len(after))
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatal("final FD mismatch")
	}
	path := os.Getenv("QDISC_RESULT_PATH")
	if path == "" {
		t.Fatal("owned output path required")
	}
	data, err := json.Marshal(map[string]any{"packets": packets, "drops": drops, "samples": 257, "fd_before": len(before), "fd_after": len(after)})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}
