// Copyright 2026 livekit-rs contributors. Apache-2.0 original-source observer.
package videolayerselector

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/protocol/logger"
	"testing"
)

func TestOriginalFrameNumberWrapperKeepsUnknownAndLateSourceReferences(t *testing.T) {
	wrapper := FrameNumberWrapper{logger: logger.GetLogger()}
	for _, in := range []struct {
		frame    uint64
		attached bool
	}{{10, true}, {14, false}, {16, false}, {15, false}, {17, false}, {60000, true}, {60001, false}, {60000, false}, {65540, true}, {100000, true}} {
		out := wrapper.UpdateAndGet(in.frame, in.attached)
		raw, err := json.Marshal(map[string]interface{}{"frame": in.frame, "attached": in.attached, "output": uint16(out)})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("SOURCE_DD_WRAPPER " + string(raw))
	}
}
