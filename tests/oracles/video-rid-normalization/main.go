package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
)

type result struct {
	ID         string       `json:"id"`
	Input      [3]string    `json:"input"`
	Normalized [3]string    `json:"normalized"`
	Mappings   [8][3]string `json:"mappings"`
}

func observe(path string) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("%w; close: %v", err, closeErr)
			}
		}
	}()

	// Source-contract inputs, not SDP validation or MAIN/SRTP evidence.
	domain := [...]string{"", "q", "h", "f", "2", "1", "0", "x"}
	out := json.NewEncoder(f)
	for i, first := range domain {
		for j, second := range domain {
			for k, third := range domain {
				input := [3]string{first, second, third}
				normalized := buffer.NormalizeVideoLayersRid(buffer.VideoLayersRid(input))
				row := result{
					ID:         fmt.Sprintf("rid-%d-%d-%d", i, j, k),
					Input:      input,
					Normalized: [3]string(normalized),
				}
				for mask := 0; mask < 8; mask++ {
					ti := &livekit.TrackInfo{}
					for quality := 0; quality < 3; quality++ {
						if mask&(1<<quality) != 0 {
							ti.Layers = append(ti.Layers, &livekit.VideoLayer{
								Quality: livekit.VideoQuality(quality),
							})
						}
					}
					for quality := 0; quality < 3; quality++ {
						row.Mappings[mask][quality] = buffer.VideoQualityToRid(
							mime.MimeTypeVP8, livekit.VideoQuality(quality), ti, normalized,
						)
					}
				}
				if err := out.Encode(row); err != nil {
					return fmt.Errorf("encode %s: %w", row.ID, err)
				}
			}
		}
	}
	return f.Sync()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: video-rid-normalization OUTPUT.jsonl")
		os.Exit(2)
	}
	if err := observe(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote 512 RID normalization source-contract rows (8 masks x 3 qualities each)")
}
