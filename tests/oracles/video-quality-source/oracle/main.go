package main

// Fixed-Go source observer for SIGNAL:VideoLayer.OpenQualityConsumers.
// Calls the REAL exported original pkg/sfu/buffer consumers on a constructed
// TrackInfo premise; no ParticipantImpl / transport / media instantiation. The
// emitted rows are the actual upstream internal outcomes, read back by the ignored
// Rust test rtc-media layers::video_quality_source_differential.

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/codecs/mime"
	pb "github.com/livekit/protocol/livekit"
)

type layerIn struct {
	Quality    int32  `json:"quality"`
	Spatial    int32  `json:"spatial"`
	Rid        string `json:"rid"`
	Width      uint32 `json:"width"`
	Height     uint32 `json:"height"`
	Bitrate    uint32 `json:"bitrate"`
	Ssrc       uint32 `json:"ssrc"`
	RepairSsrc uint32 `json:"repair_ssrc"`
}

type caseIn struct {
	ID           string    `json:"id"`
	Mime         string    `json:"mime"`
	Layers       []layerIn `json:"layers"`
	RidSpace     [3]string `json:"rid_space"`
	QueryQuality int32     `json:"query_quality"`
	QuerySpatial int32     `json:"query_spatial"`
	QueryRid     string    `json:"query_rid"`
}

func main() {
	b, e := os.ReadFile(os.Getenv("VIDEO_QUALITY_CASES"))
	if e != nil {
		panic(e)
	}
	var cases []caseIn
	if e = json.Unmarshal(b, &cases); e != nil {
		panic(e)
	}
	if len(cases) != 1056 {
		panic("source case count must be 1056")
	}
	f, e := os.OpenFile(os.Getenv("VIDEO_QUALITY_GO_RESULT"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	out := json.NewEncoder(f)
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.ID] || c.ID == "" {
			panic("inventory")
		}
		seen[c.ID] = true
		// Codec-less TrackInfo: GetVideoLayersForMimeType falls back to ti.Layers,
		// isolating the presence/mapping consumers from codec selection.
		ti := &pb.TrackInfo{Sid: c.ID}
		for _, l := range c.Layers {
			ti.Layers = append(ti.Layers, &pb.VideoLayer{
				Quality:      pb.VideoQuality(l.Quality),
				SpatialLayer: l.Spatial,
				Rid:          l.Rid,
				Width:        l.Width,
				Height:       l.Height,
				Bitrate:      l.Bitrate,
				Ssrc:         l.Ssrc,
				RepairSsrc:   l.RepairSsrc,
			})
		}
		mimeType := mime.NormalizeMimeType(c.Mime)
		ridSpace := buffer.VideoLayersRid{c.RidSpace[0], c.RidSpace[1], c.RidSpace[2]}
		quality := pb.VideoQuality(c.QueryQuality)

		lp := buffer.LayerPresenceFromTrackInfo(mimeType, ti)
		var presence any
		if lp != nil {
			presence = [3]bool{lp[0], lp[1], lp[2]}
		}
		row := map[string]any{
			"id":                      c.ID,
			"input":                   c,
			"presence":                presence,
			"quality_to_spatial":      buffer.VideoQualityToSpatialLayer(mimeType, quality, ti),
			"get_spatial_for_quality": buffer.GetSpatialLayerForVideoQuality(mimeType, quality, ti),
			"spatial_to_quality":      int32(buffer.SpatialLayerToVideoQuality(mimeType, c.QuerySpatial, ti)),
			"spatial_to_rid":          buffer.SpatialLayerToRid(mimeType, c.QuerySpatial, ti, ridSpace),
			"quality_to_rid":          buffer.VideoQualityToRid(mimeType, quality, ti, ridSpace),
			"get_spatial_for_rid":     buffer.GetSpatialLayerForRid(mimeType, c.QueryRid, ti),
			"get_quality_for_spatial": int32(buffer.GetVideoQualityForSpatialLayer(mimeType, c.QuerySpatial, ti)),
		}
		if e = out.Encode(row); e != nil {
			panic(e)
		}
	}
	if e = f.Sync(); e != nil {
		panic(e)
	}
	fmt.Fprintf(os.Stderr, "original VideoLayer open-quality consumer rows=%d\n", len(cases))
}