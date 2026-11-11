// Invoke the real fixed protocol logger from the fixed server's module graph.
// Run only by the coordinator; no private algorithm or expected encoder copy.
package main

import (
	"errors"
	"github.com/livekit/protocol/logger"
	"math"
	"os"
)

func main() {
	jsonMode := len(os.Args) < 2 || os.Args[1] != "console"
	logger.InitFromConfig(&logger.Config{JSON: jsonMode, Level: "info", Sample: false}, "livekit.typed")
	logger.Infow("typed native event", "signed", int64(-12), "unsigned", uint64(math.MaxUint64), "ratio", float64(.125), "ready", true, "text", "line\nquoted", "debug", "[1, 2]", "fault", errors.New("source error text"))
	logger.Infow("float native event", "nan", math.NaN(), "pos", math.Inf(1), "neg", math.Inf(-1), "small", float64(1e-8), "large", float64(1e22), "negative_zero", math.Copysign(0, -1))
}
