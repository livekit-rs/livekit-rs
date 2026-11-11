//go:build source_codec_observer

package connectionquality

// SourceCodecSnapshot invokes the original periodic/close snapshot consumer.
// It neither substitutes delta calculations nor changes timer configuration.
func SourceCodecSnapshot(stats *ConnectionStats) { stats.getStat() }
