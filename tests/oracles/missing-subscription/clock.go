package rtc
import "time"
var sourceOracleClock time.Time
func sourceOracleNow() time.Time{return sourceOracleClock}
func sourceOracleSince(t time.Time)time.Duration{return sourceOracleClock.Sub(t)}
