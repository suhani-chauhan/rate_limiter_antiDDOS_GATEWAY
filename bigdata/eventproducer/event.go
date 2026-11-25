package eventproducer

import "time"

// Decision values required by the contract.
const (
	DecisionAllowed     = "allowed"
	DecisionRateLimited = "rate_limited"
	DecisionBlocked     = "blocked"
)

// Event is the fixed JSON contract for topic gateway.traffic.events.
type Event struct {
	Timestamp  string `json:"timestamp"` // ISO8601 UTC, e.g. 2026-11-22T10:48:05.123Z
	IP         string `json:"ip"`
	Method     string `json:"method"`
	URL        string `json:"url"`
	StatusCode int    `json:"status_code"`
	Decision   string `json:"decision"` // allowed | rate_limited | blocked
	UserAgent  string `json:"user_agent"`
	LatencyMs  int64  `json:"latency_ms"`
}

// NewEvent builds an Event with the timestamp set to now (UTC, ISO8601).
func NewEvent(ip, method, url string, status int, decision, userAgent string, latency time.Duration) Event {
	return Event{
		Timestamp:  time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		IP:         ip,
		Method:     method,
		URL:        url,
		StatusCode: status,
		Decision:   decision,
		UserAgent:  userAgent,
		LatencyMs:  latency.Milliseconds(),
	}
}