package middleware

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"

	"rate-limiter/blocking"
	"rate-limiter/limiter"
)

type RateLimiterMiddleware struct {
	Bucket  *limiter.TokenBucket
	Blocker *blocking.IPBlocker
}

func getClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)

	if err != nil {
		return r.RemoteAddr
	}

	return host
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	message string,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	json.NewEncoder(w).Encode(map[string]string{
		"message": message,
	})
}

func (m *RateLimiterMiddleware) Handler(
	next http.Handler,
) http.Handler {

	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {

		ctx := context.Background()

		ip := getClientIP(r)

		// Check whether IP is blocked
		blocked, err := m.Blocker.IsBlocked(ctx, ip)

		if err != nil {
			writeJSON(
				w,
				http.StatusInternalServerError,
				"Redis error",
			)
			return
		}

		if blocked {
			writeJSON(
				w,
				http.StatusForbidden,
				"Your IP is blocked",
			)
			return
		}

		// Check token bucket
		allowed, remaining, err := m.Bucket.Allow(ctx, ip)

		if err != nil {
			writeJSON(
				w,
				http.StatusInternalServerError,
				"Rate limiter error",
			)
			return
		}

		w.Header().Set(
			"X-RateLimit-Remaining",
			strconv.FormatFloat(remaining, 'f', 2, 64),
		)

		// Reject if no tokens available
		if !allowed {
			writeJSON(
				w,
				http.StatusTooManyRequests,
				"Rate limit exceeded",
			)
			return
		}

		// Allow request to continue
		next.ServeHTTP(w, r)
	})
}
