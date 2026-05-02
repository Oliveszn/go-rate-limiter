package middleware

import (
	"fmt"
	"net/http"
	"sw/ratelimit"
	"time"
)

func Middleware(limiter *ratelimit.SlidingWindow, keyFn ratelimit.KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res, err := limiter.Allow(r.Context(), keyFn(r))
			if err != nil {
				// Redis is down, fail open
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", res.Remaining))

			if !res.Allowed {
				if !res.ResetAt.IsZero() {
					retryAfter := time.Until(res.ResetAt).Seconds()
					w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter))
					w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", res.ResetAt.Unix()))
				}
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
