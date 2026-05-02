package ratelimit

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

type KeyFunc func(r *http.Request) string

func ByIP(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return ip
	}
	return r.RemoteAddr
}

func ByUser(r *http.Request) string {
	if uid, ok := r.Context().Value("user_id").(string); ok {
		return uid
	}
	return ByIP(r)
}

// The script runs atomically on Redis, no race condition possible.
// KEYS[1] = the rate limit key
// ARGV[1] = window start (nanoseconds, as a score cutoff)
// ARGV[2] = current timestamp (nanoseconds, used as score + member)
// ARGV[3] = limit (max allowed requests in window)
// ARGV[4] = window duration in seconds (for TTL)
var slidingWindowScript = redis.NewScript(`
    local key        = KEYS[1]
    local window_start = tonumber(ARGV[1])
    local now        = tonumber(ARGV[2])
    local limit      = tonumber(ARGV[3])
    local ttl        = tonumber(ARGV[4])

    -- Remove all entries that have fallen outside the window
    redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

    -- Count how many requests remain in the window
    local count = redis.call('ZCARD', key)

    if count < limit then
        -- Still under limit — record this request and allow it
        redis.call('ZADD', key, now, now)
        redis.call('EXPIRE', key, ttl)
        return {1, limit - count - 1}  -- {allowed, remaining}
    else
        -- Over limit — don't add, just return remaining TTL for Retry-After
        local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local reset_at = 0
        if #oldest > 0 then
            reset_at = math.ceil((tonumber(oldest[2]) / 1e9) + ttl)
        end
        return {0, 0, reset_at}  -- {denied, remaining=0, reset_epoch}
    end
`)

type SlidingWindow struct {
	client *redis.Client
	limit  int
	window time.Duration
}

func NewSlidingWindow(client *redis.Client, limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{client: client, limit: limit, window: window}
}

type Result struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time // when the oldest request exits the window
}

func (sw *SlidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	now := time.Now()
	windowStart := now.Add(-sw.window).UnixNano()
	ttlSeconds := int(sw.window.Seconds())
	redisKey := fmt.Sprintf("rl:%s", key)

	vals, err := slidingWindowScript.Run(
		ctx,
		sw.client,
		[]string{redisKey},
		windowStart,
		now.UnixNano(),
		sw.limit,
		ttlSeconds,
	).Int64Slice()

	if err != nil {
		return Result{}, err
	}

	if vals[0] == 1 {
		return Result{
			Allowed:   true,
			Remaining: int(vals[1]),
		}, nil
	}

	// Denied, parse reset time if returned
	var resetAt time.Time
	if len(vals) > 2 && vals[2] > 0 {
		resetAt = time.Unix(vals[2], 0)
	}
	return Result{Allowed: false, Remaining: 0, ResetAt: resetAt}, nil
}
