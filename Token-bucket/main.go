package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//REDIS

func NewRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
}

// TOken bucket
// KEYS[1] = rate limit key
// ARGV[1] = current timestamp in seconds (float, for refill math)
// ARGV[2] = refill rate (tokens per second)
// ARGV[3] = bucket capacity (max tokens)
// ARGV[4] = tokens requested (usually 1)
// ARGV[5] = TTL in seconds
//
// Redis stores two fields in a hash:
//
//	tokens     — current token count (float)
//	last_refill — unix timestamp of last request (float)
var tokenBucketScript = redis.NewScript(`
    local key      = KEYS[1]
    local now      = tonumber(ARGV[1])
    local rate     = tonumber(ARGV[2])
    local capacity = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4])
    local ttl      = tonumber(ARGV[5])

    local data = redis.call('HMGET', key, 'tokens', 'last_refill')

    local tokens     = tonumber(data[1])
    local last_refill = tonumber(data[2])

    -- First request ever for this key
    if tokens == nil then
        tokens = capacity
        last_refill = now
    end

    -- Refill: add tokens proportional to elapsed time, cap at capacity
    local elapsed = math.max(0, now - last_refill)
    tokens = math.min(capacity, tokens + elapsed * rate)

    local allowed = 0
    local remaining = 0

    if tokens >= requested then
        tokens = tokens - requested
        allowed = 1
    end

    remaining = math.floor(tokens)

    -- Persist updated state back to Redis
    redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, ttl)

    -- Return: allowed, remaining, current token count (for debugging)
    return {allowed, remaining}
`)

type TokenBucketRedis struct {
	client   *redis.Client
	rate     float64       // tokens refilled per second
	capacity float64       // max tokens in bucket
	window   time.Duration // TTL for the Redis key
}

func NewTokenBucketRedis(client *redis.Client, rate, capacity float64, window time.Duration) *TokenBucketRedis {
	return &TokenBucketRedis{
		client:   client,
		rate:     rate,
		capacity: capacity,
		window:   window,
	}
}

type Result struct {
	Allowed   bool
	Remaining int
}

func (tb *TokenBucketRedis) Allow(ctx context.Context, key string) (Result, error) {
	return tb.AllowN(ctx, key, 1)
}

func (tb *TokenBucketRedis) AllowN(ctx context.Context, key string, n int) (Result, error) {
	// Seconds as float cos lua refills math with with sub second precision
	nowSeconds := float64(time.Now().UnixNano()) / 1e9
	ttl := int(tb.window.Seconds())

	vals, err := tokenBucketScript.Run(
		ctx,
		tb.client,
		[]string{fmt.Sprintf("rl:tb:%s", key)},
		nowSeconds,
		tb.rate,
		tb.capacity,
		n,
		ttl,
	).Int64Slice()

	if err != nil {
		return Result{}, err
	}

	return Result{
		Allowed:   vals[0] == 1,
		Remaining: int(vals[1]),
	}, nil
}

// Key extraction
func extractKey(r *http.Request) string {
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return "apikey:" + apiKey
	}
	if userID, ok := r.Context().Value(contextKey("userID")).(string); ok && userID != "" {
		return "user:" + userID
	}
	// Behind a proxy? use: r.Header.Get("X-Forwarded-For")
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	// if err != nil {
	// 	ip = r.RemoteAddr
	// }
	return "ip:" + ip
}

// contextKey avoids key collisions in context.Value
type contextKey string

//Middleware

func RateLimitMiddleware(limiter TokenBucketRedis) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := extractKey(r)

			result, err := limiter.Allow(r.Context(), key)
			if err != nil {
				// Redis is down — decide: fail open or fail closed
				// Fail open (allow the request) is safer for availability:
				log.Printf("rate limiter error (fail open): %v", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))

			if !result.Allowed {
				w.Header().Set("Retry-After", "1") // seconds until next token
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error":"rate limit exceeded"}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Handlers
func dataHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "ok"})
}

//Main

func main() {
	rdb := NewRedisClient()

	// Verify Redis is reachable before serving traffic
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot reach Redis: %v", err)
	}

	// 10 tokens refilled per second, burst up to 30, TTL 120s, just make sure ttl >= capacity/rate 30/10 = 3s minimum
	limiter := NewTokenBucketRedis(rdb, 10, 10, 120*time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/data", dataHandler)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      RateLimitMiddleware(*limiter)(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("listening on :8080")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
