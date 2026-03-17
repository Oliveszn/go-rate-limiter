package main

import (
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Store limiters
var (
	ipLimiters   = make(map[string]*rate.Limiter)
	userLimiters = make(map[string]*rate.Limiter)
	mu           sync.Mutex
)

// Create limiter (5 req/sec, burst 10)
func newLimiter() *rate.Limiter {
	return rate.NewLimiter(5, 10)
}

// Get IP limiter
func getIPLimiter(ip string) *rate.Limiter {
	mu.Lock()
	defer mu.Unlock()

	limiter, exists := ipLimiters[ip]
	if !exists {
		limiter = newLimiter()
		ipLimiters[ip] = limiter
	}
	return limiter
}

// Get User limiter
func getUserLimiter(userID string) *rate.Limiter {
	mu.Lock()
	defer mu.Unlock()

	limiter, exists := userLimiters[userID]
	if !exists {
		limiter = newLimiter()
		userLimiters[userID] = limiter
	}
	return limiter
}

// Middleware
func rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ip := r.RemoteAddr

		// 1. IP limit
		ipLimiter := getIPLimiter(ip)
		if !ipLimiter.Allow() {
			http.Error(w, "Too many requests (IP)", http.StatusTooManyRequests)
			return
		}

		// 2. User limit (if logged in)
		userID := r.Header.Get("X-User-ID") // simulate auth
		if userID != "" {
			userLimiter := getUserLimiter(userID)
			if !userLimiter.Allow() {
				http.Error(w, "Too many requests (User)", http.StatusTooManyRequests)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// Example handler
func helloHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello, world!"))
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", helloHandler)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      rateLimitMiddleware(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	server.ListenAndServe()
}
