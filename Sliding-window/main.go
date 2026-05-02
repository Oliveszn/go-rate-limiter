package main

import (
	"net/http"
	"sw/middleware"
	"sw/ratelimit"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	limiter := ratelimit.NewSlidingWindow(rdb, 100, time.Minute)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", handleData)
	rateLimited := middleware.Middleware(limiter, ratelimit.ByIP)(mux)

	http.ListenAndServe(":8080", rateLimited)
}

func handleData(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("hello from api"))
}
