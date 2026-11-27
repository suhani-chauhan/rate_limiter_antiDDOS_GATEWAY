package main

import (
	"log"
	"net/http"

	"rate-limiter/blocking"
	"rate-limiter/limiter"
	"rate-limiter/middleware"
	"rate-limiter/redis"
)

func main() {

	// Connect to Redis
	err := redis.ConnectRedis()

	if err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	log.Println("Redis connected successfully")

	// Configure Token Bucket
	bucket := &limiter.TokenBucket{
		Client:     redis.Client,
		Capacity:   5,
		RefillRate: 0.5,
		KeyPrefix:  "rate:",
	}

	// Configure IP Blocker
	blocker := &blocking.IPBlocker{
		Client: redis.Client,
	}

	// Create middleware
	rateLimiter := &middleware.RateLimiterMiddleware{
		Bucket:  bucket,
		Blocker: blocker,
	}

	// Backend endpoint
	apiHandler := http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {

		w.Header().Set("Content-Type", "application/json")

		w.Write([]byte(`{"message":"Request successful!"}`))
	})

	// Apply middleware
	protectedHandler := rateLimiter.Handler(apiHandler)

	// Routes
	http.Handle("/api", protectedHandler)

	http.HandleFunc("/health", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.Write([]byte("Server is running"))
	})

	// Start server
	log.Println("Rate limiter running on http://localhost:8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
