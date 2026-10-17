package httpapi

import (
	"net/http"
	"sync"
	"time"
)

// rateLimiter is a per-key token bucket, used to slow down credential
// stuffing against the login endpoint. It is in-process: with several
// apiserver replicas each enforces its own budget, so the effective limit is
// (replicas x rate). That is fine for blunting brute force; a shared store
// (Redis) would be needed for an exact global limit.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens replenished per second
	burst   float64 // bucket capacity
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(ratePerSecond, burst float64) *rateLimiter {
	if ratePerSecond <= 0 {
		ratePerSecond = 0.2 // 12 per minute
	}
	if burst <= 0 {
		burst = 10
	}
	return &rateLimiter{
		buckets: make(map[string]*bucket),
		rate:    ratePerSecond,
		burst:   burst,
		now:     time.Now,
	}
}

// allow consumes one token for key, returning false when the bucket is empty.
func (l *rateLimiter) allow(key string) bool {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	// Refill for elapsed time, capped at burst.
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	// Opportunistic sweep so a flood of distinct keys cannot grow the map
	// without bound. Full buckets are indistinguishable from idle ones.
	if len(l.buckets) > 4096 {
		l.sweepLocked(now)
	}
	return true
}

func (l *rateLimiter) sweepLocked(now time.Time) {
	for k, b := range l.buckets {
		if now.Sub(b.last) > 10*time.Minute {
			delete(l.buckets, k)
		}
	}
}

// middleware returns a handler that rejects callers over budget with 429.
// It keys on client IP; the login handler adds a second, per-username limiter.
func (l *rateLimiter) middleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		next(w, r)
	}
}
