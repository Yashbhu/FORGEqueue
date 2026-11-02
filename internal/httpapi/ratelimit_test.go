package httpapi

import (
	"testing"
	"time"
)

func TestRateLimiterBurstThenDeny(t *testing.T) {
	now := time.Unix(0, 0)
	l := newRateLimiter(1, 3) // 1 token/sec, burst 3
	l.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if !l.allow("1.2.3.4") {
			t.Fatalf("burst request %d denied, want allowed", i+1)
		}
	}
	if l.allow("1.2.3.4") {
		t.Fatal("request over burst allowed, want denied")
	}

	// Two seconds later two tokens have refilled.
	now = now.Add(2 * time.Second)
	if !l.allow("1.2.3.4") || !l.allow("1.2.3.4") {
		t.Fatal("refilled tokens denied, want allowed")
	}
	if l.allow("1.2.3.4") {
		t.Fatal("bucket not emptied after consuming refill")
	}
}

func TestRateLimiterIsolatesKeys(t *testing.T) {
	l := newRateLimiter(1, 1)
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }

	if !l.allow("a") {
		t.Fatal("first key denied")
	}
	if l.allow("a") {
		t.Fatal("second call for same key allowed")
	}
	// A different key has its own bucket.
	if !l.allow("b") {
		t.Fatal("independent key denied")
	}
}
