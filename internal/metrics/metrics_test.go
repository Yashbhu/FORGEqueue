package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// scrape reads the Prometheus exposition output and returns it as a string.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want %d", rec.Code, http.StatusOK)
	}

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}

	return string(body)
}

// sample returns the value of a single metric series line, e.g.
// "forgequeue_tasks_enqueued_total". Returns "" when the series is absent.
func sample(t *testing.T, scrapeBody, series string) string {
	t.Helper()

	for _, line := range strings.Split(scrapeBody, "\n") {
		if strings.HasPrefix(line, series+" ") {
			return line
		}
	}

	return ""
}

func TestCountersRecordValues(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	ctx := context.Background()

	m.TasksEnqueued.Add(ctx, 3)
	m.TasksEnqueued.Add(ctx, 4)
	m.TasksProcessed.Add(ctx, 2)
	m.TasksRetried.Add(ctx, 1)
	m.TasksDLQ.Add(ctx, 1)
	m.LeaseLosses.Add(ctx, 5)

	body := scrape(t, m)

	tests := []struct {
		series string
		want   string
	}{
		{"forgequeue_tasks_enqueued_total", "forgequeue_tasks_enqueued_total 7"},
		{"forgequeue_tasks_processed_total", "forgequeue_tasks_processed_total 2"},
		{"forgequeue_tasks_retried_total", "forgequeue_tasks_retried_total 1"},
		{"forgequeue_tasks_dead_lettered_total", "forgequeue_tasks_dead_lettered_total 1"},
		{"forgequeue_lease_losses_total", "forgequeue_lease_losses_total 5"},
	}

	for _, tc := range tests {
		if got := sample(t, body, tc.series); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.series, got, tc.want)
		}
	}
}

func TestRecordQueueDepthsPublishesSnapshots(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	ctx := context.Background()

	m.RecordQueueDepths(ctx, QueueDepths{
		Ready:     3,
		Inflight:  1,
		Scheduled: 2,
		Dead:      1,
	})

	body := scrape(t, m)

	tests := []struct {
		series string
		want   string
	}{
		{"forgequeue_queue_ready", "forgequeue_queue_ready 3"},
		{"forgequeue_queue_inflight", "forgequeue_queue_inflight 1"},
		{"forgequeue_queue_scheduled", "forgequeue_queue_scheduled 2"},
		{"forgequeue_queue_dead", "forgequeue_queue_dead 1"},
	}

	for _, tc := range tests {
		if got := sample(t, body, tc.series); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.series, got, tc.want)
		}
	}
}

// A depth gauge is an absolute value, so a later snapshot replaces the
// earlier one. An up/down counter would have accumulated instead, which is
// exactly the behaviour that made these numbers drift before.
func TestRecordQueueDepthsReplacesPreviousSnapshot(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	ctx := context.Background()

	m.RecordQueueDepths(ctx, QueueDepths{Ready: 10})
	body := scrape(t, m)
	if got := sample(t, body, "forgequeue_queue_ready"); got != "forgequeue_queue_ready 10" {
		t.Errorf("first snapshot: %q, want %q", got, "forgequeue_queue_ready 10")
	}

	m.RecordQueueDepths(ctx, QueueDepths{Ready: 2})
	body = scrape(t, m)
	if got := sample(t, body, "forgequeue_queue_ready"); got != "forgequeue_queue_ready 2" {
		t.Errorf("second snapshot: %q, want %q", got, "forgequeue_queue_ready 2")
	}
}

// A drained queue must read zero, not the last non-zero value. This is the
// case that makes queue depth usable on a dashboard at all.
func TestRecordQueueDepthsReportsDrainedQueueAsZero(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	ctx := context.Background()

	m.RecordQueueDepths(ctx, QueueDepths{Ready: 7})
	m.RecordQueueDepths(ctx, QueueDepths{Ready: 0})

	if got := sample(t, scrape(t, m), "forgequeue_queue_ready"); got != "forgequeue_queue_ready 0" {
		t.Errorf("drained ready queue = %q, want %q", got, "forgequeue_queue_ready 0")
	}
}

func TestHistogramsRecordCountAndSum(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	ctx := context.Background()

	m.ClaimLatency.Record(ctx, 0.002)
	m.ACKLatency.Record(ctx, 0.004)
	m.HeartbeatLatency.Record(ctx, 0.001)
	m.TaskDuration.Record(ctx, 1.5)

	body := scrape(t, m)

	for _, series := range []string{
		"forgequeue_redis_claim_seconds_count",
		"forgequeue_redis_ack_seconds_count",
		"forgequeue_redis_heartbeat_seconds_count",
		"forgequeue_task_duration_seconds_count",
	} {
		if got := sample(t, body, series); got != series+" 1" {
			t.Errorf("%s = %q, want %q", series, got, series+" 1")
		}
	}

	// One 1.5s observation should show up as a sum of 1.5.
	if got, want := sample(t, body, "forgequeue_task_duration_seconds_sum"), "forgequeue_task_duration_seconds_sum 1.5"; got != want {
		t.Errorf("task duration sum = %q, want %q", got, want)
	}
}

// A nop Metrics must still be safe to call: the worker and router default to
// it so instrumentation never needs a nil check.
func TestNopMetricsAreUsableAndSilent(t *testing.T) {
	m := NewNop()
	ctx := context.Background()

	m.TasksEnqueued.Add(ctx, 10)
	m.RecordQueueDepths(ctx, QueueDepths{Ready: 1, Inflight: 1})
	m.LeaseLosses.Add(ctx, 3)
	m.ClaimLatency.Record(ctx, 0.5)
	m.TaskDuration.Record(ctx, 12)

	if m.Ready {
		t.Error("NewNop().Ready = true, want false")
	}

	if m.Registry() != nil {
		t.Error("NewNop().Registry() != nil, want nil")
	}

	// Shutdown on a nop sink is a no-op, not a panic.
	shutdownCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := m.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Nop shutdown error = %v, want nil", err)
	}
}

func TestServerServesMetricsEndpoint(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	// NewServer with a blank address falls back to the Prometheus default.
	srv := NewServer("", m)
	if srv.srv.Addr != ":9090" {
		t.Errorf("NewServer(\"\").Addr = %q, want \":9090\"", srv.srv.Addr)
	}

	// Drive the real mux rather than binding a port, so the test stays
	// hermetic.
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// A blank interval must fall back to the default rather than spinning a
// ticker at zero, which NewTicker would panic on.
func TestDepthSamplerRejectsNonPositiveInterval(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	for _, interval := range []time.Duration{0, -time.Second} {
		s := NewDepthSampler(m, nil, interval, nil)
		if s.interval != DefaultDepthInterval {
			t.Errorf("interval %v produced %v, want the %v default",
				interval, s.interval, DefaultDepthInterval)
		}
	}
}

// Run must publish immediately and then stop on cancellation, rather than
// leaking a goroutine after shutdown.
func TestDepthSamplerRunStopsOnContextCancel(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	s := NewDepthSampler(m, nil, time.Hour, nil)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// A sampler wired without a Redis client must report the mistake, not panic
// inside the caller's goroutine.
func TestDepthSamplerWithoutClientReturnsError(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.Shutdown(ctx)
	})

	s := NewDepthSampler(m, nil, time.Second, nil)

	if _, err := s.Depths(context.Background()); err == nil {
		t.Error("Depths() with no client = nil, want an error")
	}

	// Publish must swallow it so a broken sampler cannot kill the process.
	var reported error
	s = NewDepthSampler(m, nil, time.Second, func(err error) { reported = err })
	s.Publish(context.Background())

	if reported == nil {
		t.Error("onFailure was not called for a sampler with no client")
	}
}
