// Package metrics wires OpenTelemetry instruments to a Prometheus registry
// so the queue's behaviour is observable over HTTP.
//
// Instruments are always non-nil: a disabled Metrics (via NewNop) still
// returns valid no-op instruments, so call sites never need nil checks.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics holds every instrument the queue records into. All fields are
// usable even when telemetry is disabled; they become no-ops then.
type Metrics struct {
	Ready     bool
	provider  *sdkmetric.MeterProvider
	registry  *prometheus.Registry
	latencyBs []float64
	taskBs    []float64

	TasksEnqueued  metric.Int64Counter
	TasksProcessed metric.Int64Counter
	TasksRetried   metric.Int64Counter
	TasksDLQ       metric.Int64Counter
	LeaseLosses    metric.Int64Counter

	// Queue depths are observable gauges rather than up/down counters.
	//
	// An up/down counter needs a matching decrement on every claim, ACK,
	// promotion, failure and recovery, spread across the gateway and every
	// worker process. Any missed transition leaves the value permanently
	// wrong with no way to correct it, and separate processes would each
	// export their own partial total. Redis already holds the
	// authoritative depth, so these gauges are sampled from it instead.
	QueueReady     metric.Int64Gauge
	QueueInflight  metric.Int64Gauge
	QueueScheduled metric.Int64Gauge
	QueueDLQ       metric.Int64Gauge

	ClaimLatency     metric.Float64Histogram
	ACKLatency       metric.Float64Histogram
	HeartbeatLatency metric.Float64Histogram
	TaskDuration     metric.Float64Histogram
}

// DefaultLatencyBuckets covers sub-millisecond Redis calls up to ten seconds,
// which is the range a single claim/ack/heartbeat round trip can occupy.
var DefaultLatencyBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// DefaultTaskBuckets spans a no-op handler up to a two minute handler.
var DefaultTaskBuckets = []float64{
	0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300,
}

// New builds the instruments and a private Prometheus registry. Call
// Handler to expose it, or pass Registry to NewServer.
func New() (*Metrics, error) {
	reg := prometheus.NewRegistry()

	exporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		// Scope and target_info labels are constant for this process and
		// carry no information a dashboard can use. Dropping them keeps
		// the series names readable instead of drowning every line in
		// otel_scope_* boilerplate.
		otelprom.WithoutScopeInfo(),
		otelprom.WithoutTargetInfo(),
	)
	if err != nil {
		return nil, err
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("forgequeue")

	m := &Metrics{
		Ready:     true,
		provider:  provider,
		registry:  reg,
		latencyBs: DefaultLatencyBuckets,
		taskBs:    DefaultTaskBuckets,
	}

	if m.TasksEnqueued, err = meter.Int64Counter(
		"forgequeue.tasks.enqueued",
		metric.WithDescription("Tasks accepted by the gateway."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.TasksProcessed, err = meter.Int64Counter(
		"forgequeue.tasks.processed",
		metric.WithDescription("Tasks acknowledged after a successful handler run."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.TasksRetried, err = meter.Int64Counter(
		"forgequeue.tasks.retried",
		metric.WithDescription("Task attempts rescheduled with backoff."),
		metric.WithUnit("{attempt}"),
	); err != nil {
		return nil, err
	}

	if m.TasksDLQ, err = meter.Int64Counter(
		"forgequeue.tasks.dead_lettered",
		metric.WithDescription("Tasks moved to the dead-letter queue."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.LeaseLosses, err = meter.Int64Counter(
		"forgequeue.lease.losses",
		metric.WithDescription("Heartbeats rejected because the lease was no longer held."),
		metric.WithUnit("{event}"),
	); err != nil {
		return nil, err
	}

	if m.QueueReady, err = meter.Int64Gauge(
		"forgequeue.queue.ready",
		metric.WithDescription("Tasks waiting on the ready list."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.QueueInflight, err = meter.Int64Gauge(
		"forgequeue.queue.inflight",
		metric.WithDescription("Tasks holding a lease."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.QueueScheduled, err = meter.Int64Gauge(
		"forgequeue.queue.scheduled",
		metric.WithDescription("Tasks waiting for a delayed or backoff run time."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.QueueDLQ, err = meter.Int64Gauge(
		"forgequeue.queue.dead",
		metric.WithDescription("Tasks parked in the dead-letter queue."),
		metric.WithUnit("{task}"),
	); err != nil {
		return nil, err
	}

	if m.ClaimLatency, err = meter.Float64Histogram(
		"forgequeue.redis.claim",
		metric.WithDescription("Round-trip latency of the claim script."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(m.latencyBs...),
	); err != nil {
		return nil, err
	}

	if m.ACKLatency, err = meter.Float64Histogram(
		"forgequeue.redis.ack",
		metric.WithDescription("Round-trip latency of the ack script."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(m.latencyBs...),
	); err != nil {
		return nil, err
	}

	if m.HeartbeatLatency, err = meter.Float64Histogram(
		"forgequeue.redis.heartbeat",
		metric.WithDescription("Round-trip latency of the heartbeat script."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(m.latencyBs...),
	); err != nil {
		return nil, err
	}

	if m.TaskDuration, err = meter.Float64Histogram(
		"forgequeue.task.duration",
		metric.WithDescription("Wall time the handler spent on the task."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(m.taskBs...),
	); err != nil {
		return nil, err
	}

	return m, nil
}

// NewNop returns a Metrics whose instruments record nothing. Useful in tests
// and anywhere telemetry is not wired up.
func NewNop() *Metrics {
	meter := noop.NewMeterProvider().Meter("forgequeue")
	m := &Metrics{latencyBs: DefaultLatencyBuckets, taskBs: DefaultTaskBuckets}

	m.TasksEnqueued, _ = meter.Int64Counter("forgequeue.tasks.enqueued")
	m.TasksProcessed, _ = meter.Int64Counter("forgequeue.tasks.processed")
	m.TasksRetried, _ = meter.Int64Counter("forgequeue.tasks.retried")
	m.TasksDLQ, _ = meter.Int64Counter("forgequeue.tasks.dead_lettered")
	m.LeaseLosses, _ = meter.Int64Counter("forgequeue.lease.losses")

	m.QueueReady, _ = meter.Int64Gauge("forgequeue.queue.ready")
	m.QueueInflight, _ = meter.Int64Gauge("forgequeue.queue.inflight")
	m.QueueScheduled, _ = meter.Int64Gauge("forgequeue.queue.scheduled")
	m.QueueDLQ, _ = meter.Int64Gauge("forgequeue.queue.dead")

	m.ClaimLatency, _ = meter.Float64Histogram("forgequeue.redis.claim")
	m.ACKLatency, _ = meter.Float64Histogram("forgequeue.redis.ack")
	m.HeartbeatLatency, _ = meter.Float64Histogram("forgequeue.redis.heartbeat")
	m.TaskDuration, _ = meter.Float64Histogram("forgequeue.task.duration")

	return m
}

// QueueDepths is a point-in-time snapshot of how many tasks sit in each
// queue. Collected by Redis so it reflects every process, not just the one
// reporting it.
type QueueDepths struct {
	Ready     int64
	Inflight  int64
	Scheduled int64
	Dead      int64
}

// RecordQueueDepths publishes a depth snapshot to the queue gauges.
//
// It records the ready queue even when it is empty: a zero is a meaningful
// value here ("the queue is drained"), and skipping it would leave the last
// non-zero reading on the dashboard forever.
func (m *Metrics) RecordQueueDepths(ctx context.Context, d QueueDepths) {
	m.QueueReady.Record(ctx, d.Ready)
	m.QueueInflight.Record(ctx, d.Inflight)
	m.QueueScheduled.Record(ctx, d.Scheduled)
	m.QueueDLQ.Record(ctx, d.Dead)
}

// DepthSampler periodically publishes queue depths read from Redis.
//
// Both the gateway and the worker can run one: they are separate processes
// reporting the same shared queues, and each scrape target simply states
// the current depth. Note that a dashboard should read these with max() or
// last() rather than sum(), since more than one process may report the same
// queue.
type DepthSampler struct {
	metrics   *Metrics
	client    redis.Cmdable
	interval  time.Duration
	onFailure func(error)
}

// DefaultDepthInterval is how often a DepthSampler refreshes the gauges.
const DefaultDepthInterval = 5 * time.Second

// NewDepthSampler returns a sampler reading depths from client.
//
// A nil onFailure swallows errors silently, which suits tests. Production
// callers should log so a broken Redis does not go unnoticed just because
// the queue-depth panel went flat.
func NewDepthSampler(
	m *Metrics,
	client redis.Cmdable,
	interval time.Duration,
	onFailure func(error),
) *DepthSampler {
	if interval <= 0 {
		interval = DefaultDepthInterval
	}

	return &DepthSampler{
		metrics:   m,
		client:    client,
		interval:  interval,
		onFailure: onFailure,
	}
}

// Run publishes depths until ctx is cancelled. It publishes once before
// waiting, so the first scrape does not have to wait a full interval for
// numbers that are already knowable.
func (s *DepthSampler) Run(ctx context.Context) {
	s.Publish(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.Publish(ctx)

		case <-ctx.Done():
			return
		}
	}
}

// Publish samples the depths once and records them.
//
// On failure the gauges are left untouched. Overwriting them with zero would
// be actively misleading: an unreachable Redis and a genuinely drained queue
// are indistinguishable on a dashboard, and only one of them is healthy.
func (s *DepthSampler) Publish(ctx context.Context) {
	depths, err := s.Depths(ctx)
	if err != nil {
		if s.onFailure != nil && !errors.Is(err, context.Canceled) {
			s.onFailure(err)
		}
		return
	}

	s.metrics.RecordQueueDepths(ctx, depths)
}

// Depths reads the current depth of every queue in a single round trip.
//
// The four commands are pipelined, so this costs one network round trip
// rather than four and the numbers come from roughly the same instant.
func (s *DepthSampler) Depths(ctx context.Context) (QueueDepths, error) {
	// A sampler with no client is a wiring mistake. Return an error so the
	// failure is reported rather than panicking inside the caller's
	// goroutine, which would take the whole process down.
	if s.client == nil {
		return QueueDepths{}, errors.New("depth sampler has no redis client")
	}

	pipe := s.client.Pipeline()

	ready := pipe.LLen(ctx, keyReady)
	inflight := pipe.ZCard(ctx, keyInflight)
	scheduled := pipe.ZCard(ctx, keyScheduled)
	dead := pipe.LLen(ctx, keyDead)

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return QueueDepths{}, err
	}

	return QueueDepths{
		Ready:     ready.Val(),
		Inflight:  inflight.Val(),
		Scheduled: scheduled.Val(),
		Dead:      dead.Val(),
	}, nil
}

// Redis key names holding each queue. Exported so a dashboard config or a
// test can refer to the same constants the sampler reads.
const (
	keyReady     = "queue:tasks:immediate"
	keyInflight  = "queue:tasks:inflight"
	keyScheduled = "queue:tasks:scheduled"
	keyDead      = "queue:tasks:dead"
)

// Registry exposes the underlying registry so callers can register extra
// collectors or serve it themselves.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Shutdown flushes buffered metric data.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if !m.Ready || m.provider == nil {
		return nil
	}
	return m.provider.Shutdown(ctx)
}

// Server exposes Handler over HTTP for a scraper.
type Server struct {
	srv *http.Server
}

// NewServer returns a server that serves m on addr. A blank addr serves
// /metrics on :9090.
func NewServer(addr string, m *Metrics) *Server {
	if addr == "" {
		addr = ":9090"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

// Start begins serving in the background.
func (s *Server) Start() error {
	go func() {
		//nolint:errcheck // background scrape endpoint
		s.srv.ListenAndServe()
	}()
	return nil
}

// Stop shuts the server down gracefully.
func (s *Server) Stop(ctx context.Context) error { return s.srv.Shutdown(ctx) }
