// Package metrics wires OpenTelemetry instruments to a Prometheus registry.
// Instruments are always non-nil, even when telemetry is disabled.
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

// Metrics holds every instrument the queue records into. Fields stay usable
// when telemetry is off, they just become no-ops.
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

	// Queue depths are gauges sampled from Redis. Up/down counters would
	// drift, since a missed decrement cannot be corrected.
	QueueReady     metric.Int64Gauge
	QueueInflight  metric.Int64Gauge
	QueueScheduled metric.Int64Gauge
	QueueDLQ       metric.Int64Gauge

	ClaimLatency     metric.Float64Histogram
	ACKLatency       metric.Float64Histogram
	HeartbeatLatency metric.Float64Histogram
	TaskDuration     metric.Float64Histogram
}

// DefaultLatencyBuckets covers redis calls from sub-millisecond up to ten
// seconds.
var DefaultLatencyBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// DefaultTaskBuckets spans a no-op handler up to a long-running one.
var DefaultTaskBuckets = []float64{
	0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300,
}

// New builds the instruments and a private Prometheus registry.
func New() (*Metrics, error) {
	reg := prometheus.NewRegistry()

	exporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		// Scope and target_info labels are constant here, so drop them to
		// keep the series names readable.
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

// NewNop returns a Metrics that records nothing.
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

// QueueDepths is a snapshot of how many tasks sit in each queue.
type QueueDepths struct {
	Ready     int64
	Inflight  int64
	Scheduled int64
	Dead      int64
}

// RecordQueueDepths publishes a depth snapshot to the queue gauges. Zero is
// recorded too, otherwise a drained queue leaves a stale reading behind.
func (m *Metrics) RecordQueueDepths(ctx context.Context, d QueueDepths) {
	m.QueueReady.Record(ctx, d.Ready)
	m.QueueInflight.Record(ctx, d.Inflight)
	m.QueueScheduled.Record(ctx, d.Scheduled)
	m.QueueDLQ.Record(ctx, d.Dead)
}

// DepthSampler periodically publishes queue depths read from Redis. Both the
// gateway and workers can run one, so read them with max() not sum().
type DepthSampler struct {
	metrics   *Metrics
	client    redis.Cmdable
	interval  time.Duration
	onFailure func(error)
}

// DefaultDepthInterval is how often a DepthSampler refreshes the gauges.
const DefaultDepthInterval = 5 * time.Second

// NewDepthSampler returns a sampler reading depths from client. A nil
// onFailure swallows errors, which suits tests.
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

// Run publishes depths until ctx is cancelled. Publishes once up front so
// the first scrape does not wait a full interval.
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

// Publish samples the depths once and records them. On failure the gauges are
// left untouched, so an unreachable Redis is not shown as a drained queue.
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

// Depths reads the depth of every queue in one pipelined round trip.
func (s *DepthSampler) Depths(ctx context.Context) (QueueDepths, error) {
	// No client is a wiring mistake. Return an error rather than panic.
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

// Redis key names holding each queue.
const (
	keyReady     = "queue:tasks:immediate"
	keyInflight  = "queue:tasks:inflight"
	keyScheduled = "queue:tasks:scheduled"
	keyDead      = "queue:tasks:dead"
)

// Registry exposes the underlying registry.
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

// NewServer returns a server serving m on addr. A blank addr uses :9090.
func NewServer(addr string, m *Metrics) *Server {
	if addr == "" {
		addr = ":9090"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

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
