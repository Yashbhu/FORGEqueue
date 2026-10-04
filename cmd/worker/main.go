package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"forgequeue/internal/durable"
	"forgequeue/internal/handlers"
	"forgequeue/internal/metrics"
	"forgequeue/internal/worker"
)

// Environment variables read at startup.
const (
	envRedisAddr     = "FORGEQUEUE_REDIS_ADDR"
	envConcurrency   = "FORGEQUEUE_WORKER_CONCURRENCY"
	envBatchSize     = "FORGEQUEUE_BATCH_SIZE"
	envMetricsAddr   = "FORGEQUEUE_METRICS_ADDR"
	envPostgresDSN   = "FORGEQUEUE_PG_DSN"
	defaultRedisAddr = "localhost:6379"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	redisAddr := envString(envRedisAddr, defaultRedisAddr)
	concurrency := envInt(envConcurrency, 4)
	batchSize := envInt(envBatchSize, 1)
	metricsAddr := os.Getenv(envMetricsAddr)

	log.Printf("worker starting: redis=%s concurrency=%d batch_size=%d",
		redisAddr, concurrency, batchSize)

	// fail fast
	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelBoot()

	wp, err := worker.NewWorkerPool(redisAddr, concurrency)
	if err != nil {
		log.Fatalf("failed to create worker pool: %v", err)
	}

	// Batching collapses the claim and ACK round trips for a whole batch
	// into one each. It is opt-in because a batch is handled sequentially,
	// so a large value trades per-task latency for throughput.
	wp.WithBatchSize(batchSize)

	// Telemetry is opt-in: with no scrape address configured the pool keeps
	// no-op instruments and opens no port.
	var (
		telemetry  *metrics.Metrics
		metricsSrv *metrics.Server
	)
	if metricsAddr != "" {
		m, err := metrics.New()
		if err != nil {
			log.Fatalf("failed to init telemetry: %v", err)
		}
		telemetry = m
		wp.WithMetrics(m)

		metricsSrv = metrics.NewServer(metricsAddr, m)
	}

	// The durable journal is optional and opt-in, matching the gateway. When
	// attached, the reconciler heals the window where a task is in Postgres
	// but not yet in Redis.
	if dsn := os.Getenv(envPostgresDSN); dsn != "" {
		store, err := durable.NewPostgres(bootCtx, dsn)
		if err != nil {
			log.Fatalf("failed to connect durable store: %v", err)
		}
		defer store.Close()
		wp.WithStore(store)
		log.Println("durable journal online (postgres)")
	} else {
		log.Println("durable journal disabled (set FORGEQUEUE_PG_DSN to enable)")
	}

	// Register the built-in handlers, and say which ones, so a task type
	// nobody handles is obvious from the startup log rather than from a
	// stream of timeouts later.
	registered := handlers.Default()
	for taskType, h := range registered {
		wp.RegisterHandler(taskType, h)
	}
	log.Printf("handlers registered: %s", sortedKeys(registered))

	// Fail before serving if there is nothing to serve. A pool that only
	// polls Redis with no handler registered looks healthy while silently
	// leaking every task it claims.
	if len(registered) == 0 {
		log.Fatal("no handlers registered: the worker would claim tasks it cannot process")
	}

	// The scrape endpoint goes up before the workers so telemetry is
	// reachable even if the pool is saturated.
	if metricsSrv != nil {
		if err := metricsSrv.Start(); err != nil {
			log.Fatalf("failed to start metrics server: %v", err)
		}
		log.Printf("metrics endpoint online on %s/metrics", metricsAddr)
	}

	// One signal channel for the whole process. SIGINT is Ctrl-C, SIGTERM is
	// what a container runtime sends on `docker stop` and on Kubernetes
	// pod deletion, so both need to shut down cleanly rather than kill the
	// process mid-task.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	wp.Start()

	// Block until a signal arrives.
	sig := <-shutdown
	log.Printf("received shutdown signal: %v", sig)

	// Stop accepting new claims and wait for in-flight handlers to return.
	// Stop cancels the pool context, which also stops the recovery,
	// promotion, and queue-depth loops. Tasks still holding a lease when
	// this returns are not lost: recovery reclaims them once the lease
	// expires.
	wp.Stop()
	log.Println("worker pool stopped")

	// Flush buffered metrics after the pool is idle, so a scrape during
	// shutdown still sees the final counts.
	if metricsSrv != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()

		if err := metricsSrv.Stop(stopCtx); err != nil {
			log.Printf("metrics server shutdown: %v", err)
		}
		if err := telemetry.Shutdown(stopCtx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}

	log.Println("worker offline")
}

// envString returns the value of key, or def when it is unset or empty.
func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt returns the value of key parsed as an int, or def when it is unset,
// empty, or not a valid number.
//
// A malformed value falls back to the default rather than exiting, on the
// grounds that a typo in an optional tuning knob should not stop the worker
// from processing tasks. The effective value is logged at startup either way,
// so a silently-ignored typo is still visible.
func envInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("%s=%q is not a number, using default %d", key, raw, def)
		return def
	}

	return v
}

// sortedKeys returns the map keys in a stable order so startup logs are
// comparable between runs and between replicas.
func sortedKeys(m map[string]handlers.WorkerHandler) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
