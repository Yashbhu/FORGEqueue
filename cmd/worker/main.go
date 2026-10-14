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

	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelBoot()

	wp, err := worker.NewWorkerPool(redisAddr, concurrency)
	if err != nil {
		log.Fatalf("failed to create worker pool: %v", err)
	}

	// A batch is claimed and ACKed in one round trip but processed one task
	// at a time.
	wp.WithBatchSize(batchSize)

	// No scrape address means metrics stay off.
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

	// Optional. With a DSN set, the reconciler heals tasks that are in
	// Postgres but not yet in Redis.
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

	// Log the handler list so a task type nobody handles shows up at startup
	// instead of as timeouts later.
	registered := handlers.Default()
	for taskType, h := range registered {
		wp.RegisterHandler(taskType, h)
	}
	log.Printf("handlers registered: %s", sortedKeys(registered))

	// A pool with no handlers would claim tasks it cannot process.
	if len(registered) == 0 {
		log.Fatal("no handlers registered: the worker would claim tasks it cannot process")
	}

	// Start the scrape endpoint before the pool.
	if metricsSrv != nil {
		if err := metricsSrv.Start(); err != nil {
			log.Fatalf("failed to start metrics server: %v", err)
		}
		log.Printf("metrics endpoint online on %s/metrics", metricsAddr)
	}

	// SIGINT is Ctrl-C, SIGTERM is what the container runtime sends.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	wp.Start()

	sig := <-shutdown
	log.Printf("received shutdown signal: %v", sig)

	// Cancels the pool context and waits for in-flight handlers. Tasks still
	// holding a lease are reclaimed by recovery after it expires.
	wp.Stop()
	log.Println("worker pool stopped")

	// Flush buffered metrics after the pool is idle.
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

// envInt returns key parsed as an int, or def when it is unset, empty, or not
// a valid number. A bad value falls back to def and is logged.
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

// sortedKeys returns map keys in a stable order for logging.
func sortedKeys(m map[string]handlers.WorkerHandler) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
