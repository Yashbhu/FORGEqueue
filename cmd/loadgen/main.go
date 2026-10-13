// Command loadgen sends a steady mix of tasks over the gateway gRPC API so the
// dashboard has live traffic.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	queuev1 "forgequeue/proto/v1"
)

func main() {
	addr := flag.String("gateway", "localhost:50051", "gateway gRPC address")
	token := flag.String("token", "", "bearer token (gateway rejects anonymous calls)")
	rate := flag.Float64("rate", 40, "target enqueue rate in tasks per second")
	concurrency := flag.Int("concurrency", 8, "parallel enqueue goroutines")
	duration := flag.Duration("duration", 0, "run time; 0 means run until interrupted")
	echoPct := flag.Int("echo", 60, "percentage of echo tasks")
	sleepPct := flag.Int("sleep", 20, "percentage of sleep tasks")
	failPct := flag.Int("fail", 12, "percentage of fail tasks")
	delayPct := flag.Int("delay", 8, "percentage of delayed echo tasks")
	maxRetries := flag.Int("max-retries", 3, "max retries per task")
	sleepMaxMs := flag.Int("sleep-max-ms", 4000, "max sleep duration for sleep tasks")
	delayMaxS := flag.Int("delay-max-s", 20, "max delay seconds for delayed tasks")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	flag.Parse()

	// rand.Rand is not safe to share, so each goroutine seeds its own.
	rng := rand.New(rand.NewSource(*seed))

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect %s: %v", *addr, err)
	}
	defer conn.Close()
	client := queuev1.NewQueueServiceClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var sent, failed atomic.Int64
	var wg sync.WaitGroup

	// Report a running count once a second until the context ends.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cur := sent.Load()
				fmt.Printf("[loadgen] %.1fs  sent=%d  +%d/s  errors=%d  total_rate=%.1f/s\n",
					time.Since(start).Seconds(), cur, cur-last, failed.Load(),
					float64(cur)/time.Since(start).Seconds()+0.0001)
				last = cur
			}
		}
	}()

	// Split the target rate evenly across the goroutines.
	perWorker := *rate / float64(*concurrency)
	if perWorker <= 0 {
		perWorker = 1
	}
	interval := time.Duration(float64(time.Second) / perWorker)

	mix := []struct {
		kind string
		pct  int
	}{
		{"echo", *echoPct},
		{"sleep", *sleepPct},
		{"fail", *failPct},
		{"delay", *delayPct},
	}

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(rng.Int63()))
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					kind := pick(r, mix)
					req := buildRequest(r, kind, *maxRetries, *sleepMaxMs, *delayMaxS)

					// Cap each call so one slow enqueue cannot stall the
					// goroutine forever.
					callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if *token != "" {
						callCtx = metadata.AppendToOutgoingContext(callCtx, "authorization", "Bearer "+*token)
					}
					_, err := client.EnqueueTask(callCtx, req)
					cancel()
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						failed.Add(1)
						continue
					}
					sent.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("\n[loadgen] stopped: sent=%d errors=%d\n", sent.Load(), failed.Load())
}

// pick returns a task kind sampled from the weighted mix.
func pick(r *rand.Rand, mix []struct {
	kind string
	pct  int
}) string {
	total := 0
	for _, m := range mix {
		total += m.pct
	}
	if total <= 0 {
		return "echo"
	}
	n := r.Intn(total)
	for _, m := range mix {
		if n < m.pct {
			return m.kind
		}
		n -= m.pct
	}
	return "echo"
}

// buildRequest builds a request for kind, with random durations for sleep and
// delayed tasks.
func buildRequest(r *rand.Rand, kind string, maxRetries, sleepMaxMs, delayMaxS int) *queuev1.EnqueueTaskRequest {
	req := &queuev1.EnqueueTaskRequest{
		TaskType:   kind,
		MaxRetries: int32(maxRetries),
	}
	switch kind {
	case "sleep":
		ms := 250 + r.Intn(sleepMaxMs)
		req.TaskType = "sleep"
		req.Payload = []byte(fmt.Sprintf(`{"duration_ms":%d}`, ms))
	case "fail":
		req.TaskType = "fail"
		req.Payload = []byte(`{"reason":"loadgen failure"}`)
	case "delay":
		req.TaskType = "echo"
		req.Payload = []byte(`{"note":"delayed from loadgen"}`)
		req.DelaySeconds = int32(1 + r.Intn(delayMaxS))
	default:
		req.TaskType = "echo"
		req.Payload = []byte(fmt.Sprintf(`{"note":"loadgen echo","n":%d}`, r.Intn(1_000_000)))
	}
	return req
}
