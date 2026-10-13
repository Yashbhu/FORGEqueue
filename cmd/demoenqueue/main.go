package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	queuev1 "forgequeue/proto/v1"
)

func main() {
	rounds := flag.Int("n", 1, "how many times to send the batch of tasks")
	interval := flag.Duration("interval", 0, "wait between rounds")
	addr := flag.String("gateway", "localhost:50051", "gateway gRPC address")
	token := flag.String("token", "", "bearer token (gateway rejects anonymous calls)")
	flag.Parse()

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := queuev1.NewQueueServiceClient(conn)

	// Auth header is applied per-call; unauthenticated calls hit the
	// interceptor's Unauthenticated error.
	authorized := func(ctx context.Context) context.Context {
		if *token != "" {
			return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
		}
		return ctx
	}

	// Two 8s sleeps per round so something is in flight when you screenshot.
	sleep8 := []byte(`{"duration_ms":8000}`)

	jobs := []struct {
		kind    string
		payload []byte
	}{
		{"echo", []byte("hello from forgequeue")},
		{"echo", []byte("second task")},
		{"echo", []byte("third task")},
		{"sleep", sleep8},
		{"sleep", sleep8},
		{"fail", []byte("fails on purpose")},
	}

	sent := 0
	start := time.Now()

	for r := 1; r <= *rounds; r++ {
		if *rounds > 1 {
			fmt.Printf("round %d/%d\n", r, *rounds)
		}
		for _, j := range jobs {
			// Time out each task on its own so one slow round does not abort
			// the rest.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := c.EnqueueTask(authorized(ctx), &queuev1.EnqueueTaskRequest{
				TaskType:     j.kind,
				Payload:      j.payload,
				MaxRetries:   3,
				DelaySeconds: 0,
			})
			cancel()
			if err != nil {
				log.Fatalf("enqueue %s: %v", j.kind, err)
			}
			fmt.Printf("  %-6s -> %s  %s\n", j.kind, resp.TaskId, resp.Status)
			sent++
		}
		if r < *rounds && *interval > 0 {
			time.Sleep(*interval)
		}
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "enqueued\t%d tasks in %s\n", sent, time.Since(start).Round(time.Millisecond))
	fmt.Fprintf(w, "per round\t%d tasks (3 echo, 2 sleep 8s, 1 fail)\n", len(jobs))
	w.Flush()
	fmt.Println("\nwatch the graphs:")
	fmt.Println("  http://localhost:3000  ->  Dashboards  ->  ForgeQueue  ->  ForgeQueue Overview")
	fmt.Println("  http://localhost:9095  ->  paste a PromQL query, see the Query box note in README")
}
