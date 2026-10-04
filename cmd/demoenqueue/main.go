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

	queuev1 "forgequeue/proto/v1"
)

func main() {
	rounds := flag.Int("n", 1, "how many times to send the batch of tasks")
	interval := flag.Duration("interval", 0, "wait between rounds")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	c := queuev1.NewQueueServiceClient(conn)

	// Two 8s sleeps per round. Long enough that the p95 graph has a real
	// curve in it, and that a task is still in flight whenever you go to
	// screenshot, which is what makes the queue-depth panel look alive.
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
			// Per-task timeout, not one timeout for the whole run: a slow
			// round should not abort the rounds after it.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := c.EnqueueTask(ctx, &queuev1.EnqueueTaskRequest{
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
