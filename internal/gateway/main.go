package gateway

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"forgequeue/internal/auth"
	"forgequeue/internal/durable"
	"forgequeue/internal/metrics"
	queuev1 "forgequeue/proto/v1"
)

// server private struct having pointer to taskrouter to we can transfer downward to redis
type server struct {
	queuev1.UnimplementedQueueServiceServer
	trouter *TaskRouter
}

// task router knows what to do but soemthing to call this needed via rpc
// method belonging to the server struct acting as enquetas handler pushing data down the connection pool and generate uuid
// function must return 2 things a response pointer and an error as return response,error
func (s *server) EnqueueTask(
	ctx context.Context,
	req *queuev1.EnqueueTaskRequest,
) (*queuev1.EnqueueTaskResponse, error) {

	//gaurd clause: block empty task types
	if req.TaskType == "" {
		//no response and status error and grpc understands codes
		return nil, status.Error(codes.InvalidArgument, "task type cannot be empty")
	}

	//generate identity tracking token in memory
	taskID := uuid.New().String()

	// forward parameters to your routetask method (with explicit int64 type casting)
	err := s.trouter.RouteTask(ctx, taskID, req.TaskType, req.Payload, req.MaxRetries, int64(req.DelaySeconds))
	if err != nil {
		log.Printf("failed to route task: %v", err)
		return nil, status.Error(codes.Internal, "failed to route task")
	}

	//return response
	return &queuev1.EnqueueTaskResponse{
		TaskId:    taskID,
		Status:    "QUEUED",
		Timestamp: time.Now().Unix(),
	}, nil
}

// serverTLSOption builds gRPC transport credentials from the environment.
// Returns (nil, nil) when TLS is not configured, so the gateway still runs
// plaintext on a trusted pod network for local development.
//
//	FORGEQUEUE_GRPC_TLS_CERT       server certificate (PEM)
//	FORGEQUEUE_GRPC_TLS_KEY        server private key (PEM)
//	FORGEQUEUE_GRPC_TLS_CLIENT_CA  optional CA bundle; when set, clients must
//	                               present a certificate signed by it (mTLS)
func serverTLSOption() (grpc.ServerOption, error) {
	certFile := os.Getenv("FORGEQUEUE_GRPC_TLS_CERT")
	keyFile := os.Getenv("FORGEQUEUE_GRPC_TLS_KEY")
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("both FORGEQUEUE_GRPC_TLS_CERT and FORGEQUEUE_GRPC_TLS_KEY must be set")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caFile := os.Getenv("FORGEQUEUE_GRPC_TLS_CLIENT_CA"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates parsed from %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// raw netowrk listener socket opening
func Start() {
	// Initialize your custom Redis connection pool pointer
	redisAddress := os.Getenv("FORGEQUEUE_REDIS_ADDR")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	// Build telemetry only when a scrape address is set, otherwise the
	// router keeps no-op instruments. FORGEQUEUE_METRICS_ADDR is the scrape
	// endpoint (Prometheus default :9090).
	metricsAddr := os.Getenv("FORGEQUEUE_METRICS_ADDR")

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
		metricsSrv = metrics.NewServer(metricsAddr, m)
	}

	//wait for 3 seconds to boot up otherwise cancel
	// 3 seconds gaurd creating a boot context and checking if the task router is ready
	bootctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	//look for entire start to end then execute this
	defer cancel()

	taskRouter, err := NewTaskRouter(bootctx, redisAddress)
	if err != nil {
		log.Fatalf("failed to create task router: %v", err)
	}
	taskRouter.WithMetrics(telemetry)

	// Set FORGEQUEUE_PG_DSN to journal every enqueue to Postgres first
	// (survives crashes, heals Redis), otherwise the gateway runs Redis-only.
	if dsn := os.Getenv("FORGEQUEUE_PG_DSN"); dsn != "" {
		store, err := durable.NewPostgres(bootctx, dsn)
		if err != nil {
			log.Fatalf("failed to connect durable store: %v", err)
		}
		defer store.Close()
		taskRouter.WithStore(store)
		log.Println("durable journal online (postgres)")
	} else {
		log.Println("durable journal disabled (set FORGEQUEUE_PG_DSN to enable)")
	}

	//Open the raw TCP socket listener channel on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// The gRPC surface is authenticated with the same bearer tokens the HTTP
	// API issues, so the gateway shares its signing secret. Every method is
	// authorized against methodPermissions; unknown methods fail closed.
	authSvc, err := auth.NewFromEnv()
	if err != nil {
		log.Fatalf("failed to init auth: %v", err)
	}

	//Initialize the core gRPC microservice server container
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(newAuthInterceptor(authSvc)),
	}
	if tlsOpt, err := serverTLSOption(); err != nil {
		log.Fatalf("failed to configure gRPC TLS: %v", err)
	} else if tlsOpt != nil {
		opts = append(opts, tlsOpt)
		log.Println("gRPC TLS enabled on port :50051")
	}
	grpcServer := grpc.NewServer(opts...)

	//Register our server implementation with the gRPC engine
	queuev1.RegisterQueueServiceServer(
		grpcServer,
		&server{
			trouter: taskRouter,
		})

	log.Println("gateway network online on port :50051")

	// Serve /metrics alongside the gRPC listener so Prometheus has
	// something to poll.
	if metricsSrv != nil {
		if err := metricsSrv.Start(); err != nil {
			log.Fatalf("failed to start metrics server: %v", err)
		}
		log.Printf("metrics endpoint online on %s/metrics", metricsAddr)

		// The gateway reports queue depth too, otherwise the gauges stay
		// absent until a worker happens to publish.
		go metrics.NewDepthSampler(
			telemetry,
			taskRouter.redisClient,
			metrics.DefaultDepthInterval,
			func(err error) {
				log.Printf("queue depth sample failed: %v", err)
			},
		).Run(context.Background())
	}

	//Run the infinite network event execution loop
	// create an anonymous goroutine to serve the gRPC server
	go func() {
		err := grpcServer.Serve(listener)
		if err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("critical error running gRPC server loop: %v", err)
		}
	}()
	//signal channel to listen for shutdown signals with buffer 1
	shutdownChan := make(chan os.Signal, 1)
	//if the process receives an interrupt or termination signal, send it to the shutdown channel
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	//block the main thrrad until shutdown signal is received
	sh := <-shutdownChan
	log.Printf("received shutdown signal: %v", sh)
	grpcServer.GracefulStop()

	// Stop the scrape endpoint and flush metrics before exiting.
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

	//graceful shutdown
	log.Println("gateway network offline")

}
