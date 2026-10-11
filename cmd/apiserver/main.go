package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgequeue/internal/auth"
	"forgequeue/internal/httpapi"
	"forgequeue/internal/redisutil"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	redisAddr := os.Getenv("FORGEQUEUE_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	apiAddr := os.Getenv("FORGEQUEUE_API_ADDR")
	if apiAddr == "" {
		apiAddr = ":8080"
	}

	authSvc, err := auth.NewFromEnv()
	if err != nil {
		log.Fatalf("failed to configure auth: %v", err)
	}

	bootCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rdb, err := redisutil.NewClient(redisAddr, 10, bootCtx)
	if err != nil {
		log.Fatalf("failed to connect to redis: %v", err)
	}

	srv := httpapi.NewServer(apiAddr, rdb, authSvc)
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start api server: %v", err)
	}
	log.Printf("api server online on %s", apiAddr)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	<-shutdown

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	_ = srv.Stop(stopCtx)
	log.Println("api server offline")
}
