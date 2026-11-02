package redisutil

import (
	"crypto/tls"
	"testing"
)

func TestOptionsFromEnvPassword(t *testing.T) {
	t.Setenv("FORGEQUEUE_REDIS_PASSWORD", "s3cret")
	t.Setenv("FORGEQUEUE_REDIS_DB", "3")

	opts, err := OptionsFromEnv("redis:6379", 0)
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.Password != "s3cret" {
		t.Errorf("password = %q, want s3cret", opts.Password)
	}
	if opts.DB != 3 {
		t.Errorf("db = %d, want 3", opts.DB)
	}
	if opts.PoolSize <= 0 {
		t.Errorf("pool size = %d, want > 0 default", opts.PoolSize)
	}
	if opts.TLSConfig != nil {
		t.Errorf("TLS unexpectedly enabled")
	}
}

func TestOptionsFromEnvTLS(t *testing.T) {
	t.Setenv("FORGEQUEUE_REDIS_TLS", "true")
	t.Setenv("FORGEQUEUE_REDIS_TLS_INSECURE", "true")

	opts, err := OptionsFromEnv("redis:6379", 10)
	if err != nil {
		t.Fatalf("OptionsFromEnv: %v", err)
	}
	if opts.TLSConfig == nil {
		t.Fatal("expected TLS config")
	}
	if !opts.TLSConfig.InsecureSkipVerify {
		t.Errorf("expected InsecureSkipVerify")
	}
	if opts.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("min TLS version = %d, want TLS 1.2", opts.TLSConfig.MinVersion)
	}
}

func TestOptionsFromEnvBadDB(t *testing.T) {
	t.Setenv("FORGEQUEUE_REDIS_DB", "not-a-number")
	if _, err := OptionsFromEnv("redis:6379", 1); err == nil {
		t.Fatal("expected error for non-numeric FORGEQUEUE_REDIS_DB")
	}
}
