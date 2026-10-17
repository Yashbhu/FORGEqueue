package redisutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// OptionsFromEnv builds redis.Options from the environment. Every service that
// talks to Redis (gateway, worker, apiserver) funnels through here so auth and
// TLS are configured in exactly one place.
//
//	FORGEQUEUE_REDIS_PASSWORD       AUTH password (empty disables AUTH)
//	FORGEQUEUE_REDIS_DB             database index, default 0
//	FORGEQUEUE_REDIS_TLS            "true" enables TLS
//	FORGEQUEUE_REDIS_TLS_INSECURE   "true" skips certificate verification (dev only)
//	FORGEQUEUE_REDIS_TLS_CA         path to a PEM CA bundle to trust
//
// A password with TLS turned off still authenticates; Redis just carries the
// credential in cleartext, which is acceptable only on a trusted pod network.
func OptionsFromEnv(addr string, poolSize int) (*redis.Options, error) {
	if poolSize <= 0 {
		poolSize = 10
	}
	opts := &redis.Options{
		Addr:     addr,
		PoolSize: poolSize,
	}

	if pw := os.Getenv("FORGEQUEUE_REDIS_PASSWORD"); pw != "" {
		opts.Password = pw
	}

	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_REDIS_DB")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("redisutil: invalid FORGEQUEUE_REDIS_DB %q", raw)
		}
		opts.DB = n
	}

	if isTruthy(os.Getenv("FORGEQUEUE_REDIS_TLS")) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if isTruthy(os.Getenv("FORGEQUEUE_REDIS_TLS_INSECURE")) {
			// VerifyPeerCertificate stays set, but the chain is not checked.
			cfg.InsecureSkipVerify = true
		}
		if caPath := os.Getenv("FORGEQUEUE_REDIS_TLS_CA"); caPath != "" {
			pem, err := os.ReadFile(caPath)
			if err != nil {
				return nil, fmt.Errorf("redisutil: read FORGEQUEUE_REDIS_TLS_CA: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("redisutil: no certificates parsed from %s", caPath)
			}
			cfg.RootCAs = pool
		}
		opts.TLSConfig = cfg
	}

	return opts, nil
}

// NewClient builds a client from the environment and verifies the connection
// before returning. A Redis that needs AUTH will fail fast here rather than at
// the first claim.
func NewClient(addr string, poolSize int, ctx context.Context) (*redis.Client, error) {
	opts, err := OptionsFromEnv(addr, poolSize)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
