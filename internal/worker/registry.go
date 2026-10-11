package worker

import (
	"context"
	"encoding/json"
	"time"
)

// metaKey is the Redis key holding this pool's registration document.
func (wp *WorkerPool) metaKey() string {
	return workerMetaPrefix + wp.id + ":meta"
}

// leasesKey is the Redis set of task IDs this pool is currently holding
// leases for. The dashboard reads it to show what a worker is processing.
func (wp *WorkerPool) leasesKey() string {
	return workerMetaPrefix + wp.id + ":leases"
}

// publishMeta writes the registration document and refreshes its TTL. It is
// called once on Start and then on every heartbeat tick. Best effort: a
// failure to publish only hides the worker from the dashboard, it never
// stops task processing.
func (wp *WorkerPool) publishMeta() {
	meta := workerMeta{
		ID:            wp.id,
		Hostname:      wp.hostname,
		Version:       wp.version,
		Status:        "active",
		StartedAt:     wp.startedAt.UTC().Format(time.RFC3339Nano),
		LastHeartbeat: time.Now().UTC().Format(time.RFC3339Nano),
		Concurrency:   wp.concurrencyLimit,
	}
	body, err := json.Marshal(meta)
	if err != nil {
		return
	}

	ctx := context.Background()
	pipe := wp.redisClient.TxPipeline()
	pipe.SAdd(ctx, workerRegistrySet, wp.id)
	pipe.Set(ctx, wp.metaKey(), body, workerMetaTTL)
	pipe.Expire(ctx, wp.leasesKey(), workerMetaTTL)
	_, _ = pipe.Exec(ctx)
}

// registrationLoop republishes the worker's identity on a fixed interval so
// the TTL never lapses while the pool is alive.
func (wp *WorkerPool) registrationLoop() {
	ticker := time.NewTicker(workerHeartbeatEvery)
	defer ticker.Stop()

	wp.publishMeta()
	for {
		select {
		case <-ticker.C:
			wp.publishMeta()
		case <-wp.quit:
			return
		}
	}
}

// deregister removes this worker from the registry so a clean shutdown
// disappears from the dashboard immediately instead of waiting for the TTL.
func (wp *WorkerPool) deregister() {
	ctx := context.Background()
	pipe := wp.redisClient.TxPipeline()
	pipe.SRem(ctx, workerRegistrySet, wp.id)
	pipe.Del(ctx, wp.metaKey())
	pipe.Del(ctx, wp.leasesKey())
	_, _ = pipe.Exec(ctx)
}

// markLeases records, in one round trip, the set of tasks this pool just
// claimed. The TTL is refreshed so the set outlives the work.
func (wp *WorkerPool) markLeases(taskIDs []string) {
	if len(taskIDs) == 0 {
		return
	}
	members := make([]interface{}, len(taskIDs))
	for i, id := range taskIDs {
		members[i] = id
	}
	ctx := context.Background()
	pipe := wp.redisClient.TxPipeline()
	pipe.SAdd(ctx, wp.leasesKey(), members...)
	pipe.Expire(ctx, wp.leasesKey(), workerMetaTTL)
	_, _ = pipe.Exec(ctx)
}

// clearLeases drops the given tasks from this pool's lease set once their
// outcomes are known (ACKed, retried or dead-lettered).
func (wp *WorkerPool) clearLeases(taskIDs []string) {
	if len(taskIDs) == 0 {
		return
	}
	members := make([]interface{}, len(taskIDs))
	for i, id := range taskIDs {
		members[i] = id
	}
	_ = wp.redisClient.SRem(context.Background(), wp.leasesKey(), members...).Err()
}
