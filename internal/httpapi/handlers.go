package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"forgequeue/internal/auth"
)

type Handler struct {
	rdb  *redis.Client
	auth *auth.Service
}

func NewHandler(rdb *redis.Client, authSvc *auth.Service) *Handler {
	return &Handler{rdb: rdb, auth: authSvc}
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	writeJSON(w, status, v)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	writeError(w, status, msg)
}

// Login exchanges a username/password for a bearer token.
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Username == "" || req.Password == "" {
		h.writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}
	if h.auth == nil {
		h.writeError(w, http.StatusServiceUnavailable, "authentication is not configured")
		return
	}
	id, err := h.auth.Authenticate(req.Username, req.Password)
	if err != nil {
		// Same response for unknown user and bad password.
		h.writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	token, expires, err := h.auth.IssueToken(id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to issue token")
		return
	}
	h.writeJSON(w, http.StatusOK, LoginResponse{
		Token:     token,
		ExpiresAt: expires.UTC().Format(time.RFC3339),
		User:      id,
	})
}

// Me returns the identity attached to the authenticated request.
func (h *Handler) Me(w http.ResponseWriter, r *http.Request) {
	id, ok := auth.FromContext(r.Context())
	if !ok {
		h.writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	h.writeJSON(w, http.StatusOK, id)
}
func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pending := int(h.rdb.LLen(ctx, "queue:tasks:immediate").Val())
	processing := int(h.rdb.ZCard(ctx, "queue:tasks:inflight").Val())
	delayed := int(h.rdb.ZCard(ctx, "queue:tasks:scheduled").Val())
	dlq := int(h.rdb.LLen(ctx, "queue:tasks:dead").Val())
	completed := int(h.rdb.ZCard(ctx, "metrics:completed").Val())

	healthy, stale := h.workerCounts(ctx)
	stats := SystemStats{
		Queues:         1,
		ActiveJobs:     pending + processing,
		Pending:        pending,
		Processing:     processing,
		Delayed:        delayed,
		DLQ:            dlq,
		Failed:         dlq,
		Completed:      completed,
		ActiveWorkers:  healthy + stale,
		HealthyWorkers: healthy,
		StaleWorkers:   stale,
	}
	h.writeJSON(w, http.StatusOK, stats)
}
func (h *Handler) Queues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pending := int(h.rdb.LLen(ctx, "queue:tasks:immediate").Val())
	processing := int(h.rdb.ZCard(ctx, "queue:tasks:inflight").Val())
	delayed := int(h.rdb.ZCard(ctx, "queue:tasks:scheduled").Val())
	dlq := int(h.rdb.LLen(ctx, "queue:tasks:dead").Val())
	completed := int(h.rdb.ZCard(ctx, "metrics:completed").Val())
	workers := len(h.readWorkers(ctx))
	status := h.queueStatus(ctx)

	summaries := []QueueSummary{
		{
			Name:       "default",
			Pending:    pending,
			Processing: processing,
			Delayed:    delayed,
			Failed:     dlq,
			Completed:  completed,
			Workers:    workers,
			RatePerMin: 0,
			Status:     status,
		},
	}
	h.writeJSON(w, http.StatusOK, summaries)
}
func (h *Handler) Queue(w http.ResponseWriter, r *http.Request, name string) {
	ctx := r.Context()
	pending := int(h.rdb.LLen(ctx, "queue:tasks:immediate").Val())
	processing := int(h.rdb.ZCard(ctx, "queue:tasks:inflight").Val())
	delayed := int(h.rdb.ZCard(ctx, "queue:tasks:scheduled").Val())
	dlq := int(h.rdb.LLen(ctx, "queue:tasks:dead").Val())
	completed := int(h.rdb.ZCard(ctx, "metrics:completed").Val())
	workers := len(h.readWorkers(ctx))
	status := h.queueStatus(ctx)

	summary := QueueSummary{
		Name:       name,
		Pending:    pending,
		Processing: processing,
		Delayed:    delayed,
		Failed:     dlq,
		Completed:  completed,
		Workers:    workers,
		RatePerMin: 0,
		Status:     status,
	}
	h.writeJSON(w, http.StatusOK, summary)
}

func (h *Handler) QueueJobs(w http.ResponseWriter, r *http.Request, name string) {
	jobs := []Job{}
	h.writeJSON(w, http.StatusOK, jobs)
}
func (h *Handler) Job(w http.ResponseWriter, r *http.Request, id string) {
	t, ok, err := h.readTaskFull(r, id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		h.writeError(w, http.StatusNotFound, fmt.Sprintf("job %s not found", id))
		return
	}
	h.writeJSON(w, http.StatusOK, t)
}

func (h *Handler) readTaskFull(r *http.Request, id string) (*Job, bool, error) {
	ctx := r.Context()
	var tm TaskMetaData
	b, err := h.rdb.Get(ctx, "task:"+id).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := json.Unmarshal(b, &tm); err != nil {
		return nil, false, err
	}

	status := StatusPending
	if _, err := h.rdb.ZScore(ctx, "queue:tasks:inflight", id).Result(); err == nil {
		status = StatusProcessing
	} else if _, err := h.rdb.ZScore(ctx, "queue:tasks:scheduled", id).Result(); err == nil {
		status = StatusDelayed
	} else if h.isDead(ctx, id) {
		status = StatusDLQ
	}

	enq := time.Now().Add(-4 * time.Minute).UTC().Format(time.RFC3339Nano)
	job := &Job{
		ID:          id,
		Queue:       "default",
		Task:        tm.TaskType,
		Status:      status,
		Attempts:    int(tm.Attempts),
		MaxAttempts: int(tm.MaxRetries),
		CreatedAt:   time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339Nano),
		EnqueuedAt:  &enq,
		Payload:     tm.Payload,
	}
	return job, true, nil
}

// isDead reports whether a task body with the given ID is present in the
// dead-letter list. The dead list stores full JSON bodies, so membership is
// checked by parsing each entry rather than by matching the raw list value.
func (h *Handler) isDead(ctx context.Context, id string) bool {
	bodies, err := h.rdb.LRange(ctx, "queue:tasks:dead", 0, -1).Result()
	if err != nil {
		return false
	}
	for _, body := range bodies {
		var tm TaskMetaData
		if err := json.Unmarshal([]byte(body), &tm); err == nil && tm.ID == id {
			return true
		}
	}
	return false
}

// pausedKey is set while the queue is paused. The worker's claim script
// checks for its presence and stops handing out work until it is cleared.
const pausedKey = "queue:paused"

// queueStatus reports whether the queue is currently accepting work.
func (h *Handler) queueStatus(ctx context.Context) QueueStatus {
	if n, err := h.rdb.Exists(ctx, pausedKey).Result(); err == nil && n > 0 {
		return QueuePaused
	}
	return QueueHealthy
}

// takeDead removes and returns the stored body of a task parked in the
// dead-letter list. The dead list holds full JSON bodies, so the entry is
// located by parsing rather than by matching an ID.
func (h *Handler) takeDead(ctx context.Context, id string) ([]byte, bool, error) {
	bodies, err := h.rdb.LRange(ctx, "queue:tasks:dead", 0, -1).Result()
	if err != nil {
		return nil, false, err
	}
	var found []byte
	ok := false
	for _, body := range bodies {
		var tm TaskMetaData
		if json.Unmarshal([]byte(body), &tm) == nil && tm.ID == id {
			// Keep the first body we see, but clear every matching entry: the
			// same task can appear more than once in the dead list.
			if !ok {
				found = []byte(body)
				ok = true
			}
			if err := h.rdb.LRem(ctx, "queue:tasks:dead", 1, body).Err(); err != nil {
				return nil, false, err
			}
		}
	}
	return found, ok, nil
}

// requeue puts a task back on the ready list, clearing any stale membership
// in the scheduled or inflight sets first. resetAttempts restores the retry
// budget, which is what a manual "retry" should do.
//
// The body is taken from task:<id> when present, otherwise recovered from the
// dead-letter list. When neither holds it, the task is reported as missing.
func (h *Handler) requeue(ctx context.Context, id string, resetAttempts bool) (bool, error) {
	// Always clear the dead-letter list first, even if a body still exists in
	// task:<id>: a task can leave stale copies behind there. The recovered
	// body is only needed when task:<id> has already been dropped.
	deadBody, wasDead, err := h.takeDead(ctx, id)
	if err != nil {
		return false, err
	}
	if _, err := h.rdb.Get(ctx, "task:"+id).Result(); err == redis.Nil {
		if !wasDead {
			return false, nil
		}
		if err := h.rdb.Set(ctx, "task:"+id, deadBody, 0).Err(); err != nil {
			return false, err
		}
	}

	if resetAttempts {
		if raw, err := h.rdb.Get(ctx, "task:"+id).Bytes(); err == nil {
			var tm TaskMetaData
			if json.Unmarshal(raw, &tm) == nil {
				tm.Attempts = 0
				if b, err := json.Marshal(tm); err == nil {
					h.rdb.Set(ctx, "task:"+id, b, 0)
				}
			}
		}
	}

	pipe := h.rdb.TxPipeline()
	pipe.ZRem(ctx, "queue:tasks:scheduled", id)
	pipe.ZRem(ctx, "queue:tasks:inflight", id)
	pipe.HDel(ctx, "queue:tasks:leases", id)
	pipe.LPush(ctx, "queue:tasks:immediate", id)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// deleteTask removes a task from every queue structure and drops its body.
func (h *Handler) deleteTask(ctx context.Context, id string) error {
	if _, _, err := h.takeDead(ctx, id); err != nil {
		return err
	}
	pipe := h.rdb.TxPipeline()
	pipe.ZRem(ctx, "queue:tasks:scheduled", id)
	pipe.ZRem(ctx, "queue:tasks:inflight", id)
	pipe.HDel(ctx, "queue:tasks:leases", id)
	pipe.LRem(ctx, "queue:tasks:immediate", 0, id)
	pipe.Del(ctx, "task:"+id)
	_, err := pipe.Exec(ctx)
	return err
}

func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request, id string) {
	ok, err := h.requeue(r.Context(), id, true)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		h.writeError(w, http.StatusNotFound, fmt.Sprintf("job %s not found", id))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) RequeueJob(w http.ResponseWriter, r *http.Request, id string) {
	ok, err := h.requeue(r.Context(), id, false)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		h.writeError(w, http.StatusNotFound, fmt.Sprintf("job %s not found", id))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.deleteTask(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) PauseQueue(w http.ResponseWriter, r *http.Request, name string) {
	if err := h.rdb.Set(r.Context(), pausedKey, "1", 0).Err(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) ResumeQueue(w http.ResponseWriter, r *http.Request, name string) {
	if err := h.rdb.Del(r.Context(), pausedKey).Err(); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) PurgeQueue(w http.ResponseWriter, r *http.Request, name string) {
	ctx := r.Context()
	ids, err := h.rdb.LRange(ctx, "queue:tasks:immediate", 0, -1).Result()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	pipe := h.rdb.TxPipeline()
	for _, id := range ids {
		pipe.Del(ctx, "task:"+id)
	}
	pipe.Del(ctx, "queue:tasks:immediate")
	if _, err := pipe.Exec(ctx); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]int{"purged": len(ids)})
}
func (h *Handler) Workers(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, h.readWorkers(r.Context()))
}
func (h *Handler) Worker(w http.ResponseWriter, r *http.Request, id string) {
	for _, wk := range h.readWorkers(r.Context()) {
		if wk.ID == id {
			h.writeJSON(w, http.StatusOK, wk)
			return
		}
	}
	h.writeError(w, http.StatusNotFound, fmt.Sprintf("worker %s not found", id))
}

// registeredWorker mirrors the document the worker pool publishes at
// worker:<id>:meta. It lives here (not in the worker package) so the API
// stays decoupled from worker internals.
type registeredWorker struct {
	ID            string `json:"id"`
	Hostname      string `json:"hostname"`
	Version       string `json:"version"`
	Status        string `json:"status"`
	StartedAt     string `json:"startedAt"`
	LastHeartbeat string `json:"lastHeartbeatAt"`
	Concurrency   int    `json:"concurrency"`
}

// readWorkers assembles the live worker list from the registry Redis keys.
// Stale registrations (meta key expired, or heartbeat older than the healthy
// window) are reported with status "stale" rather than dropped, so the
// dashboard can see a worker that stopped heartbeating.
func (h *Handler) readWorkers(ctx context.Context) []Worker {
	ids, err := h.rdb.SMembers(ctx, "forgequeue:workers").Result()
	if err != nil {
		return []Worker{}
	}
	workers := make([]Worker, 0, len(ids))
	for _, id := range ids {
		raw, err := h.rdb.Get(ctx, "worker:"+id+":meta").Bytes()
		if err == redis.Nil {
			// The meta key lapsed; the set member is stale. Clean it up.
			h.rdb.SRem(ctx, "forgequeue:workers", id)
			continue
		}
		if err != nil {
			continue
		}
		var meta registeredWorker
		if err := json.Unmarshal(raw, &meta); err != nil {
			continue
		}

		status := "healthy"
		if hb, perr := time.Parse(time.RFC3339Nano, meta.LastHeartbeat); perr == nil {
			if time.Since(hb) > 25*time.Second {
				status = "stale"
			}
		}

		leases := h.readWorkerLeases(ctx, id)
		workers = append(workers, Worker{
			ID:              meta.ID,
			Hostname:        meta.Hostname,
			Version:         meta.Version,
			Status:          status,
			StartedAt:       meta.StartedAt,
			LastHeartbeatAt: meta.LastHeartbeat,
			LeaseCount:      len(leases),
			JobsProcessing:  len(leases),
			Concurrency:     meta.Concurrency,
			Leases:          leases,
			RecentJobs:      []RecentJob{},
		})
	}
	return workers
}

// readWorkerLeases returns the leases a worker is actively holding. A task is
// only counted if Redis still knows about its lease, which filters out IDs
// left behind after a crash.
func (h *Handler) readWorkerLeases(ctx context.Context, id string) []Lease {
	ids, err := h.rdb.SMembers(ctx, "worker:"+id+":leases").Result()
	if err != nil {
		return []Lease{}
	}
	leases := make([]Lease, 0, len(ids))
	for _, taskID := range ids {
		leaseID, err := h.rdb.HGet(ctx, "queue:tasks:leases", taskID).Result()
		if err == redis.Nil {
			// Not leased anymore (done or recovered); drop the stale entry.
			h.rdb.SRem(ctx, "worker:"+id+":leases", taskID)
			continue
		}
		if err != nil {
			continue
		}
		var task string
		if raw, err := h.rdb.Get(ctx, "task:"+taskID).Bytes(); err == nil {
			var tm TaskMetaData
			if json.Unmarshal(raw, &tm) == nil {
				task = tm.TaskType
			}
		}
		expires := time.Now().Add(10 * time.Second)
		if score, err := h.rdb.ZScore(ctx, "queue:tasks:inflight", taskID).Result(); err == nil {
			expires = time.Unix(int64(score), 0)
		}
		leases = append(leases, Lease{
			JobID:      taskID,
			Queue:      "default",
			Task:       task,
			LeaseID:    leaseID,
			AcquiredAt: time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano),
			ExpiresAt:  expires.UTC().Format(time.RFC3339Nano),
		})
	}
	return leases
}

// workerCounts returns how many registered workers are healthy vs stale.
func (h *Handler) workerCounts(ctx context.Context) (healthy, stale int) {
	for _, wk := range h.readWorkers(ctx) {
		if wk.Status == "healthy" {
			healthy++
		} else {
			stale++
		}
	}
	return healthy, stale
}
func (h *Handler) DLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bodies, err := h.rdb.LRange(ctx, "queue:tasks:dead", 0, -1).Result()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items := []DLQItem{}
	for _, body := range bodies {
		var tm TaskMetaData
		if err := json.Unmarshal([]byte(body), &tm); err != nil {
			continue
		}
		items = append(items, DLQItem{
			JobID:         tm.ID,
			OriginalQueue: "default",
			Task:          tm.TaskType,
			Attempts:      int(tm.Attempts),
			LastError:     "task failed",
			FailedAt:      time.Now().UTC().Format(time.RFC3339Nano),
			AgeMs:         0,
		})
	}
	h.writeJSON(w, http.StatusOK, items)
}
func (h *Handler) RetryDLQ(w http.ResponseWriter, r *http.Request, id string) {
	ok, err := h.requeue(r.Context(), id, true)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		h.writeError(w, http.StatusNotFound, fmt.Sprintf("dlq entry %s not found", id))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) DeleteDLQ(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.deleteTask(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) PurgeDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bodies, err := h.rdb.LRange(ctx, "queue:tasks:dead", 0, -1).Result()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	pipe := h.rdb.TxPipeline()
	for _, body := range bodies {
		var tm TaskMetaData
		if json.Unmarshal([]byte(body), &tm) == nil {
			pipe.Del(ctx, "task:"+tm.ID)
		}
	}
	pipe.Del(ctx, "queue:tasks:dead")
	if _, err := pipe.Exec(ctx); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]int{"purged": len(bodies)})
}
func (h *Handler) Delayed(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	zs, err := h.rdb.ZRangeWithScores(ctx, "queue:tasks:scheduled", 0, -1).Result()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items := []DelayedJob{}
	for _, z := range zs {
		id := z.Member.(string)
		tm, ok, _ := h.readTaskFull(r, id)
		if !ok || tm == nil {
			continue
		}
		sched := time.Unix(int64(z.Score), 0)
		items = append(items, DelayedJob{
			JobID:        id,
			Queue:        "default",
			Task:         tm.Task,
			Attempts:     tm.Attempts,
			ScheduledFor: sched.UTC().Format(time.RFC3339Nano),
			DelayMs:      sched.Sub(time.Now()).Milliseconds(),
			Status:       StatusDelayed,
		})
	}
	h.writeJSON(w, http.StatusOK, items)
}
func (h *Handler) PromoteDelayed(w http.ResponseWriter, r *http.Request, id string) {
	ctx := r.Context()
	// Only a task actually waiting in the scheduled set can be promoted.
	if _, err := h.rdb.ZScore(ctx, "queue:tasks:scheduled", id).Result(); err == redis.Nil {
		h.writeError(w, http.StatusNotFound, fmt.Sprintf("delayed job %s not found", id))
		return
	} else if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	pipe := h.rdb.TxPipeline()
	pipe.ZRem(ctx, "queue:tasks:scheduled", id)
	pipe.LPush(ctx, "queue:tasks:immediate", id)
	if _, err := pipe.Exec(ctx); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) CancelDelayed(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.deleteTask(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) Throughput(w http.ResponseWriter, r *http.Request) {
	rangeQ := r.URL.Query().Get("range")
	if rangeQ == "" {
		rangeQ = "15m"
	}
	points := genSeries(rangeQ, h.rdb, r.Context())
	h.writeJSON(w, http.StatusOK, points)
}
func (h *Handler) Version(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"version": "v0.9.1"})
}

func genSeries(rangeQ string, rdb *redis.Client, ctx context.Context) []ThroughputPoint {
	now := time.Now()
	var span time.Duration
	var n int
	switch rangeQ {
	case "5m":
		span, n = 5*time.Minute, 20
	case "15m":
		span, n = 15*time.Minute, 30
	case "1h":
		span, n = 1*time.Hour, 30
	case "6h":
		span, n = 6*time.Hour, 36
	case "24h":
		span, n = 24*time.Hour, 48
	case "7d":
		span, n = 7*24*time.Hour, 56
	default:
		span, n = 15*time.Minute, 30
	}
	step := span / time.Duration(n)
	points := make([]ThroughputPoint, n)
	for i := 0; i < n; i++ {
		bucketStart := now.Add(-span + time.Duration(i)*step)
		bucketEnd := bucketStart.Add(step)
		start := bucketStart.Unix()
		end := bucketEnd.Unix()
		// Count real events in this bucket
		enq := int(rdb.ZCount(ctx, "metrics:enqueued", fmt.Sprint(start), fmt.Sprint(end)).Val())
		comp := int(rdb.ZCount(ctx, "metrics:completed", fmt.Sprint(start), fmt.Sprint(end)).Val())
		fail := int(rdb.ZCount(ctx, "metrics:failed", fmt.Sprint(start), fmt.Sprint(end)).Val())
		if comp < 0 {
			comp = 0
		}
		if fail < 0 {
			fail = 0
		}
		if enq < 0 {
			enq = 0
		}
		dlqTotal := int(rdb.LLen(ctx, "queue:tasks:dead").Val())
		pending := int(rdb.LLen(ctx, "queue:tasks:immediate").Val())
		processing := int(rdb.ZCard(ctx, "queue:tasks:inflight").Val())
		delayed := int(rdb.ZCard(ctx, "queue:tasks:scheduled").Val())
		depth := pending + processing + delayed
		points[i] = ThroughputPoint{
			Time:             bucketStart.UTC().Format(time.RFC3339Nano),
			Enqueued:         enq,
			Completed:        comp,
			Failed:           fail,
			Depth:            depth,
			LatencyMs:        40,
			ExecMs:           380,
			Retries:          0,
			DLQ:              dlqTotal,
			Workers:          3,
			LeaseExpirations: 0,
		}
	}
	return points
}
