package httpapi

import (
	"encoding/json"

	"forgequeue/internal/auth"
)

// LoginRequest is the body accepted by POST /api/auth/login.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse is returned on successful login. The token is a bearer JWT.
type LoginResponse struct {
	Token     string        `json:"token"`
	ExpiresAt string        `json:"expiresAt"`
	User      auth.Identity `json:"user"`
}

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusRetrying   JobStatus = "retrying"
	StatusDelayed    JobStatus = "delayed"
	StatusDLQ        JobStatus = "dlq"
	StatusCancelled  JobStatus = "cancelled"
	StatusUnknown    JobStatus = "unknown"
)

type QueueStatus string

const (
	QueueHealthy  QueueStatus = "healthy"
	QueuePaused   QueueStatus = "paused"
	QueueDegraded QueueStatus = "degraded"
	QueueUnknown  QueueStatus = "unknown"
)

type RetryPolicy struct {
	Backoff string `json:"backoff"`
	BaseMs  int    `json:"baseMs"`
	MaxMs   int    `json:"maxMs"`
	Jitter  bool   `json:"jitter"`
}

type JobError struct {
	Message string  `json:"message"`
	Stack   *string `json:"stack,omitempty"`
	Code    *string `json:"code,omitempty"`
}

type Attempt struct {
	Attempt    int     `json:"attempt"`
	WorkerID   *string `json:"workerId,omitempty"`
	StartedAt  string  `json:"startedAt"`
	FinishedAt *string `json:"finishedAt,omitempty"`
	DurationMs *int    `json:"durationMs,omitempty"`
	Result     any     `json:"result,omitempty"`
	Error      *string `json:"error,omitempty"`
}

type Job struct {
	ID              string          `json:"id"`
	Queue           string          `json:"queue"`
	Task            string          `json:"task"`
	Status          JobStatus       `json:"status"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"maxAttempts"`
	CreatedAt       string          `json:"createdAt"`
	EnqueuedAt      *string         `json:"enqueuedAt,omitempty"`
	StartedAt       *string         `json:"startedAt,omitempty"`
	CompletedAt     *string         `json:"completedAt,omitempty"`
	FailedAt        *string         `json:"failedAt,omitempty"`
	DurationMs      *int            `json:"durationMs,omitempty"`
	WorkerID        *string         `json:"workerId,omitempty"`
	LeaseID         *string         `json:"leaseId,omitempty"`
	LeaseExpiresAt  *string         `json:"leaseExpiresAt,omitempty"`
	NextRetryAt     *string         `json:"nextRetryAt,omitempty"`
	RetryPolicy     *RetryPolicy    `json:"retryPolicy,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	Result          any             `json:"result,omitempty"`
	Error           *JobError       `json:"error,omitempty"`
	LastErrorAt     *string         `json:"lastErrorAt,omitempty"`
	LastError       *string         `json:"lastError,omitempty"`
	AttemptsHistory []Attempt       `json:"attemptsHistory,omitempty"`
}

type QueueSummary struct {
	Name        string      `json:"name"`
	Pending     int         `json:"pending"`
	Processing  int         `json:"processing"`
	Delayed     int         `json:"delayed"`
	Failed      int         `json:"failed"`
	Completed   int         `json:"completed"`
	Workers     int         `json:"workers"`
	RatePerMin  float64     `json:"ratePerMin"`
	OldestJobAt *string     `json:"oldestJobAt,omitempty"`
	Status      QueueStatus `json:"status"`
}

type Lease struct {
	JobID      string `json:"jobId"`
	Queue      string `json:"queue"`
	Task       string `json:"task"`
	LeaseID    string `json:"leaseId"`
	AcquiredAt string `json:"acquiredAt"`
	ExpiresAt  string `json:"expiresAt"`
}

type RecentJob struct {
	JobID      string    `json:"jobId"`
	Queue      string    `json:"queue"`
	Status     JobStatus `json:"status"`
	DurationMs *int      `json:"durationMs,omitempty"`
	FinishedAt *string   `json:"finishedAt,omitempty"`
}

type Worker struct {
	ID              string      `json:"id"`
	Hostname        string      `json:"hostname"`
	Version         string      `json:"version"`
	Status          string      `json:"status"`
	StartedAt       string      `json:"startedAt"`
	LastHeartbeatAt string      `json:"lastHeartbeatAt"`
	LeaseCount      int         `json:"leaseCount"`
	JobsProcessing  int         `json:"jobsProcessing"`
	Concurrency     int         `json:"concurrency"`
	Leases          []Lease     `json:"leases"`
	RecentJobs      []RecentJob `json:"recentJobs"`
}

type DLQItem struct {
	JobID         string `json:"jobId"`
	OriginalQueue string `json:"originalQueue"`
	Task          string `json:"task"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"lastError"`
	FailedAt      string `json:"failedAt"`
	AgeMs         int64  `json:"ageMs"`
	Job           *Job   `json:"job,omitempty"`
}

type DelayedJob struct {
	JobID        string    `json:"jobId"`
	Queue        string    `json:"queue"`
	Task         string    `json:"task"`
	Attempts     int       `json:"attempts"`
	ScheduledFor string    `json:"scheduledFor"`
	DelayMs      int64     `json:"delayMs"`
	Status       JobStatus `json:"status"`
}

type ThroughputPoint struct {
	Time             string `json:"time"`
	Enqueued         int    `json:"enqueued"`
	Completed        int    `json:"completed"`
	Failed           int    `json:"failed"`
	Depth            int    `json:"depth"`
	LatencyMs        int    `json:"latencyMs"`
	ExecMs           int    `json:"execMs"`
	Retries          int    `json:"retries"`
	DLQ              int    `json:"dlq"`
	Workers          int    `json:"workers"`
	LeaseExpirations int    `json:"leaseExpirations"`
}

type SystemStats struct {
	Queues                   int `json:"queues"`
	ActiveJobs               int `json:"activeJobs"`
	Pending                  int `json:"pending"`
	Processing               int `json:"processing"`
	Failed                   int `json:"failed"`
	Completed                int `json:"completed"`
	Delayed                  int `json:"delayed"`
	DLQ                      int `json:"dlq"`
	ActiveWorkers            int `json:"activeWorkers"`
	HealthyWorkers           int `json:"healthyWorkers"`
	StaleWorkers             int `json:"staleWorkers"`
	WorkersWithExpiredLeases int `json:"workersWithExpiredLeases"`
}

type TaskMetaData struct {
	ID         string          `json:"id"`
	TaskType   string          `json:"task_type"`
	Payload    json.RawMessage `json:"payload"`
	MaxRetries int32           `json:"max_retries"`
	Attempts   int32           `json:"attempts"`
}
