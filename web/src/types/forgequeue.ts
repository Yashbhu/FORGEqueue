export type JobStatus =
  "pending" | "processing" | "completed" | "failed" | "retrying" | "delayed" | "dlq" | "cancelled";
export type QueueStatus = "healthy" | "paused" | "degraded" | "unknown";
export type WorkerStatus = "healthy" | "idle" | "busy" | "stale" | "disconnected";

export interface Attempt {
  attempt: number;
  workerId: string | null;
  startedAt: string;
  finishedAt: string | null;
  durationMs: number | null;
  result?: unknown;
  error?: string | null;
}

export interface Job {
  id: string;
  queue: string;
  task: string;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  createdAt: string;
  enqueuedAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  failedAt: string | null;
  durationMs: number | null;
  workerId: string | null;
  leaseId: string | null;
  leaseExpiresAt: string | null;
  nextRetryAt: string | null;
  retryPolicy?: {
    backoff: "exponential" | "fixed";
    baseMs: number;
    maxMs: number;
    jitter: boolean;
  };
  payload: unknown;
  result?: unknown;
  error?: { message: string; stack?: string; code?: string };
  lastErrorAt?: string | null;
  lastError?: string | null;
  attemptsHistory?: Attempt[];
}

export interface QueueSummary {
  name: string;
  pending: number;
  processing: number;
  delayed: number;
  failed: number;
  completed: number;
  workers: number;
  ratePerMin: number;
  oldestJobAt: string | null;
  status: QueueStatus;
}

export interface Lease {
  jobId: string;
  queue: string;
  task: string;
  leaseId: string;
  acquiredAt: string;
  expiresAt: string;
}

export interface RecentJob {
  jobId: string;
  queue: string;
  status: JobStatus;
  durationMs: number | null;
  finishedAt: string | null;
}

export interface Worker {
  id: string;
  hostname: string;
  version: string;
  status: WorkerStatus;
  startedAt: string;
  lastHeartbeatAt: string;
  leaseCount: number;
  jobsProcessing: number;
  concurrency: number;
  leases: Lease[];
  recentJobs: RecentJob[];
}

export interface DLQItem {
  jobId: string;
  originalQueue: string;
  task: string;
  attempts: number;
  lastError: string;
  failedAt: string;
  ageMs: number;
  job: Job;
}

export interface DelayedJob {
  jobId: string;
  queue: string;
  task: string;
  attempts: number;
  scheduledFor: string;
  delayMs: number;
  status: JobStatus;
}

export interface ThroughputPoint {
  time: string;
  enqueued: number;
  completed: number;
  failed: number;
}

export interface MetricsPoint extends ThroughputPoint {
  depth: number;
  latencyMs: number;
  execMs: number;
  retries: number;
  dlq: number;
  workers: number;
  leaseExpirations: number;
}

export interface SystemStats {
  queues: number;
  activeJobs: number;
  pending: number;
  processing: number;
  failed: number;
  delayed: number;
  dlq: number;
  activeWorkers: number;
  healthyWorkers: number;
  staleWorkers: number;
  workersWithExpiredLeases: number;
}
