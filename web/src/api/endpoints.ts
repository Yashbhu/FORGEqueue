import type {
  DelayedJob,
  DLQItem,
  Job,
  MetricsPoint,
  QueueSummary,
  SystemStats,
  Worker,
} from "@/types/forgequeue";
import type { Session, SessionUser } from "@/lib/auth";
import { request } from "./client";

export type MetricsRange = "5m" | "15m" | "1h" | "6h" | "24h" | "7d";

export interface Api {
  login(username: string, password: string): Promise<Session>;
  me(): Promise<SessionUser>;
  stats(): Promise<SystemStats>;
  queues(): Promise<QueueSummary[]>;
  queue(name: string): Promise<QueueSummary>;
  queueJobs(name: string): Promise<Job[]>;
  job(id: string): Promise<Job>;
  retryJob(id: string): Promise<void>;
  requeueJob(id: string): Promise<void>;
  deleteJob(id: string): Promise<void>;
  pauseQueue(name: string): Promise<void>;
  resumeQueue(name: string): Promise<void>;
  purgeQueue(name: string): Promise<{ purged: number }>;
  workers(): Promise<Worker[]>;
  worker(id: string): Promise<Worker>;
  dlq(): Promise<DLQItem[]>;
  retryDlq(id: string): Promise<void>;
  deleteDlq(id: string): Promise<void>;
  purgeDlq(): Promise<{ purged: number }>;
  delayed(): Promise<DelayedJob[]>;
  promoteDelayed(id: string): Promise<void>;
  cancelDelayed(id: string): Promise<void>;
  throughput(range: MetricsRange, queue?: string): Promise<MetricsPoint[]>;
  version(): Promise<{ version: string }>;
}

const e = encodeURIComponent;
const httpApi: Api = {
  login: (username, password) => request("POST", "/auth/login", { username, password }),
  me: () => request("GET", "/auth/me"),
  stats: () => request("GET", "/system/stats"),
  queues: () => request("GET", "/queues"),
  queue: (n) => request("GET", `/queues/${e(n)}`),
  queueJobs: (n) => request("GET", `/queues/${e(n)}/jobs`),
  job: (id) => request("GET", `/jobs/${e(id)}`),
  retryJob: (id) => request("POST", `/jobs/${e(id)}/retry`),
  requeueJob: (id) => request("POST", `/jobs/${e(id)}/requeue`),
  deleteJob: (id) => request("DELETE", `/jobs/${e(id)}`),
  pauseQueue: (n) => request("POST", `/queues/${e(n)}/pause`),
  resumeQueue: (n) => request("POST", `/queues/${e(n)}/resume`),
  purgeQueue: (n) => request("POST", `/queues/${e(n)}/purge`),
  workers: () => request("GET", "/workers"),
  worker: (id) => request("GET", `/workers/${e(id)}`),
  dlq: () => request("GET", "/dlq"),
  retryDlq: (id) => request("POST", `/dlq/${e(id)}/retry`),
  deleteDlq: (id) => request("DELETE", `/dlq/${e(id)}`),
  purgeDlq: () => request("POST", "/dlq/purge"),
  delayed: () => request("GET", "/delayed"),
  promoteDelayed: (id) => request("POST", `/delayed/${e(id)}/promote`),
  cancelDelayed: (id) => request("DELETE", `/delayed/${e(id)}`),
  throughput: (r, q) =>
    request("GET", `/metrics/throughput?range=${r}${q ? `&queue=${e(q)}` : ""}`),
  version: () => request("GET", "/system/version"),
};

export const api: Api = httpApi;
