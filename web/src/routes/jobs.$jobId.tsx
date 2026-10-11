import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { RotateCcw, Trash2, Undo2 } from "lucide-react";
import { api } from "@/api/endpoints";
import { useAction, useJob } from "@/api/queries";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { JsonViewer } from "@/components/json-viewer";
import { DataTable } from "@/components/data-table";
import {
  CopyButton,
  Duration,
  EmptyState,
  ErrorBlock,
  KV,
  Mono,
  PageHeader,
  Section,
  StatusBadge,
  TimeAgo,
  useNow,
} from "@/components/shared";
import { Button } from "@/components/ui/button";
import { fmtDuration } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { Job } from "@/types/forgequeue";

export const Route = createFileRoute("/jobs/$jobId")({
  ssr: false,
  head: ({ params }) => ({
    meta: [
      { title: `Job ${params.jobId.slice(0, 8)} — ForgeQueue Console` },
      {
        name: "description",
        content: "Full job inspection: timeline, attempts, payload, errors, retries and lease.",
      },
      { property: "og:title", content: `Job ${params.jobId.slice(0, 8)} — ForgeQueue Console` },
      {
        property: "og:description",
        content: "Full job inspection: timeline, attempts, payload, errors, retries and lease.",
      },
    ],
  }),
  component: JobDetail,
});

function Timeline({ job }: { job: Job }) {
  type Step = {
    label: string;
    at: string | null | undefined;
    tone?: string;
    note?: string | undefined;
  };
  const steps: Step[] = [
    { label: "Created", at: job.createdAt },
    { label: "Enqueued", at: job.enqueuedAt },
    { label: "Claimed (lease acquired)", at: job.startedAt, note: job.leaseId ?? undefined },
    ...(job.status === "processing"
      ? ([
          {
            label: "Processing · heartbeat lease",
            at: job.leaseExpiresAt,
            tone: "bg-info",
            note: "lease expires",
          },
        ] as Step[])
      : []),
    ...(job.completedAt
      ? ([{ label: "Completed", at: job.completedAt, tone: "bg-success" }] as Step[])
      : []),
    ...(job.failedAt
      ? ([
          {
            label: job.status === "dlq" ? "Moved to DLQ" : "Failed",
            at: job.failedAt,
            tone: "bg-destructive",
          },
        ] as Step[])
      : []),
    ...(job.nextRetryAt
      ? ([
          {
            label: job.status === "delayed" ? "Scheduled run" : "Next retry",
            at: job.nextRetryAt,
            tone: "bg-warning",
          },
        ] as Step[])
      : []),
  ];
  return (
    <ol className="relative ml-1.5 border-l pl-4">
      {steps.map((s, i) => (
        <li key={i} className="relative pb-2.5 last:pb-0">
          <span
            className={cn(
              "absolute -left-[21px] top-1 size-2 rounded-full border-2 border-card",
              s.at ? (s.tone ?? "bg-muted-foreground") : "bg-border",
            )}
          />
          <div className="flex items-baseline gap-2 text-xs">
            <span className={cn(!s.at && "text-muted-foreground")}>{s.label}</span>
            <TimeAgo iso={s.at} future={!!s.at && Date.parse(s.at) > Date.now()} />
            {s.note && <Mono className="text-muted-foreground">{s.note}</Mono>}
          </div>
        </li>
      ))}
    </ol>
  );
}

function JobDetail() {
  const { jobId } = Route.useParams();
  const { data: job, error, isLoading } = useJob(jobId);
  const navigate = useNavigate();
  const now = useNow(1000);
  const retry = useAction(api.retryJob, () => "Job retried");
  const requeue = useAction(api.requeueJob, () => "Job re-enqueued");
  const del = useAction(api.deleteJob, () => "Job deleted");

  if (error) return <EmptyState title="Job not found" description={error.message} />;
  if (isLoading || !job) return <div className="h-40 animate-pulse rounded-md bg-muted/40" />;

  const leaseRemaining = job.leaseExpiresAt ? Date.parse(job.leaseExpiresAt) - now : null;
  return (
    <>
      <div className="mb-1 text-xs text-muted-foreground">
        <Link to="/queues" className="hover:text-foreground">
          Queues
        </Link>{" "}
        /{" "}
        <Link
          to="/queues/$queueName"
          params={{ queueName: job.queue }}
          className="font-mono hover:text-foreground"
        >
          {job.queue}
        </Link>{" "}
        / job
      </div>
      <PageHeader
        title={
          <span className="flex items-center gap-1.5">
            <Mono className="text-sm">{job.id}</Mono>
            <CopyButton value={job.id} label="Copy job ID" />
            <StatusBadge status={job.status} />
          </span>
        }
        meta={
          <>
            <span className="text-muted-foreground">queue</span> <Mono>{job.queue}</Mono>
            <span className="text-muted-foreground">task</span> <Mono>{job.task}</Mono>
          </>
        }
        actions={
          <>
            <ConfirmDialog
              trigger={
                <Button size="sm" variant="outline" className="h-7 text-xs">
                  <RotateCcw className="size-3" /> Retry
                </Button>
              }
              title="Retry job?"
              description="Job is moved back to pending, keeping its attempt count."
              confirmLabel="Retry"
              onConfirm={() => retry.mutate(job.id)}
            />
            <ConfirmDialog
              trigger={
                <Button size="sm" variant="outline" className="h-7 text-xs">
                  <Undo2 className="size-3" /> Re-enqueue
                </Button>
              }
              title="Re-enqueue job?"
              description="A fresh enqueue with attempts reset to 0. Ensure the handler is idempotent."
              confirmLabel="Re-enqueue"
              onConfirm={() => requeue.mutate(job.id)}
            />
            <ConfirmDialog
              trigger={
                <Button size="sm" variant="outline" className="h-7 text-xs text-destructive">
                  <Trash2 className="size-3" /> Delete
                </Button>
              }
              destructive
              title="Delete job?"
              description={
                <>
                  Permanently removes <code className="font-mono">{job.id}</code> from Redis and
                  PostgreSQL.
                </>
              }
              confirmLabel="Delete job"
              onConfirm={() =>
                del.mutate(job.id, {
                  onSuccess: () =>
                    navigate({ to: "/queues/$queueName", params: { queueName: job.queue } }),
                })
              }
            />
          </>
        }
      />
      <div className="grid gap-3 lg:grid-cols-3">
        <div className="space-y-3 lg:col-span-2">
          <Section title="Job overview">
            <KV
              items={[
                ["created_at", <TimeAgo iso={job.createdAt} />],
                ["enqueued_at", <TimeAgo iso={job.enqueuedAt} />],
                ["started_at", <TimeAgo iso={job.startedAt} />],
                ["completed_at", <TimeAgo iso={job.completedAt} />],
                ["failed_at", <TimeAgo iso={job.failedAt} />],
                ["duration", <Duration ms={job.durationMs} />],
                ["attempts", `${job.attempts} / ${job.maxAttempts}`],
                [
                  "worker_id",
                  job.workerId && (
                    <Link
                      to="/workers/$workerId"
                      params={{ workerId: job.workerId }}
                      className="text-primary hover:underline"
                    >
                      {job.workerId}
                    </Link>
                  ),
                ],
                ["lease_id", job.leaseId],
                ["lease_expires_at", <TimeAgo iso={job.leaseExpiresAt} future />],
                ["next_retry_at", <TimeAgo iso={job.nextRetryAt} future />],
              ]}
            />
          </Section>
          {job.error && (
            <Section title="Error">
              <ErrorBlock {...job.error} />
            </Section>
          )}
          {job.result !== undefined && (
            <Section title="Result">
              <JsonViewer value={job.result} />
            </Section>
          )}
          <Section title="Payload">
            <JsonViewer value={job.payload} />
          </Section>
          <Section title={`Attempts (${job.attemptsHistory?.length ?? 0})`} flush>
            <DataTable
              rows={job.attemptsHistory ?? []}
              getId={(a) => String(a.attempt)}
              emptyTitle="Not attempted yet"
              columns={[
                { key: "n", header: "#", cell: (a) => <Mono>{a.attempt}</Mono> },
                { key: "w", header: "Worker", cell: (a) => <Mono>{a.workerId ?? "—"}</Mono> },
                { key: "s", header: "Started", cell: (a) => <TimeAgo iso={a.startedAt} /> },
                {
                  key: "f",
                  header: "Finished",
                  cell: (a) =>
                    a.finishedAt ? (
                      <TimeAgo iso={a.finishedAt} />
                    ) : (
                      <span className="text-info">running</span>
                    ),
                },
                {
                  key: "d",
                  header: "Duration",
                  align: "right",
                  cell: (a) => fmtDuration(a.durationMs),
                },
                {
                  key: "r",
                  header: "Outcome",
                  className: "max-w-[360px] truncate",
                  cell: (a) =>
                    a.error ? (
                      <Mono className="text-destructive">{a.error}</Mono>
                    ) : a.finishedAt ? (
                      <Mono className="text-success">ok</Mono>
                    ) : (
                      "—"
                    ),
                },
              ]}
            />
          </Section>
        </div>
        <div className="space-y-3">
          <Section title="Execution timeline">
            <Timeline job={job} />
          </Section>
          <Section title="Retry information">
            <KV
              items={[
                ["retries", Math.max(0, job.attempts - 1)],
                ["remaining", Math.max(0, job.maxAttempts - job.attempts)],
                ["backoff", job.retryPolicy?.backoff],
                [
                  "base / max",
                  job.retryPolicy &&
                    `${fmtDuration(job.retryPolicy.baseMs)} / ${fmtDuration(job.retryPolicy.maxMs)}`,
                ],
                ["jitter", job.retryPolicy ? String(job.retryPolicy.jitter) : null],
                ["next_retry", <TimeAgo iso={job.nextRetryAt} future />],
              ]}
            />
          </Section>
          <Section title="Lease information">
            {job.leaseId ? (
              <KV
                items={[
                  ["worker", job.workerId],
                  [
                    "lease_id",
                    <span className="flex items-center gap-0.5">
                      {job.leaseId}
                      <CopyButton value={job.leaseId} />
                    </span>,
                  ],
                  ["acquired", <TimeAgo iso={job.startedAt} />],
                  ["expires", <TimeAgo iso={job.leaseExpiresAt} future />],
                  [
                    "remaining",
                    <span
                      className={
                        leaseRemaining !== null && leaseRemaining < 5000 ? "text-destructive" : ""
                      }
                    >
                      {leaseRemaining !== null && leaseRemaining <= 0
                        ? "expired"
                        : fmtDuration(leaseRemaining)}
                    </span>,
                  ],
                ]}
              />
            ) : (
              <p className="text-xs text-muted-foreground">
                No active lease. Job is not currently owned by a worker.
              </p>
            )}
          </Section>
        </div>
      </div>
    </>
  );
}
