import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { RotateCcw, Trash2, Zap, X } from "lucide-react";
import { api } from "@/api/endpoints";
import { useAction, useBulk } from "@/api/queries";
import { DataTable } from "./data-table";
import { ConfirmDialog } from "./confirm-dialog";
import { Duration, JobId, Mono, StatusBadge, TimeAgo, useNow } from "./shared";
import { Button } from "@/components/ui/button";
import { fmtDuration } from "@/lib/format";
import type { DelayedJob, Job } from "@/types/forgequeue";

export function BulkBar({
  count,
  children,
  onClear,
}: {
  count: number;
  children: React.ReactNode;
  onClear: () => void;
}) {
  if (!count) return null;
  return (
    <div className="flex items-center gap-1.5 border-b bg-accent/40 px-2 py-1 text-xs">
      <span className="font-mono">{count} selected</span>
      {children}
      <Button variant="ghost" size="sm" className="ml-auto h-6 text-xs" onClick={onClear}>
        Clear
      </Button>
    </div>
  );
}

export function JobsTable({ jobs, showQueue }: { jobs: Job[] | undefined; showQueue?: boolean }) {
  const [sel, setSel] = useState<Set<string>>(new Set());
  const retry = useBulk(api.retryJob, "Retried");
  const del = useBulk(api.deleteJob, "Deleted");
  useNow(5000);
  const ids = [...sel];
  return (
    <>
      <BulkBar count={sel.size} onClear={() => setSel(new Set())}>
        <Button
          variant="outline"
          size="sm"
          className="h-6 text-xs"
          onClick={() => (retry.mutate(ids), setSel(new Set()))}
        >
          <RotateCcw className="size-3" /> Retry
        </Button>
        <ConfirmDialog
          trigger={
            <Button variant="outline" size="sm" className="h-6 text-xs text-destructive">
              <Trash2 className="size-3" /> Delete
            </Button>
          }
          destructive
          title={`Delete ${sel.size} jobs?`}
          description={`This permanently removes ${sel.size} jobs from Redis and PostgreSQL. This cannot be undone.`}
          confirmLabel={`Delete ${sel.size} jobs`}
          onConfirm={() => (del.mutate(ids), setSel(new Set()))}
        />
      </BulkBar>
      <DataTable
        rows={jobs}
        getId={(j) => j.id}
        selected={sel}
        onSelectedChange={setSel}
        defaultSort={{ key: "created", dir: "desc" }}
        emptyTitle="No jobs"
        columns={[
          { key: "id", header: "Job ID", cell: (j) => <JobId id={j.id} /> },
          ...(showQueue
            ? [
                {
                  key: "queue",
                  header: "Queue",
                  sort: (j: Job) => j.queue,
                  cell: (j: Job) => <Mono>{j.queue}</Mono>,
                },
              ]
            : []),
          { key: "task", header: "Task", sort: (j) => j.task, cell: (j) => <Mono>{j.task}</Mono> },
          {
            key: "status",
            header: "Status",
            sort: (j) => j.status,
            cell: (j) => <StatusBadge status={j.status} />,
          },
          {
            key: "att",
            header: "Attempts",
            align: "right",
            sort: (j) => j.attempts,
            cell: (j) => `${j.attempts}/${j.maxAttempts}`,
          },
          {
            key: "created",
            header: "Created",
            sort: (j) => j.createdAt,
            cell: (j) => <TimeAgo iso={j.createdAt} />,
          },
          {
            key: "enq",
            header: "Enqueued",
            sort: (j) => j.enqueuedAt ?? "",
            cell: (j) => <TimeAgo iso={j.enqueuedAt} />,
          },
          {
            key: "age",
            header: "Age",
            align: "right",
            sort: (j) => -Date.parse(j.createdAt),
            cell: (j) => fmtDuration(Date.now() - Date.parse(j.createdAt)),
          },
          {
            key: "worker",
            header: "Worker",
            cell: (j) =>
              j.workerId ? (
                <Link
                  to="/workers/$workerId"
                  params={{ workerId: j.workerId }}
                  className="font-mono text-xs text-primary hover:underline"
                  onClick={(e) => e.stopPropagation()}
                >
                  {j.workerId}
                </Link>
              ) : (
                <span className="text-muted-foreground">—</span>
              ),
          },
          {
            key: "a",
            header: "",
            align: "right",
            cell: (j) =>
              ["failed", "retrying", "cancelled"].includes(j.status) ? (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 px-1.5 text-xs"
                  onClick={() => retry.mutate([j.id])}
                >
                  <RotateCcw className="size-3" /> Retry
                </Button>
              ) : null,
          },
        ]}
      />
    </>
  );
}

export function DelayedTable({
  rows,
  showQueue = true,
}: {
  rows: DelayedJob[] | undefined;
  showQueue?: boolean;
}) {
  const now = useNow(1000);
  const promote = useAction(api.promoteDelayed, (id) => `Promoted ${id.slice(0, 8)} to pending`);
  const cancel = useAction(api.cancelDelayed, (id) => `Cancelled ${id.slice(0, 8)}`);
  return (
    <DataTable
      rows={rows}
      getId={(d) => d.jobId}
      defaultSort={{ key: "sched", dir: "asc" }}
      emptyTitle="No delayed jobs"
      columns={[
        { key: "id", header: "Job ID", cell: (d) => <JobId id={d.jobId} /> },
        ...(showQueue
          ? [
              {
                key: "queue",
                header: "Queue",
                sort: (d: DelayedJob) => d.queue,
                cell: (d: DelayedJob) => <Mono>{d.queue}</Mono>,
              },
            ]
          : []),
        { key: "task", header: "Task", sort: (d) => d.task, cell: (d) => <Mono>{d.task}</Mono> },
        { key: "att", header: "Attempts", align: "right", cell: (d) => d.attempts },
        {
          key: "sched",
          header: "Scheduled for",
          sort: (d) => d.scheduledFor,
          cell: (d) => <TimeAgo iso={d.scheduledFor} future />,
        },
        {
          key: "rem",
          header: "Delay remaining",
          align: "right",
          sort: (d) => Date.parse(d.scheduledFor),
          cell: (d) => {
            const r = Date.parse(d.scheduledFor) - now;
            return (
              <span className={r < 60_000 ? "text-warning" : ""}>
                {r <= 0 ? "due" : <Duration ms={r} />}
              </span>
            );
          },
        },
        { key: "st", header: "Status", cell: (d) => <StatusBadge status={d.status} /> },
        {
          key: "a",
          header: "Actions",
          align: "right",
          cell: (d) => (
            <span className="inline-flex gap-0.5">
              <ConfirmDialog
                trigger={
                  <Button variant="ghost" size="sm" className="h-6 px-1.5 text-xs">
                    <Zap className="size-3" /> Promote
                  </Button>
                }
                title="Promote job now?"
                description={
                  <>
                    Job <code className="font-mono">{d.jobId}</code> will be moved to{" "}
                    <code className="font-mono">{d.queue}</code> pending immediately, skipping its
                    scheduled delay.
                  </>
                }
                confirmLabel="Promote now"
                onConfirm={() => promote.mutate(d.jobId)}
              />
              <ConfirmDialog
                trigger={
                  <Button variant="ghost" size="sm" className="h-6 px-1.5 text-xs text-destructive">
                    <X className="size-3" /> Cancel
                  </Button>
                }
                destructive
                title="Cancel scheduled job?"
                description={
                  <>
                    Job <code className="font-mono">{d.jobId}</code> will be marked cancelled and
                    never run.
                  </>
                }
                confirmLabel="Cancel job"
                onConfirm={() => cancel.mutate(d.jobId)}
              />
            </span>
          ),
        },
      ]}
    />
  );
}
