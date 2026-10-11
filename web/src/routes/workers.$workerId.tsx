import { createFileRoute, Link } from "@tanstack/react-router";
import { useWorker } from "@/api/queries";
import { DataTable } from "@/components/data-table";
import {
  EmptyState,
  JobId,
  KV,
  Mono,
  PageHeader,
  Section,
  StatusBadge,
  TimeAgo,
  useNow,
} from "@/components/shared";
import { fmtDuration } from "@/lib/format";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/workers/$workerId")({
  ssr: false,
  head: ({ params }) => ({
    meta: [
      { title: `Worker ${params.workerId} — ForgeQueue Console` },
      {
        name: "description",
        content: `Heartbeat, leases and recent jobs for worker ${params.workerId}.`,
      },
      { property: "og:title", content: `Worker ${params.workerId} — ForgeQueue Console` },
      {
        property: "og:description",
        content: `Heartbeat, leases and recent jobs for worker ${params.workerId}.`,
      },
    ],
  }),
  component: WorkerDetail,
});

function WorkerDetail() {
  const { workerId } = Route.useParams();
  const { data: w, error } = useWorker(workerId);
  const now = useNow(1000);
  if (error) return <EmptyState title="Worker not found" description={error.message} />;
  if (!w) return <div className="h-40 animate-pulse rounded-md bg-muted/40" />;
  const hbAge = now - Date.parse(w.lastHeartbeatAt);
  return (
    <>
      <div className="mb-1 text-xs text-muted-foreground">
        <Link to="/workers" className="hover:text-foreground">
          Workers
        </Link>{" "}
        / <Mono>{w.id}</Mono>
      </div>
      <PageHeader
        title={
          <>
            <Mono className="text-base">{w.id}</Mono> <StatusBadge status={w.status} />
          </>
        }
        meta={<Mono className="text-muted-foreground">{w.hostname}</Mono>}
        actions={
          <div
            className={cn(
              "rounded-sm border px-2 py-1 text-right",
              hbAge > 30_000 ? "tone-warning" : "tone-success",
            )}
          >
            <div className="text-[10px] uppercase tracking-wider">Since last heartbeat</div>
            <div className="font-mono text-sm">{fmtDuration(hbAge)}</div>
          </div>
        }
      />
      <div className="space-y-3">
        <Section title="Info">
          <KV
            items={[
              ["hostname", w.hostname],
              ["version", w.version],
              ["concurrency", w.concurrency],
              ["jobs_processing", `${w.jobsProcessing} / ${w.concurrency}`],
              ["lease_count", w.leaseCount],
              ["started_at", <TimeAgo iso={w.startedAt} />],
              ["uptime", fmtDuration(now - Date.parse(w.startedAt))],
              ["last_heartbeat_at", <TimeAgo iso={w.lastHeartbeatAt} />],
            ]}
          />
        </Section>
        <Section title={`Current leases (${w.leases.length})`} flush>
          <DataTable
            rows={w.leases}
            getId={(l) => l.leaseId}
            emptyTitle="No active leases"
            rowClassName={(l) => (Date.parse(l.expiresAt) < now ? "bg-destructive/5" : undefined)}
            columns={[
              { key: "j", header: "Job ID", cell: (l) => <JobId id={l.jobId} /> },
              { key: "q", header: "Queue", cell: (l) => <Mono>{l.queue}</Mono> },
              { key: "t", header: "Task", cell: (l) => <Mono>{l.task}</Mono> },
              {
                key: "l",
                header: "Lease ID",
                cell: (l) => <Mono className="text-muted-foreground">{l.leaseId}</Mono>,
              },
              { key: "a", header: "Acquired", cell: (l) => <TimeAgo iso={l.acquiredAt} /> },
              {
                key: "e",
                header: "Expires",
                align: "right",
                cell: (l) => {
                  const r = Date.parse(l.expiresAt) - now;
                  return r <= 0 ? (
                    <span className="text-destructive">expired {fmtDuration(-r)} ago</span>
                  ) : (
                    <span>in {fmtDuration(r)}</span>
                  );
                },
              },
            ]}
          />
        </Section>
        <Section title="Recent jobs" flush>
          <DataTable
            rows={w.recentJobs}
            getId={(r) => r.jobId}
            emptyTitle="No recent jobs"
            columns={[
              { key: "j", header: "Job ID", cell: (r) => <JobId id={r.jobId} /> },
              { key: "q", header: "Queue", cell: (r) => <Mono>{r.queue}</Mono> },
              { key: "s", header: "Status", cell: (r) => <StatusBadge status={r.status} /> },
              {
                key: "d",
                header: "Duration",
                align: "right",
                cell: (r) => fmtDuration(r.durationMs),
              },
              { key: "f", header: "Finished", cell: (r) => <TimeAgo iso={r.finishedAt} /> },
            ]}
          />
        </Section>
      </div>
    </>
  );
}
