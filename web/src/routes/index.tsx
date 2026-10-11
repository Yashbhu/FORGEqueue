import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { Pause, Play } from "lucide-react";
import { useAction, useDlq, useQueues, useStats, useThroughput, useWorkers } from "@/api/queries";
import { api } from "@/api/endpoints";
import { DataTable } from "@/components/data-table";
import { Legend, THROUGHPUT, TimeChart } from "@/components/charts";
import {
  JobId,
  Mono,
  PageHeader,
  Section,
  StatCard,
  StatusBadge,
  TimeAgo,
} from "@/components/shared";
import { Button } from "@/components/ui/button";
import { fmtNum, fmtRate } from "@/lib/format";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Overview — ForgeQueue Console" },
      {
        name: "description",
        content: "Live overview of ForgeQueue queues, workers, throughput and recent failures.",
      },
      { property: "og:title", content: "Overview — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Live overview of ForgeQueue queues, workers, throughput and recent failures.",
      },
    ],
  }),
  component: Overview,
});

function Overview() {
  const s = useStats().data;
  const tp = useThroughput("1h").data;
  const queues = useQueues().data;
  const workers = useWorkers().data;
  const dlq = useDlq().data;
  const navigate = useNavigate();
  const pause = useAction(api.pauseQueue, (n) => `Paused queue ${n}`);
  const resume = useAction(api.resumeQueue, (n) => `Resumed queue ${n}`);

  return (
    <>
      <PageHeader title="Overview" description="What's happening right now across the cluster." />
      <div className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-8">
        <StatCard label="Queues" value={s?.queues} to="/queues" />
        <StatCard label="Active jobs" value={s?.activeJobs} />
        <StatCard label="Pending" value={s?.pending} to="/queues" />
        <StatCard label="Processing" value={s?.processing} tone="info" />
        <StatCard label="Failed" value={s?.failed} tone={s?.failed ? "danger" : undefined} />
        <StatCard label="Delayed" value={s?.delayed} to="/delayed" />
        <StatCard label="DLQ" value={s?.dlq} tone={s?.dlq ? "danger" : undefined} to="/dlq" />
        <StatCard
          label="Active workers"
          value={s?.activeWorkers}
          hint={s ? `${s.staleWorkers} stale` : undefined}
          to="/workers"
        />
      </div>

      <div className="mb-3 grid gap-3 lg:grid-cols-3">
        <Section
          title="Throughput · last 1h (jobs/interval)"
          actions={<Legend series={THROUGHPUT} />}
          className="lg:col-span-2"
        >
          <TimeChart data={tp} series={THROUGHPUT} height={200} />
        </Section>
        <Section
          title="Worker health"
          actions={
            <Link to="/workers" className="text-[11px] text-primary hover:underline">
              All workers
            </Link>
          }
          flush
        >
          <div className="grid grid-cols-3 border-b text-center">
            {[
              ["Healthy", s?.healthyWorkers, "text-success"],
              ["Stale", s?.staleWorkers, "text-warning"],
              ["Expired leases", s?.workersWithExpiredLeases, "text-destructive"],
            ].map(([l, v, c]) => (
              <div key={l as string} className="border-r py-1.5 last:border-r-0">
                <div className={cn("font-mono text-sm", c as string)}>
                  {(v as number | undefined) ?? "–"}
                </div>
                <div className="text-[11px] text-muted-foreground">{l}</div>
              </div>
            ))}
          </div>
          <ul>
            {workers?.map((w) => (
              <li key={w.id}>
                <Link
                  to="/workers/$workerId"
                  params={{ workerId: w.id }}
                  className={cn(
                    "flex items-center gap-2 border-b px-2.5 py-1.5 text-xs last:border-b-0 hover:bg-accent/50",
                    w.status === "stale" && "bg-warning/5",
                  )}
                >
                  <Mono>{w.id}</Mono>
                  <StatusBadge status={w.status} />
                  <span className="ml-auto font-mono text-muted-foreground">
                    {w.jobsProcessing}/{w.concurrency}
                  </span>
                  <span className="w-20 text-right">
                    <TimeAgo iso={w.lastHeartbeatAt} />
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </Section>
      </div>

      <Section title="Queues" className="mb-3" flush>
        <DataTable
          rows={queues}
          getId={(q) => q.name}
          onRowClick={(q) => navigate({ to: "/queues/$queueName", params: { queueName: q.name } })}
          defaultSort={{ key: "pending", dir: "desc" }}
          columns={[
            {
              key: "name",
              header: "Queue",
              cell: (q) => <Mono className="text-primary">{q.name}</Mono>,
              sort: (q) => q.name,
            },
            {
              key: "pending",
              header: "Depth",
              align: "right",
              cell: (q) => fmtNum(q.pending),
              sort: (q) => q.pending,
            },
            {
              key: "processing",
              header: "Processing",
              align: "right",
              cell: (q) => fmtNum(q.processing),
              sort: (q) => q.processing,
            },
            {
              key: "delayed",
              header: "Delayed",
              align: "right",
              cell: (q) => fmtNum(q.delayed),
              sort: (q) => q.delayed,
            },
            {
              key: "failed",
              header: "Failed",
              align: "right",
              cell: (q) => (
                <span className={q.failed ? "text-destructive" : ""}>{fmtNum(q.failed)}</span>
              ),
              sort: (q) => q.failed,
            },
            {
              key: "rate",
              header: "Rate",
              align: "right",
              cell: (q) => fmtRate(q.ratePerMin),
              sort: (q) => q.ratePerMin,
            },
            { key: "oldest", header: "Oldest", cell: (q) => <TimeAgo iso={q.oldestJobAt} /> },
            { key: "status", header: "Status", cell: (q) => <StatusBadge status={q.status} /> },
            {
              key: "a",
              header: "",
              align: "right",
              cell: (q) => (
                <span onClick={(e) => e.stopPropagation()}>
                  {q.status === "paused" ? (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-1.5 text-xs"
                      onClick={() => resume.mutate(q.name)}
                    >
                      <Play className="size-3" /> Resume
                    </Button>
                  ) : (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-1.5 text-xs"
                      onClick={() => pause.mutate(q.name)}
                    >
                      <Pause className="size-3" /> Pause
                    </Button>
                  )}
                </span>
              ),
            },
          ]}
        />
      </Section>

      <Section
        title="Recent failures (DLQ)"
        actions={
          <Link to="/dlq" className="text-[11px] text-primary hover:underline">
            Open DLQ
          </Link>
        }
        flush
      >
        <DataTable
          rows={dlq
            ?.slice()
            .sort((a, b) => b.failedAt.localeCompare(a.failedAt))
            .slice(0, 8)}
          getId={(d) => d.jobId}
          emptyTitle="No failures"
          columns={[
            { key: "id", header: "Job ID", cell: (d) => <JobId id={d.jobId} /> },
            { key: "q", header: "Queue", cell: (d) => <Mono>{d.originalQueue}</Mono> },
            {
              key: "e",
              header: "Error",
              className: "max-w-[420px] truncate",
              cell: (d) => <Mono className="text-destructive">{d.lastError}</Mono>,
            },
            { key: "att", header: "Attempts", align: "right", cell: (d) => d.attempts },
            { key: "f", header: "Failed at", cell: (d) => <TimeAgo iso={d.failedAt} /> },
          ]}
        />
      </Section>
    </>
  );
}
